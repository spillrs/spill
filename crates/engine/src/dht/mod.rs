//! Process-owned IPv4 BEP 5 node. One socket/task, bounded state and explicit consent.
//! Only source/transaction-matched replies verify contacts; routers never enter routing.
mod bootstrap;
mod lookup;
mod routing;
pub(crate) use lookup::{Discovery, DiscoveryEvent};
mod state;

use crate::{
    identity::entropy,
    session::dht::{Handle, MAX_SAVED_NODES, SavedState},
};
use protocol::{
    bencode::{self, Node as BNode, Value},
    compact,
    dht::{
        NodeId,
        krpc::{Body, Kind, Message, NodeV4, Query as WireQuery},
    },
};
use routing::{K, Routing};
use state::{Peers, Rate, Tokens};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io,
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{Semaphore, mpsc, oneshot, watch},
    time::Instant,
};

pub(crate) const DATAGRAM: usize = 4096;
const MAX_REPLY_NODES: usize = 128;
const CODEC: bencode::Limits = bencode::Limits {
    max_input_len: DATAGRAM,
    max_output_len: DATAGRAM,
    max_string_len: DATAGRAM,
    max_nodes: 512,
    max_depth: 8,
};

mod limits;
use limits::Limits;
use protocol::dht::{OwnedQuery as Query, OwnedReply as Reply};
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Stats {
    pub routing_nodes: usize,
    pub good_nodes: usize,
    pub pending: usize,
    pub torrents: usize,
    pub peers: usize,
    pub rate_sources: usize,
    pub answered: u64,
    pub dropped: u64,
}
pub(crate) enum Command {
    BootstrapFinished(oneshot::Sender<()>),
    RefreshTargets(oneshot::Sender<Vec<NodeId>>),
    Snapshot(oneshot::Sender<SavedState>),
    Routers(Vec<SocketAddrV4>, oneshot::Sender<()>),
    Request {
        address: SocketAddrV4,
        query: Query,
        reply: oneshot::Sender<io::Result<Reply>>,
    },
    Probe(SocketAddrV4),
    Closest(NodeId, oneshot::Sender<Vec<NodeV4>>),
    Peers(
        protocol::identity::InfoHashV1,
        oneshot::Sender<Vec<SocketAddrV4>>,
    ),
    Stats(oneshot::Sender<Stats>),
}
struct Pending {
    address: SocketAddrV4,
    query: Query,
    deadline: Instant,
    reply: Option<oneshot::Sender<io::Result<Reply>>>,
}
/// Caller owns the task lifetime and must join `run` on shutdown. Dropping `run`
/// closes all pending callers; no child tasks are spawned.
pub(crate) struct Node {
    allow_local_network: bool,
    socket: Arc<UdpSocket>,
    incoming: mpsc::Receiver<(Vec<u8>, SocketAddr)>,
    id: NodeId,
    limits: Limits,
    commands: mpsc::Receiver<Command>,
    routing: Routing,
    tokens: Tokens,
    peers: Peers,
    rate: Rate,
    pending: HashMap<[u8; 8], Pending>,
    answered: u64,
    dropped: u64,
    routers: HashSet<SocketAddrV4>,
    hints: Vec<SocketAddrV4>,
    bootstrapped: bool,
    bootstrap: Option<std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>>,
}

mod handle;

impl Node {
    /// Restore identity; reverify saved contacts before consented, pinned router bootstrap.
    pub(crate) async fn with_state(
        socket: Arc<UdpSocket>,
        incoming: mpsc::Receiver<(Vec<u8>, SocketAddr)>,
        allow_local_network: bool,
        allow_network: bool,
        saved: Option<SavedState>,
        routers: Vec<String>,
    ) -> io::Result<(Self, Handle)> {
        let SocketAddr::V4(address) = socket.local_addr()? else {
            unreachable!("IPv4 session")
        };
        if saved.as_ref().is_some_and(|s| {
            s.nodes.len() > MAX_SAVED_NODES
                || s.nodes.iter().any(|n| !valid_address(n.address, false))
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid saved DHT state",
            ));
        }
        if !allow_network {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DHT requires network consent",
            ));
        }
        let limits = crate::dht::Limits::DEFAULT;
        if !valid_address(address, true) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DHT bind address",
            ));
        }
        let id = if let Some(saved) = &saved {
            saved.id
        } else {
            NodeId(entropy()?)
        };
        let now = Instant::now();
        let tokens = Tokens::new(now)?;
        let (sender, commands) = mpsc::channel(limits.command_queue);
        let handle = Handle {
            allow_local_network,
            commands: sender,
            address,
            id,
            lookups: std::sync::Arc::new(Semaphore::new(limits.max_lookups)),
            lookup_queries: limits.max_lookup_queries,
            max_peers: crate::profile::MAX_QUEUED,
        };
        let numeric_routers = routers
            .iter()
            .filter_map(|s| s.parse::<SocketAddrV4>().ok())
            .collect();
        let bootstrap = Some(Box::pin(bootstrap::run(
            handle.clone(),
            saved.unwrap_or(SavedState { id, nodes: vec![] }),
            routers,
            allow_local_network,
        ))
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = io::Result<()>> + Send>,
            >);
        Ok((
            Self {
                allow_local_network,
                socket,
                incoming,
                id,
                limits,
                commands,
                routing: Routing::new(id, limits.max_buckets),
                tokens,
                peers: Peers::new(limits),
                rate: Rate::new(limits, now),
                pending: HashMap::new(),
                answered: 0,
                dropped: 0,
                routers: numeric_routers,
                hints: Vec::new(),
                bootstrapped: false,
                bootstrap,
            },
            handle,
        ))
    }
    fn stats(&self) -> Stats {
        let (torrents, peers) = self.peers.counts();
        Stats {
            routing_nodes: self.routing.len(),
            good_nodes: self.routing.good(Instant::now()),
            pending: self.pending.len(),
            torrents,
            peers,
            rate_sources: self.rate.len(),
            answered: self.answered,
            dropped: self.dropped,
        }
    }
    fn snapshot(&self) -> SavedState {
        let stats = self.stats();
        tracing::info!(
            routing_nodes = stats.routing_nodes,
            good_nodes = stats.good_nodes,
            pending = stats.pending,
            torrents = stats.torrents,
            peers = stats.peers,
            rate_sources = stats.rate_sources,
            answered = stats.answered,
            dropped = stats.dropped,
            "dht_state_snapshot"
        );
        SavedState {
            id: self.id,
            nodes: self.routing.saved(Instant::now()),
        }
    }
    pub(crate) async fn run_with_state(
        mut self,
        mut stop: watch::Receiver<bool>,
    ) -> io::Result<SavedState> {
        let mut bootstrap = self.bootstrap.take();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut maintenance = Instant::now();
        loop {
            if *stop.borrow() {
                return Ok(self.snapshot());
            }
            tokio::select! {
                biased;
                _ = stop.changed() => return Ok(self.snapshot()),
                result = async { bootstrap.as_mut().expect("bootstrap enabled").await }, if bootstrap.is_some() => { result?; bootstrap = None; },
                _ = tick.tick() => {
                    let now = Instant::now();
                    self.tokens.rotate(now)?;
                    self.expire(now);
                    if now >= maintenance {
                        self.peers.expire(now);
                        self.rate.expire(now);
                        for node in self.routing.questionable(now).into_iter().take(16) {
                            self.probe(node.address).await?;
                        }
                        maintenance = now + Duration::from_secs(60);
                    }
                }
                command = self.commands.recv(), if !self.commands.is_closed() || !self.commands.is_empty() => {
                    match command {
                        Some(Command::RefreshTargets(reply)) => {
                            let _ = reply.send(self.routing.refresh_targets(Instant::now())?);
                        }
                        Some(Command::BootstrapFinished(reply)) => {
                            self.bootstrapped = true;
                            for address in std::mem::take(&mut self.hints) { self.probe(address).await?; }
                            let _ = reply.send(());
                        }
                        Some(Command::Snapshot(reply)) => { let _ = reply.send(self.snapshot()); }
                        Some(Command::Routers(addresses, reply)) => {
                            self.routers = addresses.into_iter().collect();
                            self.routing.remove_addresses(&self.routers);
                            let _ = reply.send(());
                        }
                        Some(Command::Stats(reply)) => { let _ = reply.send(self.stats()); }
                        Some(Command::Closest(target, reply)) => {
                            let now = Instant::now();
                            self.routing.touch(target, now);
                            let _ = reply.send(self.routing.lookup_candidates(target, now));
                        }
                        Some(Command::Peers(hash, reply)) => { let _ = reply.send(self.peers.get(hash.as_bytes(), Instant::now())); }
                        Some(Command::Probe(address)) => {
                            if self.bootstrapped { self.probe(address).await?; }
                            else if self.hints.len() < self.limits.command_queue && !self.hints.contains(&address) { self.hints.push(address); }
                        },
                        Some(Command::Request { address, query, reply }) => {
                            self.start(address, query, Some(reply)).await?;
                        }
                        None => {}
                    }
                }
                received = self.incoming.recv() => {
                    let (bytes, address) = received.expect("session UDP router");
                    if let SocketAddr::V4(address) = address {
                        self.receive(&bytes, address).await?;
                    }
                }
            }
        }
    }
    fn expire(&mut self, now: Instant) {
        self.pending.retain(|_, p| {
            if p.reply.as_ref().is_some_and(|r| r.is_closed()) {
                return false;
            }
            if now < p.deadline {
                return true;
            }
            self.routing.failed(p.address);
            if let Some(reply) = p.reply.take() {
                let _ = reply.send(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DHT query timed out",
                )));
            }
            false
        });
    }
    async fn probe(&mut self, address: SocketAddrV4) -> io::Result<()> {
        if self.pending.values().any(|p| p.address == address)
            // Reserve at least one transaction for foreground queries, even at tiny caps.
            || self.pending.len() >= self.limits.max_pending.saturating_sub(1)
            || self.pending.values().filter(|p| p.reply.is_none()).count()
                >= (self.limits.max_pending / 4).clamp(1, 16)
        {
            return Ok(());
        }
        self.start(address, Query::Ping, None).await
    }
    async fn start(
        &mut self,
        address: SocketAddrV4,
        query: Query,
        reply: Option<oneshot::Sender<io::Result<Reply>>>,
    ) -> io::Result<()> {
        let reject = if matches!(&query, Query::AnnouncePeer { port, token, .. } if *port == 0 || token.len() > 64)
        {
            Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DHT announce",
            ))
        } else if !crate::network::peer(address.into(), self.allow_local_network) {
            Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DHT contact",
            ))
        } else if self.pending.len() >= self.limits.max_pending {
            Some(io::Error::new(
                io::ErrorKind::WouldBlock,
                "DHT transaction limit",
            ))
        } else {
            None
        };
        if let Some(error) = reject {
            if let Some(reply) = reply {
                let _ = reply.send(Err(error));
            }
            return Ok(());
        }
        let mut transaction = entropy::<8>()?;
        while self.pending.contains_key(&transaction) {
            transaction = entropy()?;
        }
        let packet = query
            .encode(&transaction, self.id, CODEC, K, 100)
            .map_err(io::Error::other)?;
        match self.send(&packet, address).await {
            Ok(()) => {
                self.pending.insert(
                    transaction,
                    Pending {
                        address,
                        query,
                        deadline: Instant::now() + self.limits.query_timeout,
                        reply,
                    },
                );
            }
            Err(error) => {
                if let Some(reply) = reply {
                    let _ = reply.send(Err(error));
                }
            }
        }
        Ok(())
    }
    async fn send(&mut self, packet: &[u8], address: SocketAddrV4) -> io::Result<()> {
        if !crate::network::peer(address.into(), self.allow_local_network) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DHT endpoint blocked by network policy",
            ));
        }
        tokio::time::timeout(Duration::from_secs(1), self.socket.send_to(packet, address))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DHT send timed out"))??;
        Ok(())
    }
    async fn receive(&mut self, packet: &[u8], address: SocketAddrV4) -> io::Result<()> {
        if !crate::network::peer(address.into(), self.allow_local_network) {
            return Ok(());
        }
        let Ok(message) = Message::parse(packet, CODEC, MAX_REPLY_NODES, 100) else {
            self.dropped = self.dropped.saturating_add(1);
            return Ok(());
        };
        match message.kind() {
            Kind::Query { id, query } => {
                let now = Instant::now();
                if message.transaction_id().len() > 32 || !self.rate.query(*address.ip(), now) {
                    self.dropped = self.dropped.saturating_add(1);
                    return Ok(());
                }
                let packet = self.answer(message.transaction_id(), query, address, now)?;
                if self.rate.reply(packet.len(), now) {
                    // Remote socket errors must not terminate the process-wide node.
                    if self.send(&packet, address).await.is_ok() {
                        self.answered = self.answered.saturating_add(1);
                    }
                } else {
                    self.dropped = self.dropped.saturating_add(1);
                }
                let node = NodeV4 { id, address };
                let read_only = message
                    .document()
                    .get(b"ro")
                    .is_some_and(|ro| matches!(ro.value(), Value::Integer(value) if *value != 0));
                if !read_only && !self.routers.contains(&address) {
                    self.routing.heard(node, false, now);
                }
                if !read_only
                    && !self.routers.contains(&address)
                    && !self.routing.closest(id, now).contains(&node)
                {
                    self.probe(address).await?;
                }
            }
            Kind::Response {
                id, nodes, token, ..
            } => {
                let Ok(transaction) = <[u8; 8]>::try_from(message.transaction_id()) else {
                    return Ok(());
                };
                let Some(pending) = self.pending.get(&transaction) else {
                    return Ok(());
                };
                if pending.address != address || Instant::now() >= pending.deadline {
                    return Ok(());
                }
                if nodes.is_some_and(|n| !n.remainder().is_empty())
                    || matches!(pending.query, Query::FindNode(_)) && nodes.is_none()
                    || token.is_some_and(|t| t.len() > 64)
                    || matches!(pending.query, Query::GetPeers(_))
                        && nodes.is_none()
                        && message.peers().is_none()
                {
                    return Ok(());
                }
                let mut pending = self
                    .pending
                    .remove(&transaction)
                    .expect("matched transaction");
                if !self.routers.contains(&address) {
                    self.routing
                        .heard(NodeV4 { id, address }, true, Instant::now());
                }
                let nodes = nodes
                    .map(|n| {
                        n.iter()
                            .filter(|n| {
                                crate::network::peer(n.address.into(), self.allow_local_network)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(reply) = pending.reply.take() {
                    let peers = message
                        .peers()
                        .into_iter()
                        .flatten()
                        .filter_map(|p| match p {
                            SocketAddr::V4(p)
                                if crate::network::peer(p.into(), self.allow_local_network) =>
                            {
                                Some(p)
                            }
                            _ => None,
                        })
                        .collect();
                    let _ = reply.send(Ok(Reply {
                        id,
                        nodes,
                        token: token.map(<[u8]>::to_vec),
                        peers,
                    }));
                }
            }
            Kind::Error { code, .. } => {
                let Ok(transaction) = <[u8; 8]>::try_from(message.transaction_id()) else {
                    return Ok(());
                };
                if self
                    .pending
                    .get(&transaction)
                    .is_some_and(|p| p.address == address && Instant::now() < p.deadline)
                {
                    let mut pending = self.pending.remove(&transaction).expect("matched error");
                    if let Some(reply) = pending.reply.take() {
                        let _ =
                            reply.send(Err(io::Error::other(format!("DHT remote error {code}"))));
                    }
                }
            }
            Kind::Unknown { .. } => {}
        }
        Ok(())
    }
    fn answer(
        &mut self,
        transaction: &[u8],
        query: WireQuery<'_>,
        source: SocketAddrV4,
        now: Instant,
    ) -> io::Result<Vec<u8>> {
        self.tokens.rotate(now)?;
        let mut fields = BTreeMap::from([(b"id".as_slice(), bytes(&self.id.0))]);
        let raw_nodes;
        let token;
        let raw_peers;
        let error = match query {
            WireQuery::Ping => None,
            WireQuery::FindNode { target, want } => {
                raw_nodes = if want.is_none_or(|w| w.ipv4) {
                    self.routing
                        .closest(target, now)
                        .iter()
                        .flat_map(NodeV4::encode)
                        .collect::<Vec<_>>()
                } else {
                    vec![]
                };
                fields.insert(b"nodes", bytes(&raw_nodes));
                None
            }
            WireQuery::GetPeers { info_hash, want } => {
                token = self.tokens.issue(*source.ip(), info_hash.as_bytes());
                fields.insert(b"token", bytes(&token));
                raw_peers = self
                    .peers
                    .get(info_hash.as_bytes(), now)
                    .into_iter()
                    .map(compact::encode_v4)
                    .collect::<Vec<_>>();
                if raw_peers.is_empty() {
                    raw_nodes = if want.is_none_or(|w| w.ipv4) {
                        self.routing
                            .closest(NodeId(*info_hash.as_bytes()), now)
                            .iter()
                            .flat_map(NodeV4::encode)
                            .collect::<Vec<_>>()
                    } else {
                        vec![]
                    };
                    fields.insert(b"nodes", bytes(&raw_nodes));
                } else {
                    fields.insert(
                        b"values",
                        BNode::new(Value::List(raw_peers.iter().map(|p| bytes(p)).collect())),
                    );
                }
                None
            }
            WireQuery::AnnouncePeer {
                info_hash,
                port,
                token,
            } => {
                if !self.tokens.valid(token, *source.ip(), info_hash.as_bytes()) {
                    Some((203, b"invalid token".as_slice()))
                } else if !crate::network::peer(
                    SocketAddrV4::new(*source.ip(), port.unwrap_or(source.port())).into(),
                    self.allow_local_network,
                ) {
                    Some((203, b"peer endpoint blocked by network policy".as_slice()))
                } else if !self.peers.put(
                    *info_hash.as_bytes(),
                    SocketAddrV4::new(*source.ip(), port.unwrap_or(source.port())),
                    now,
                ) {
                    Some((202, b"peer store full or invalid port".as_slice()))
                } else {
                    None
                }
            }
            WireQuery::Unknown { .. } => Some((204, b"unknown method".as_slice())),
        };
        let body = if let Some((code, message)) = error {
            Body::Error { code, message }
        } else {
            Body::Response(BNode::new(Value::Dictionary(fields)))
        };
        Message::new(transaction, body, None, K, 100)
            .and_then(|m| m.encode(CODEC))
            .map_err(io::Error::other)
    }
}
pub(crate) fn valid_address(address: SocketAddrV4, bind: bool) -> bool {
    let ip = address.ip();
    !ip.is_multicast()
        && !ip.is_broadcast()
        && (bind || !ip.is_unspecified() && address.port() != 0)
}
fn bytes(value: &[u8]) -> BNode<'_> {
    BNode::new(Value::Bytes(value))
}
fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "DHT node stopped")
}
