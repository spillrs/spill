//! Session-owned BEP 5/32 node. One socket/task, bounded state and explicit consent.
//! Only source/transaction-matched replies verify contacts; routers never enter routing.
mod bootstrap;
mod lookup;
mod routing;
pub(in crate::engine) use lookup::{Discovery, DiscoveryEvent};
mod state;

use crate::engine::{
    identity::entropy,
    session::{
        dht::{Handle, MAX_SAVED_NODES, SavedState},
        udp,
    },
};
use crate::protocol::{
    bencode::{self, Node as BNode, Value},
    compact,
    dht::{
        Contact, NodeId,
        krpc::{Body, Kind, Message, NodeV4, NodeV6, Query as WireQuery, Want},
    },
};
use routing::{K, Routing};
use state::{Peers, Rate, Tokens};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, oneshot, watch},
    time::Instant,
};

pub(in crate::engine) const DATAGRAM: usize = 4096;
const MAX_REPLY_NODES: usize = 128;
const CODEC: bencode::Limits = bencode::Limits {
    max_input_len: DATAGRAM,
    max_output_len: 1024,
    max_string_len: DATAGRAM,
    max_nodes: 512,
    max_depth: 8,
};

mod limits;
use crate::protocol::dht::{OwnedQuery as Query, OwnedReply as Reply};
use limits::Limits;
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::engine) struct Stats {
    pub routing_nodes: usize,
    pub good_nodes: usize,
    pub pending: usize,
    pub torrents: usize,
    pub peers: usize,
    pub rate_sources: usize,
    pub answered: u64,
    pub dropped: u64,
}
pub(in crate::engine) enum Command {
    BootstrapFinished(bool, oneshot::Sender<()>),
    RefreshTargets(bool, oneshot::Sender<Vec<NodeId>>),
    Snapshot(oneshot::Sender<SavedState>),
    Routers(bool, Vec<SocketAddr>, oneshot::Sender<()>),
    Request {
        address: SocketAddr,
        query: Query,
        reply: oneshot::Sender<io::Result<Reply>>,
    },
    Probe(SocketAddr),
    Closest(bool, NodeId, oneshot::Sender<Vec<Contact>>),
    Peers([u8; 20], bool, oneshot::Sender<Vec<SocketAddr>>),
    Stats(bool, oneshot::Sender<Stats>),
}
struct Pending {
    address: SocketAddr,
    query: Query,
    deadline: Instant,
    reply: Option<oneshot::Sender<io::Result<Reply>>>,
}
/// Caller owns the task lifetime and must join `run_with_state` on shutdown.
/// Dropping it closes all pending callers; no child tasks are spawned.
pub(in crate::engine) struct Node {
    allow_local_network: bool,
    transport: udp::Transport,
    incoming: mpsc::Receiver<(Vec<u8>, SocketAddr)>,
    id: NodeId,
    limits: Limits,
    commands: mpsc::Receiver<Command>,
    routing: [Routing; 2],
    tokens: Tokens,
    peers: Peers,
    rate: Rate,
    pending: HashMap<[u8; 8], Pending>,
    answered: u64,
    dropped: u64,
    routers: HashSet<SocketAddr>,
    hints: Vec<SocketAddr>,
    bootstrapped: [bool; 2],
    bootstrap: Option<std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>>,
}

mod handle;

impl Node {
    /// Restore identity; reverify saved contacts before consented, pinned router bootstrap.
    pub(in crate::engine) fn with_state(
        transport: udp::Transport,
        incoming: mpsc::Receiver<(Vec<u8>, SocketAddr)>,
        allow_local_network: bool,
        saved: Option<SavedState>,
        routers: Vec<String>,
    ) -> io::Result<(Self, Handle)> {
        let address = transport.address;
        let limits = Limits::DEFAULT;
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
            families: transport.families,
            id,
            lookups: Arc::new(Semaphore::new(limits.max_lookups)),
            lookup_queries: limits.max_lookup_queries,
            max_peers: crate::engine::profile::MAX_QUEUED,
        };
        let numeric_routers = routers
            .iter()
            .filter_map(|s| s.parse::<SocketAddr>().ok())
            .collect();
        let bootstrap_handle = handle.clone();
        let bootstrap = Some(Box::pin(async move {
            let saved = saved.unwrap_or(SavedState { id, nodes: vec![] });
            let run = |ipv6| {
                let handle = bootstrap_handle.clone();
                let saved = saved.clone();
                let routers = routers.clone();
                async move {
                    if handle.families[usize::from(ipv6)] {
                        bootstrap::run(handle, ipv6, saved, routers, allow_local_network).await?;
                    }
                    Ok::<(), io::Error>(())
                }
            };
            tokio::try_join!(run(false), run(true))?;
            Ok(())
        })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = io::Result<()>> + Send>,
            >);
        Ok((
            Self {
                allow_local_network,
                transport,
                incoming,
                id,
                limits,
                commands,
                routing: std::array::from_fn(|_| Routing::new(id, limits.max_buckets)),
                tokens,
                peers: Peers::new(limits),
                rate: Rate::new(limits, now),
                pending: HashMap::new(),
                answered: 0,
                dropped: 0,
                routers: numeric_routers,
                hints: Vec::new(),
                bootstrapped: [false; 2],
                bootstrap,
            },
            handle,
        ))
    }
    fn stats(&self, ipv6: bool) -> Stats {
        let (torrents, peers) = self.peers.counts();
        Stats {
            routing_nodes: self.routing[usize::from(ipv6)].len(),
            good_nodes: self.routing[usize::from(ipv6)].good(Instant::now()),
            pending: self.pending.len(),
            torrents,
            peers,
            rate_sources: self.rate.len(),
            answered: self.answered,
            dropped: self.dropped,
        }
    }
    fn snapshot(&self) -> SavedState {
        let mut stats = self.stats(false);
        stats.routing_nodes += self.routing[1].len();
        stats.good_nodes += self.routing[1].good(Instant::now());
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
            nodes: self
                .routing
                .iter()
                .flat_map(|r| r.saved(Instant::now()))
                .collect(),
        }
    }
    pub(in crate::engine) async fn run_with_state(
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
                        let stale: Vec<_> = self.routing.iter().flat_map(|r| r.questionable(now).into_iter().take(8)).collect();
                        for node in stale { self.probe(node.address).await?; }
                        maintenance = now + Duration::from_secs(60);
                    }
                }
                command = self.commands.recv(), if !self.commands.is_closed() || !self.commands.is_empty() => {
                    match command {
                        Some(Command::RefreshTargets(ipv6, reply)) => {
                            let _ = reply.send(self.routing[usize::from(ipv6)].refresh_targets(Instant::now())?);
                        }
                        Some(Command::BootstrapFinished(ipv6, reply)) => {
                            self.bootstrapped[usize::from(ipv6)] = true;
                            for address in std::mem::take(&mut self.hints) {
                                if address.is_ipv6() == ipv6 { self.probe(address).await?; }
                                else { self.hints.push(address); }
                            }
                            let _ = reply.send(());
                        }
                        Some(Command::Snapshot(reply)) => { let _ = reply.send(self.snapshot()); }
                        Some(Command::Routers(ipv6, addresses, reply)) => {
                            self.routers.retain(|a| a.is_ipv6() != ipv6);
                            self.routers.extend(addresses);
                            self.routing[usize::from(ipv6)].remove_addresses(&self.routers);
                            let _ = reply.send(());
                        }
                        Some(Command::Stats(ipv6, reply)) => { let _ = reply.send(self.stats(ipv6)); }
                        Some(Command::Closest(ipv6, target, reply)) => {
                            let routing = &mut self.routing[usize::from(ipv6)];
                            let now = Instant::now();
                            routing.touch(target, now);
                            let _ = reply.send(routing.lookup_candidates(target, now));
                        }
                        Some(Command::Peers(hash, ipv6, reply)) => { let _ = reply.send(self.peers.get(&hash, ipv6, Instant::now())); }
                        Some(Command::Probe(address)) => {
                            if self.bootstrapped[usize::from(address.is_ipv6())] { self.probe(address).await?; }
                            else if self.hints.len() < self.limits.command_queue && !self.hints.contains(&address) { self.hints.push(address); }
                        }
                        Some(Command::Request { address, query, reply }) => { self.start(address, query, Some(reply)).await?; }
                        None => {}
                    }
                }
                received = self.incoming.recv() => {
                    let (bytes, address) = received.expect("session UDP router");
                    self.receive(&bytes, address).await?;
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
            self.routing[usize::from(p.address.is_ipv6())].failed(p.address);
            if let Some(reply) = p.reply.take() {
                let _ = reply.send(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DHT query timed out",
                )));
            }
            false
        });
    }
    async fn probe(&mut self, address: SocketAddr) -> io::Result<()> {
        if self.pending.values().any(|p| p.address == address)
            // Leave room for foreground queries.
            || self.pending.len() >= self.limits.max_pending.saturating_sub(1)
            || self.pending.values().filter(|p| p.reply.is_none()).count() >= (self.limits.max_pending / 4).clamp(1, 16)
        {
            return Ok(());
        }
        self.start(address, Query::Ping, None).await
    }
    async fn start(
        &mut self,
        address: SocketAddr,
        query: Query,
        reply: Option<oneshot::Sender<io::Result<Reply>>>,
    ) -> io::Result<()> {
        let reject = if matches!(&query, Query::AnnouncePeer { port, token, .. } if *port == 0 || token.len() > 64)
        {
            Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DHT announce",
            ))
        } else if !crate::engine::network::peer(address, self.allow_local_network) {
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
        let want = if matches!(query, Query::FindNode(_)) {
            Want {
                ipv4: self.transport.families[0],
                ipv6: self.transport.families[1],
            }
        } else {
            Want {
                ipv4: address.is_ipv4(),
                ipv6: address.is_ipv6(),
            }
        };
        let packet = query
            .encode(&transaction, self.id, Some(want), CODEC, K, 100)
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
    async fn send(&mut self, packet: &[u8], address: SocketAddr) -> io::Result<()> {
        if !crate::engine::network::peer(address, self.allow_local_network) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DHT endpoint blocked by network policy",
            ));
        }
        let destination = match address {
            SocketAddr::V4(v4) if self.transport.address.is_ipv6() => {
                SocketAddr::new(v4.ip().to_ipv6_mapped().into(), v4.port())
            }
            address => address,
        };
        tokio::time::timeout(
            Duration::from_secs(1),
            self.transport.socket.send_to(packet, destination),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DHT send timed out"))??;
        Ok(())
    }
    async fn receive(&mut self, packet: &[u8], address: SocketAddr) -> io::Result<()> {
        if !valid_address(address, false)
            || !crate::engine::network::peer(address, self.allow_local_network)
        {
            return Ok(());
        }
        let Ok(message) = Message::parse(packet, CODEC, MAX_REPLY_NODES, 100) else {
            self.dropped = self.dropped.saturating_add(1);
            return Ok(());
        };
        let family = usize::from(address.is_ipv6());
        match message.kind() {
            Kind::Query { id, query } => {
                let now = Instant::now();
                if message.transaction_id().len() > 32 || !self.rate.query(address.ip(), now) {
                    self.dropped = self.dropped.saturating_add(1);
                    return Ok(());
                }
                self.tokens.rotate(now)?;
                let Ok(packet) = self.answer(message.transaction_id(), query, address, now) else {
                    self.dropped = self.dropped.saturating_add(1);
                    return Ok(());
                };
                if self.rate.reply(packet.len(), now) {
                    if self.send(&packet, address).await.is_ok() {
                        self.answered = self.answered.saturating_add(1);
                    }
                } else {
                    self.dropped = self.dropped.saturating_add(1);
                }
                let node = Contact { id, address };
                let read_only = message
                    .document()
                    .get(b"ro")
                    .is_some_and(|ro| matches!(ro.value(), Value::Integer(value) if *value != 0));
                if !read_only && !self.routers.contains(&address) {
                    self.routing[family].heard(node, false, now);
                    if !self.routing[family].closest(id, now).contains(&node) {
                        self.probe(address).await?;
                    }
                }
            }
            Kind::Response {
                id,
                nodes,
                nodes6,
                token,
                ..
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
                let has_nodes = self.transport.families[0] && nodes.is_some()
                    || self.transport.families[1] && nodes6.is_some();
                if nodes.is_some_and(|n| !n.remainder().is_empty())
                    || nodes6.is_some_and(|n| !n.remainder().is_empty())
                    || matches!(pending.query, Query::FindNode(_)) && !has_nodes
                    || token.is_some_and(|t| t.len() > 64)
                    || matches!(pending.query, Query::GetPeers(_))
                        && !has_nodes
                        && message.peers().is_none()
                {
                    return Ok(());
                }
                let mut pending = self
                    .pending
                    .remove(&transaction)
                    .expect("matched transaction");
                if !self.routers.contains(&address) {
                    self.routing[family].heard(Contact { id, address }, true, Instant::now());
                }
                let nodes: Vec<_> = nodes
                    .as_ref()
                    .into_iter()
                    .flat_map(|n| {
                        n.iter().map(|n| Contact {
                            id: n.id,
                            address: n.address.into(),
                        })
                    })
                    .chain(nodes6.as_ref().into_iter().flat_map(|n| {
                        n.iter().map(|n| Contact {
                            id: n.id,
                            address: n.address.into(),
                        })
                    }))
                    .filter(|n| {
                        self.transport.families[usize::from(n.address.is_ipv6())]
                            && valid_address(n.address, false)
                            && crate::engine::network::peer(n.address, self.allow_local_network)
                    })
                    .collect();
                // Cross-family referrals are hints, never proof of reachability.
                for node in nodes
                    .iter()
                    .filter(|n| n.address.is_ipv6() != address.is_ipv6())
                    .take(K)
                {
                    if node.id != self.id && !self.routers.contains(&node.address) {
                        let routing = &self.routing[usize::from(node.address.is_ipv6())];
                        if !routing.closest(node.id, Instant::now()).contains(node) {
                            self.probe(node.address).await?;
                        }
                    }
                }
                if let Some(reply) = pending.reply.take() {
                    let peers = message
                        .peers()
                        .into_iter()
                        .flatten()
                        .filter(|p| {
                            valid_address(*p, false)
                                && crate::engine::network::peer(*p, self.allow_local_network)
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
        source: SocketAddr,
        now: Instant,
    ) -> Result<Vec<u8>, crate::protocol::dht::krpc::Error> {
        let mut fields = BTreeMap::from([(b"id".as_slice(), bytes(&self.id.0))]);
        let raw_nodes;
        let raw_nodes6;
        let token;
        let mut raw_peers = Vec::new();
        let target = match query {
            WireQuery::FindNode { target, want } => Some((target, want)),
            WireQuery::GetPeers { info_hash, want } => Some((NodeId(info_hash), want)),
            _ => None,
        };
        if let Some((target, want)) = target {
            let want = want.unwrap_or(Want {
                ipv4: source.is_ipv4(),
                ipv6: source.is_ipv6(),
            });
            if want.ipv4 {
                raw_nodes = self.routing[0]
                    .closest(target, now)
                    .iter()
                    .flat_map(|n| {
                        let SocketAddr::V4(address) = n.address else {
                            unreachable!("IPv4 routing table")
                        };
                        NodeV4 { id: n.id, address }.encode()
                    })
                    .collect::<Vec<_>>();
                fields.insert(b"nodes", bytes(&raw_nodes));
            }
            if want.ipv6 {
                raw_nodes6 = self.routing[1]
                    .closest(target, now)
                    .iter()
                    .flat_map(|n| {
                        let SocketAddr::V6(address) = n.address else {
                            unreachable!("IPv6 routing table")
                        };
                        NodeV6 { id: n.id, address }
                            .encode()
                            .expect("unscoped IPv6 contact")
                    })
                    .collect::<Vec<_>>();
                fields.insert(b"nodes6", bytes(&raw_nodes6));
            }
        }
        let error = match query {
            WireQuery::Ping | WireQuery::FindNode { .. } => None,
            WireQuery::GetPeers { info_hash, .. } => {
                token = self.tokens.issue(source.ip(), &info_hash);
                fields.insert(b"token", bytes(&token));
                raw_peers = self
                    .peers
                    .get(&info_hash, source.is_ipv6(), now)
                    .into_iter()
                    .map(|p| match p {
                        SocketAddr::V4(p) => compact::encode_v4(p).to_vec(),
                        SocketAddr::V6(p) => {
                            compact::encode_v6(p).expect("unscoped IPv6 peer").to_vec()
                        }
                    })
                    .collect::<Vec<_>>();
                None
            }
            WireQuery::AnnouncePeer {
                info_hash,
                port,
                token,
            } => {
                let peer = SocketAddr::new(source.ip(), port.unwrap_or(source.port()));
                if !self.tokens.valid(token, source.ip(), &info_hash) {
                    Some((203, b"invalid token".as_slice()))
                } else if !crate::engine::network::peer(peer, self.allow_local_network) {
                    Some((203, b"peer endpoint blocked by network policy".as_slice()))
                } else if !self.peers.put(info_hash, peer, now) {
                    Some((202, b"peer store full or invalid port".as_slice()))
                } else {
                    None
                }
            }
            WireQuery::Unknown { .. } => Some((204, b"unknown method".as_slice())),
        };
        loop {
            let mut response = fields.clone();
            if !raw_peers.is_empty() {
                response.insert(
                    b"values",
                    BNode::new(Value::List(raw_peers.iter().map(|p| bytes(p)).collect())),
                );
            }
            let body = if let Some((code, message)) = error {
                Body::Error { code, message }
            } else {
                Body::Response(BNode::new(Value::Dictionary(response)))
            };
            let encoded =
                Message::new(transaction, body, None, K * 2, 100).and_then(|m| m.encode(CODEC));
            match encoded {
                Err(crate::protocol::dht::krpc::Error::Bencode(ref error))
                    if error.kind == bencode::ErrorKind::OutputLimit && !raw_peers.is_empty() =>
                {
                    // Fit the actual envelope, including any optional fields.
                    raw_peers.pop();
                }
                result => return result,
            }
        }
    }
}
pub(in crate::engine) fn valid_address(address: SocketAddr, bind: bool) -> bool {
    match address {
        SocketAddr::V4(a) => {
            !a.ip().is_multicast()
                && !a.ip().is_broadcast()
                && (bind || !a.ip().is_unspecified() && a.port() != 0)
        }
        SocketAddr::V6(a) => {
            !a.ip().is_multicast()
                && a.ip().to_ipv4_mapped().is_none()
                && !a.ip().is_unicast_link_local()
                && a.scope_id() == 0
                && a.flowinfo() == 0
                && (bind || !a.ip().is_unspecified() && a.port() != 0)
        }
    }
}
fn bytes(value: &[u8]) -> BNode<'_> {
    BNode::new(Value::Bytes(value))
}
fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "DHT node stopped")
}
