//! Iterative BEP 5 lookups: alpha=3, eight live results, bounded fallback candidates.
//! Futures are polled in the torrent owner; cancellation spawns/abandons no tasks.
use super::{
    Handle, NodeId, NodeV4, Query, Reply,
    routing::{self, K},
};
use protocol::{identity::InfoHashV1, metainfo::OwnedMetainfo};
use std::{
    collections::{HashMap, HashSet},
    future::{Future, poll_fn},
    io,
    net::SocketAddrV4,
    pin::Pin,
    task::Poll,
    time::Duration,
};
use tokio::time::Instant;

const ALPHA: usize = 3;
const HEDGE_AFTER: Duration = Duration::from_millis(1500);
const MAX_ACTIVE: usize = ALPHA * 2;
const REANNOUNCE: Duration = Duration::from_secs(15 * 60);
const RETRY: Duration = Duration::from_secs(30);
#[derive(Debug)]
pub(crate) struct Lookup {
    pub(crate) peers: Vec<SocketAddrV4>,
    pub(crate) queried: usize,
    pub(crate) announced: usize,
}
struct Candidate {
    node: NodeV4,
    verified: bool,
    token: Option<Vec<u8>>,
}
type Request = Pin<Box<dyn Future<Output = (SocketAddrV4, io::Result<Reply>)> + Send>>;

/// Slow requests stay alive for late replies, but stop occupying an alpha slot.
/// The separate hard cap bounds speculative work and global pending pressure.
#[derive(Default)]
pub(super) struct Requests {
    active: Vec<(Request, Instant)>,
}
impl Requests {
    pub(crate) fn can_start(&self) -> bool {
        let now = Instant::now();
        self.active.len() < MAX_ACTIVE
            && self
                .active
                .iter()
                .filter(|(_, started)| now.duration_since(*started) < HEDGE_AFTER)
                .count()
                < ALPHA
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty()
    }
    pub(crate) fn push(&mut self, handle: Handle, address: SocketAddrV4, query: Query) {
        self.active.push((
            Box::pin(async move { (address, handle.request_wait(address, query).await) }),
            Instant::now(),
        ));
    }
    pub(crate) async fn next(&mut self) -> Option<(SocketAddrV4, io::Result<Reply>)> {
        let now = Instant::now();
        let hedge = if self.active.len() < MAX_ACTIVE {
            self.active
                .iter()
                .map(|(_, started)| *started + HEDGE_AFTER)
                .filter(|due| *due > now)
                .min()
        } else {
            None
        };
        tokio::select! {
            result = poll_fn(|cx| {
                for (index, (future, _)) in self.active.iter_mut().enumerate() {
                    if let Poll::Ready(result) = future.as_mut().poll(cx) {
                        return Poll::Ready((index, result));
                    }
                }
                Poll::Pending
            }) => {
                let (index, result) = result;
                drop(self.active.swap_remove(index));
                Some(result)
            }
            _ = async {
                match hedge {
                    Some(due) => tokio::time::sleep_until(due).await,
                    None => std::future::pending().await,
                }
            } => None,
        }
    }
}
impl Handle {
    async fn discover_stream(
        &self,
        meta: &OwnedMetainfo,
        port: tokio::sync::watch::Receiver<u16>,
        peers: Option<tokio::sync::mpsc::Sender<SocketAddrV4>>,
    ) -> io::Result<Lookup> {
        if meta.is_private() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private torrents cannot use DHT",
            ));
        }
        let _permit = self
            .lookups
            .try_acquire()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "DHT lookup limit"))?;
        self.lookup(meta.info_hash(), Some(port), peers).await
    }
    pub(crate) async fn metadata_peers(
        &self,
        hash: InfoHashV1,
        peers: tokio::sync::mpsc::Sender<SocketAddrV4>,
    ) -> io::Result<Lookup> {
        let _permit = self
            .lookups
            .try_acquire()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "DHT lookup limit"))?;
        self.lookup(hash, None, Some(peers)).await
    }
    async fn lookup(
        &self,
        hash: InfoHashV1,
        port: Option<tokio::sync::watch::Receiver<u16>>,
        peer_stream: Option<tokio::sync::mpsc::Sender<SocketAddrV4>>,
    ) -> io::Result<Lookup> {
        let target = NodeId(*hash.as_bytes());
        let mut closest: Vec<_> = self
            .closest(target)
            .await?
            .into_iter()
            .take(self.lookup_queries)
            .map(|node| Candidate {
                node,
                verified: true,
                token: None,
            })
            .collect();
        let mut peers: Vec<_> = self
            .local_peers(hash)
            .await?
            .into_iter()
            .filter(|p| port.is_none() || *p != self.address)
            .take(self.max_peers)
            .collect();
        if let Some(stream) = &peer_stream {
            for &peer in &peers {
                let _ = stream.try_send(peer);
            }
        }
        if closest.is_empty() {
            if !peers.is_empty() {
                return Ok(Lookup {
                    peers,
                    queried: 0,
                    announced: 0,
                });
            }
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "no verified DHT routing contacts",
            ));
        }
        let mut asked = HashSet::new();
        let mut responded_nodes: HashMap<SocketAddrV4, NodeId> = HashMap::new();
        let mut active = Requests::default();
        let mut responded = 0;
        loop {
            // Once the closest K responsive nodes are known, only closer
            // candidates can improve the result. Failed referrals never erase fallbacks.
            let mut live: Vec<_> = responded_nodes.values().copied().collect();
            live.sort_unstable_by_key(|id| routing::distance(*id, target));
            let cutoff = live.get(K - 1).map(|id| routing::distance(*id, target));
            while active.can_start() && asked.len() < self.lookup_queries {
                let Some(node) = closest.iter().map(|c| c.node).find(|n| {
                    !asked.contains(&n.address)
                        && n.id != self.id()
                        && cutoff.is_none_or(|cutoff| routing::distance(n.id, target) < cutoff)
                }) else {
                    break;
                };
                asked.insert(node.address);
                active.push(self.clone(), node.address, Query::GetPeers(hash));
            }
            if active.is_empty() {
                break;
            }
            let Some((address, result)) = active.next().await else {
                continue;
            };
            match result {
                Ok(reply) => {
                    responded += 1;
                    responded_nodes.insert(address, reply.id);
                    for peer in reply.peers {
                        if (port.is_none() || peer != self.address)
                            && peers.len() < self.max_peers
                            && !peers.contains(&peer)
                        {
                            peers.push(peer);
                            if let Some(stream) = &peer_stream {
                                let _ = stream.try_send(peer);
                            }
                        }
                    }
                    closest.retain(|c| c.node.address != address);
                    closest.push(Candidate {
                        node: NodeV4 {
                            id: reply.id,
                            address,
                        },
                        verified: true,
                        token: reply.token,
                    });
                    for contact in reply.nodes {
                        if contact.id != self.id()
                            && !closest.iter().any(|c| {
                                c.node.id == contact.id || c.node.address == contact.address
                            })
                        {
                            closest.push(Candidate {
                                node: contact,
                                verified: false,
                                token: None,
                            });
                        }
                    }
                    closest.sort_unstable_by_key(|c| routing::distance(c.node.id, target));
                    // Bound referrals independently of the K-node result set.
                    // Never evict verified fallbacks or queried/in-flight entries.
                    let mut unasked = 0;
                    closest.retain(|c| {
                        if c.verified || asked.contains(&c.node.address) {
                            return true;
                        }
                        unasked += 1;
                        unasked <= self.lookup_queries
                    });
                }
                Err(_) => closest.retain(|c| c.node.address != address),
            }
        }
        if responded == 0 && peers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DHT lookup found no responsive nodes",
            ));
        }
        let mut announced = 0;
        for candidate in closest.into_iter().filter(|c| c.token.is_some()).take(K) {
            if let (Some(port), Some(token)) = (port.as_ref().map(|p| *p.borrow()), candidate.token)
                && self
                    .request_wait(
                        candidate.node.address,
                        Query::AnnouncePeer { hash, port, token },
                    )
                    .await
                    .is_ok()
            {
                announced += 1;
            }
        }
        Ok(Lookup {
            peers,
            queried: asked.len(),
            announced,
        })
    }
}
pub(crate) enum DiscoveryEvent {
    Peers(Vec<SocketAddrV4>),
    Finished(Lookup),
}
pub(crate) struct Discovery {
    handle: Handle,
    meta: OwnedMetainfo,
    port: tokio::sync::watch::Receiver<u16>,
    due: Instant,
    pending: Option<Pin<Box<dyn Future<Output = io::Result<Lookup>> + Send>>>,
    peers: Option<tokio::sync::mpsc::Receiver<SocketAddrV4>>,
    last_lookup: Option<Instant>,
    last_queried: usize,
    last_announced: usize,
    last_peers: usize,
    last_error: Option<String>,
}
impl Discovery {
    pub(crate) fn new(
        handle: Handle,
        meta: OwnedMetainfo,
        port: tokio::sync::watch::Receiver<u16>,
    ) -> io::Result<Self> {
        if meta.is_private() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private torrents cannot use DHT",
            ));
        }
        Ok(Self {
            handle,
            meta,
            peers: None,
            port,
            due: Instant::now(),
            pending: None,
            last_lookup: None,
            last_queried: 0,
            last_announced: 0,
            last_peers: 0,
            last_error: None,
        })
    }
    pub(crate) fn snapshot(&self) -> crate::torrent::telemetry::DhtStatus {
        crate::torrent::telemetry::DhtStatus {
            lookup_pending: self.pending.is_some(),
            next_lookup_ms: self
                .due
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(u64::MAX as u128) as u64,
            last_lookup_ago_ms: self
                .last_lookup
                .map(|t| t.elapsed().as_millis().min(u64::MAX as u128) as u64),
            queried: self.last_queried,
            announced: self.last_announced,
            peers_returned: self.last_peers,
            error: self.last_error.clone(),
        }
    }
    async fn start(&mut self) {
        if self.pending.is_none() {
            tokio::time::sleep_until(self.due).await;
            let (sender, peers) = tokio::sync::mpsc::channel(self.handle.max_peers.max(1));
            self.peers = Some(peers);
            let (handle, meta, port) = (self.handle.clone(), self.meta.clone(), self.port.clone());
            self.pending = Some(Box::pin(async move {
                handle.discover_stream(&meta, port, Some(sender)).await
            }));
        }
    }
    pub(crate) async fn next_event(&mut self) -> io::Result<DiscoveryEvent> {
        self.start().await;
        tokio::select! {
            biased;
            Some(peer) = async {
                match &mut self.peers {
                    Some(peers) => peers.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                let mut batch = vec![peer];
                if let Some(peers) = &mut self.peers {
                    while let Ok(peer) = peers.try_recv() {
                        batch.push(peer);
                    }
                }
                Ok(DiscoveryEvent::Peers(batch))
            },
            result = self.pending.as_mut().expect("lookup present") => {
                self.finish(&result);
                result.map(DiscoveryEvent::Finished)
            }
        }
    }
    fn finish(&mut self, result: &io::Result<Lookup>) {
        self.pending = None;
        self.peers = None;
        self.last_lookup = Some(Instant::now());
        self.last_error = result.as_ref().err().map(|e| format!("{:?}", e.kind()));
        (self.last_queried, self.last_announced, self.last_peers) = result
            .as_ref()
            .map(|r| (r.queried, r.announced, r.peers.len()))
            .unwrap_or_default();
        self.due = Instant::now()
            + if result.as_ref().is_ok_and(|r| r.announced > 0) {
                REANNOUNCE
            } else {
                RETRY
            };
    }
}
