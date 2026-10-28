//! Shared inbound routing. Reservations are conservative and retained until removal
//! joins the torrent. Admission grants no discovery authority.
pub mod dht;
pub(in crate::engine) mod dial;
mod listener;
pub mod metadata;
pub mod port_map;
pub(in crate::engine) mod udp;
pub(in crate::engine) mod utp;
use crate::engine::{
    profile::Profile,
    storage::writer::RECHECK_CONCURRENCY,
    torrent::{Network, Networking, RunPolicy, TorrentHandle, TorrentOptions},
};
/// Explicit consent and persisted state for the session-owned DHT node.
pub struct DhtOptions {
    pub allow_network: bool,
    pub saved: Option<crate::engine::session::dht::SavedState>,
    /// An empty list disables router bootstrap.
    pub routers: Vec<String>,
}
use crate::protocol::{
    identity::InfoHashV1,
    peer::{HANDSHAKE_LEN, Handshake},
};
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::unix::AsyncFd,
    net::{TcpListener, TcpStream},
    sync::{Mutex as AsyncMutex, Semaphore, mpsc, watch},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Permit LAN and loopback endpoints for peers and discovery sources.
    pub allow_local_network: bool,
    pub utp: bool,
    pub torrents: usize,
    pub pending_handshakes: usize,
    /// Pending (not established) inbound connections per canonical source IP.
    pub pending_handshakes_per_ip: usize,
    /// Session-wide cap on the sum of admitted per-torrent peer reservations.
    pub peers: usize,
    pub pending_dials: usize,
    /// Sum of admitted cache and writer-queue ceilings, not total process RSS.
    pub payload_bytes: usize,
    pub handshake_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            allow_local_network: false,
            utp: true,
            // Seeds retain sockets, cache and writer reservations too; keep
            // them bounded rather than allowing unlimited seeding admission.
            torrents: 32,
            pending_handshakes: 32,
            pending_handshakes_per_ip: 2,
            peers: 32 * Profile::DEFAULT.peers,
            pending_dials: 32,
            // 32 ordinary torrents reserve 1 GiB (including up to 512 MiB
            // of upload caches). Extra headroom admits larger resource budgets.
            // Allocated on demand; peer and whole-piece scratch buffers are
            // additional RSS, not covered by this reservation ceiling.
            payload_bytes: 2 * 1024 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(15),
        }
    }
}
pub(in crate::engine) enum Inbound {
    Tcp(std::net::TcpStream),
    Utp(utp::Stream),
}
type Routes = Arc<Mutex<HashMap<InfoHashV1, mpsc::Sender<Inbound>>>>;
#[derive(Clone)]
struct Pending {
    counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
    limits: Limits,
}
struct HandshakePermit {
    pending: Pending,
    ip: IpAddr,
}
impl Pending {
    fn acquire(&self, ip: IpAddr) -> Option<HandshakePermit> {
        let ip = ip.to_canonical();
        let mut counts = self.counts.lock().unwrap();
        if counts.values().sum::<usize>() >= self.limits.pending_handshakes
            || counts.get(&ip).copied().unwrap_or(0) >= self.limits.pending_handshakes_per_ip
        {
            return None;
        }
        *counts.entry(ip).or_default() += 1;
        Some(HandshakePermit {
            pending: self.clone(),
            ip,
        })
    }
}
impl Drop for HandshakePermit {
    fn drop(&mut self) {
        let mut counts = self.pending.counts.lock().unwrap();
        let count = counts.get_mut(&self.ip).expect("pending handshake source");
        *count -= 1;
        if *count == 0 {
            counts.remove(&self.ip);
        }
    }
}
struct Owner {
    observer: crate::engine::torrent::TorrentObserver,
    join: Arc<JoinBarrier>,
    profile: Profile,
    removing: bool,
}
struct JoinBarrier {
    state: AsyncMutex<OwnerJoin>,
    joined: AtomicBool,
}
struct OwnerJoin {
    handle: TorrentHandle,
    result: Option<Result<(), Arc<io::Error>>>,
}
/// An unrouted, stopping owner. Join outside the caller's Session lock, then
/// call [`Session::finish_remove`] to release its reservation. Dropping or
/// canceling this token leaves the owner joinable by removal or shutdown.
pub struct Removal {
    hash: InfoHashV1,
    owner: Arc<JoinBarrier>,
}
impl Removal {
    pub async fn join(&self) -> io::Result<()> {
        let mut owner = self.owner.state.lock().await;
        if owner.result.is_none() {
            let result = owner.handle.join().await;
            owner.result = Some(result.map_err(Arc::new));
            self.owner.joined.store(true, Ordering::Release);
        }
        // Multiple removal/shutdown waiters share one join barrier and retain
        // its original failure domain and source, rather than joining twice.
        owner
            .result
            .as_ref()
            .expect("joined result")
            .as_ref()
            .map(|_| ())
            .map_err(|error| io::Error::new(error.kind(), error.clone()))
    }
}
pub struct Session {
    bandwidth: crate::engine::bandwidth::Budget,
    rechecks: Arc<Semaphore>,
    address: SocketAddr,
    advertised_port: watch::Sender<u16>,
    mapping_status: watch::Receiver<port_map::Status>,
    mapping_task: Option<JoinHandle<()>>,
    limits: Limits,
    routes: Routes,
    dial_queue: dial::Queue,
    tracker_http: Option<reqwest::Client>,
    tracker_udp: Option<udp::Transport>,
    udp_task: Option<JoinHandle<io::Result<()>>>,
    udp_stop: watch::Sender<bool>,
    utp: Option<Arc<Mutex<utp::Mux>>>,
    utp_task: Option<JoinHandle<()>>,
    utp_router: Option<JoinHandle<()>>,
    utp_stop: watch::Sender<bool>,
    pending: Pending,
    torrents: HashMap<InfoHashV1, Owner>,
    stop: watch::Sender<bool>,
    listener: Option<JoinHandle<io::Result<()>>>,
    dht: Option<dht::OwnedDht>,
}
impl Session {
    /// Binding the caller-supplied address is explicit network consent.
    pub async fn bind(
        address: SocketAddr,
        limits: Limits,
        port_mapping: Option<port_map::Gateway>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let client = crate::engine::discovery::http_client(limits.allow_local_network)?;
        let mut udp = udp::Transport::bind(address)?;
        let mut session = Self::new(address, limits, Some(listener));
        let (mux, incoming) = utp::Mux::new(
            udp.socket.clone(),
            session.pending.clone(),
            limits.peers + limits.pending_handshakes,
            limits.utp,
        );
        session.utp_task = Some(tokio::spawn(utp::run(
            mux.clone(),
            session.utp_stop.subscribe(),
        )));
        session.utp_router = Some(tokio::spawn(route_utp(
            incoming,
            session.routes.clone(),
            limits,
            session.stop.subscribe(),
        )));
        session.dial_queue.set_utp(limits.utp.then(|| mux.clone()));
        session.utp = Some(mux.clone());
        udp.utp = Some(mux);
        session.udp_task = Some(tokio::spawn(udp.clone().run(session.udp_stop.subscribe())));
        session.tracker_udp = Some(udp);
        session.tracker_http = Some(client);
        if let Some(gateway) = port_mapping {
            let (status, receiver) = watch::channel(port_map::Status {
                enabled: true,
                ..port_map::Status::default()
            });
            session.mapping_status = receiver;
            if address.is_ipv4() {
                session.mapping_task = Some(tokio::spawn(port_map::run(
                    address,
                    gateway,
                    session.advertised_port.clone(),
                    status,
                    session.stop.subscribe(),
                )));
            } else {
                let error = "port mapping requires an IPv4 listener";
                tracing::warn!(error, "port_mapping_disabled");
                status.send_modify(|s| s.last_error = Some(error.to_owned()));
            }
        }
        Ok(session)
    }
    /// Own rechecks without granting any network authority.
    pub fn offline(limits: Limits) -> io::Result<Self> {
        Ok(Self::new(
            "0.0.0.0:0".parse().expect("fixed address"),
            limits,
            None,
        ))
    }
    fn new(address: SocketAddr, limits: Limits, listener: Option<TcpListener>) -> Self {
        let routes = Routes::default();
        let (stop, stopping) = watch::channel(false);
        let pending = Pending {
            counts: Arc::default(),
            limits,
        };
        let task = listener.map(|listener| {
            tokio::spawn(route(listener, routes.clone(), pending.clone(), stopping))
        });
        Self {
            bandwidth: crate::engine::bandwidth::Budget::default(),
            rechecks: Arc::new(Semaphore::new(RECHECK_CONCURRENCY)),
            address,
            advertised_port: watch::channel(address.port()).0,
            mapping_status: watch::channel(port_map::Status::default()).1,
            mapping_task: None,
            limits,
            routes,
            dial_queue: dial::Queue::new(limits.pending_dials, limits.allow_local_network),
            tracker_http: None,
            tracker_udp: None,
            udp_task: None,
            udp_stop: watch::channel(false).0,
            utp: None,
            utp_task: None,
            utp_router: None,
            utp_stop: watch::channel(false).0,
            pending,
            torrents: HashMap::new(),
            stop,
            listener: task,
            dht: None,
        }
    }
    pub fn metadata_network(&self) -> io::Result<metadata::Network> {
        if *self.stop.borrow() || self.listener.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "session networking disabled",
            ));
        }
        Ok(metadata::Network {
            port: self.advertised_port.subscribe(),
            dial_queue: self.dial_queue.clone(),
            tracker_http: self.tracker_http.clone().expect("online session"),
            tracker_udp: self.tracker_udp.clone().expect("online session"),
            allow_local_network: self.limits.allow_local_network,
            dht: self.dht.as_ref().map(|d| d.handle.clone()),
            stop: self.stop.subscribe(),
        })
    }
    pub fn port_mapping(&self) -> port_map::Status {
        self.mapping_status.borrow().clone()
    }
    pub fn advertised_port(&self) -> u16 {
        *self.advertised_port.borrow()
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub fn torrent(&self, hash: InfoHashV1) -> Option<crate::engine::torrent::TorrentObserver> {
        self.torrents
            .get(&hash)
            .filter(|owner| !owner.removing)
            .map(|owner| owner.observer.clone())
    }
    pub fn admit(&mut self, options: TorrentOptions) -> io::Result<InfoHashV1> {
        let hash = options.metainfo.info_hash();
        let profile = options.profile;
        let recheck = matches!(options.run, RunPolicy::Recheck);
        let shared_dht = options.dht.as_ref().is_none_or(|handle| {
            self.dht.as_ref().is_some_and(|dht| {
                dht.handle.id() == handle.id() && dht.handle.local_addr() == handle.local_addr()
            })
        });
        if *self.stop.borrow()
            || (self.listener.is_none() && !recheck)
            || !shared_dht
            || self.torrents.contains_key(&hash)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session stopped, duplicate torrent or networking unavailable",
            ));
        }
        let profiles = self
            .torrents
            .values()
            .map(|owner| &owner.profile)
            .chain(std::iter::once(&profile));
        let mut peers = 0usize;
        let mut payload = 0usize;
        for p in profiles {
            peers = peers.saturating_add(p.peers);
            payload = payload
                .saturating_add(p.cache_bytes)
                .saturating_add(p.write_queue_bytes);
        }
        if self.torrents.len() >= self.limits.torrents
            || peers > self.limits.peers
            || payload > self.limits.payload_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "session reservation limit",
            ));
        }
        // Absorb handshake bursts up to the torrent's admitted peer budget.
        let (sender, sockets) = mpsc::channel(profile.peers);
        let bandwidth = crate::engine::bandwidth::Throttle::new(
            self.bandwidth.clone(),
            crate::engine::bandwidth::Budget::new(options.settings.bandwidth()),
        );
        let networking = if recheck {
            Networking::Recheck
        } else {
            Networking::Online(Network {
                advertised_port: self.advertised_port.subscribe(),
                sockets,
                dial_queue: self.dial_queue.clone(),
                tracker_http: self.tracker_http.clone().expect("online session"),
                tracker_udp: self.tracker_udp.clone().expect("online session"),
                allow_local_network: self.limits.allow_local_network,
            })
        };
        let handle = options.spawn_session(bandwidth, networking, self.rechecks.clone())?;
        self.routes.lock().unwrap().insert(hash, sender);
        self.torrents.insert(
            hash,
            Owner {
                observer: handle.observer(),
                join: Arc::new(JoinBarrier {
                    state: AsyncMutex::new(OwnerJoin {
                        handle,
                        result: None,
                    }),
                    joined: AtomicBool::new(false),
                }),
                profile,
                removing: false,
            },
        );
        Ok(hash)
    }
    /// Unroute and mark removal under the caller's Session lock. Admission
    /// still counts this owner's reservation until finish_remove after join.
    pub fn take(&mut self, hash: InfoHashV1) -> Option<Removal> {
        self.routes.lock().unwrap().remove(&hash);
        let owner = self.torrents.get_mut(&hash)?;
        owner.removing = true;
        owner.observer.request_stop();
        Some(Removal {
            hash,
            owner: owner.join.clone(),
        })
    }
    /// Release only the matching owner's reservation, after its join (including
    /// error results). Returns false if not joined or already removed.
    pub fn finish_remove(&mut self, removal: &Removal) -> bool {
        let joined = removal.owner.joined.load(Ordering::Acquire);
        if joined
            && self
                .torrents
                .get(&removal.hash)
                .is_some_and(|owner| Arc::ptr_eq(&owner.join, &removal.owner))
        {
            self.torrents.remove(&removal.hash);
            return true;
        }
        false
    }
    pub async fn remove(&mut self, hash: InfoHashV1) -> io::Result<()> {
        if let Some(removal) = self.take(hash) {
            let result = removal.join().await;
            self.finish_remove(&removal);
            result?;
        }
        Ok(())
    }
    pub async fn shutdown(&mut self) -> io::Result<()> {
        let mut failure = None;
        self.stop.send_replace(true);
        if let Some(mux) = &self.utp {
            mux.lock().unwrap().stop_accepting();
        }
        if let Some(task) = self.utp_router.take()
            && let Err(error) = task.await
        {
            failure = Some(io::Error::other(error));
        }
        self.routes.lock().unwrap().clear();
        for owner in self.torrents.values() {
            owner.observer.request_stop();
        }
        if let Some(task) = self.mapping_task.take()
            && let Err(error) = task.await
        {
            tracing::warn!(%error, "port_mapping_task_failed");
        }
        if let Some(task) = self.listener.as_mut() {
            let result = task.await.map_err(io::Error::other).and_then(|r| r);
            self.listener = None;
            if let Err(error) = result {
                failure = Some(error);
            }
        }
        let hashes: Vec<_> = self.torrents.keys().copied().collect();
        for hash in hashes {
            let removal = self.take(hash).expect("reserved owner");
            let result = removal.join().await;
            self.finish_remove(&removal);
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        self.utp_stop.send_replace(true);
        if let Some(task) = self.utp_task.take()
            && let Err(error) = task.await
        {
            failure.get_or_insert_with(|| io::Error::other(error));
        }
        if let Some(dht) = &mut self.dht
            && let Err(error) = dht.shutdown().await
        {
            failure.get_or_insert(error);
        }
        // Stopped announces still need replies while torrents join.
        self.udp_stop.send_replace(true);
        if let Some(task) = self.udp_task.as_mut() {
            if let Err(error) = task.await.map_err(io::Error::other).and_then(|r| r) {
                failure.get_or_insert(error);
            }
            self.udp_task = None;
        }
        failure.map_or(Ok(()), Err)
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        self.utp_stop.send_replace(true);
        self.udp_stop.send_replace(true);
        for owner in self.torrents.values() {
            owner.observer.request_stop();
        }
    }
}
async fn route(
    listener: TcpListener,
    routes: Routes,
    admission: Pending,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let limits = admission.limits;
    let mut pending = JoinSet::new();
    let mut accepts = listener::AcceptBackoff::new("session");
    let result = loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break Ok(()),
            completed = pending.join_next(), if !pending.is_empty() => {
                completed.expect("pending handshake").expect("handshake task");
            },
            accepted = accepts.accept(|| listener.accept()), if pending.len() < limits.pending_handshakes => {
                let (socket, source) = match accepted { Ok(value) => value, Err(error) => break Err(error) };
                let Some(permit) = admission.acquire(source.ip()) else { continue; };
                let routes = routes.clone();
                pending.spawn(async move {
                    let _permit = permit;
                    if let Ok(Ok((hash, socket))) = tokio::time::timeout(limits.handshake_timeout, peek_hash(socket)).await
                        && let Some(sender) = routes.lock().unwrap().get(&hash) {
                        // Deregister from the listener's reactor; the pinned
                        // torrent registers this nonblocking socket on its shard.
                        if let Ok(socket) = socket.into_std() {
                            let _ = sender.try_send(Inbound::Tcp(socket));
                        }
                    }
                });
            }
        }
    };
    pending.abort_all();
    while pending.join_next().await.is_some() {}
    result
}
async fn route_utp(
    mut incoming: mpsc::Receiver<(utp::Stream, HandshakePermit)>,
    routes: Routes,
    limits: Limits,
    mut stop: watch::Receiver<bool>,
) {
    let mut pending = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            completed = pending.join_next(), if !pending.is_empty() => {
                completed.expect("pending uTP handshake").expect("uTP routing task");
            },
            accepted = incoming.recv() => {
                let Some((socket, permit)) = accepted else { break; };
                let routes = routes.clone();
                pending.spawn(async move {
                    let _permit = permit;
                    let mut bytes = [0; HANDSHAKE_LEN];
                    if let Ok(Ok(HANDSHAKE_LEN)) = tokio::time::timeout(limits.handshake_timeout, socket.peek(&mut bytes)).await
                        && let Ok((handshake, _)) = Handshake::decode(&bytes)
                    {
                        let sender = routes.lock().unwrap().get(&InfoHashV1::from_bytes(handshake.info_hash)).cloned();
                        if let Some(sender) = sender {
                            match sender.try_send(Inbound::Utp(socket)) {
                                Ok(()) => return,
                                Err(error) => { if let Inbound::Utp(socket) = error.into_inner() { socket.reset(); } return; }
                            }
                        }
                    }
                    socket.reset();
                });
            }
        }
    }
    pending.abort_all();
    while pending.join_next().await.is_some() {}
}
async fn peek_hash(socket: TcpStream) -> io::Result<(InfoHashV1, TcpStream)> {
    // Own the readiness guard while routing: TcpStream::peek retains readable
    // readiness for an unchanged prefix and would spin on fragmented input.
    // Tokio's replacement registration API requires unsafe. The deprecated
    // safe constructor is sound for this owned std TcpStream: its descriptor
    // cannot be replaced or closed while registered, and we never use get_mut.
    #[allow(deprecated)]
    let socket = AsyncFd::new(socket.into_std()?)?;
    let mut bytes = [0; HANDSHAKE_LEN];
    loop {
        let mut ready = socket.readable().await?;
        let closed = ready.ready().is_read_closed();
        // Clear BEFORE peeking. Bytes arriving before the clear are included in
        // this peek; bytes arriving after it produce another readiness event.
        // Clearing after a partial peek could lose the final fragment's wakeup.
        ready.clear_ready();
        let length = match socket.get_ref().peek(&mut bytes) {
            Ok(length) => length,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        };
        if length == bytes.len() {
            let (handshake, _) = Handshake::decode(&bytes).map_err(io::Error::other)?;
            return Ok((
                InfoHashV1::from_bytes(handshake.info_hash),
                TcpStream::from_std(socket.into_inner())?,
            ));
        }
        if length == 0 || closed {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
}
