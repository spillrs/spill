//! Shared networking, resource budgets, and execution ownership.
//!
//! Bind a [`Session`], then [`Session::admit`] torrents. Admission reserves budgets
//! through joined cleanup; [`Session::shutdown`] stops and joins owned work.
//! Unlike [`super::download`], default reusable sessions do not enable DHT.
pub mod dht;
pub(in crate::engine) mod dial;
mod listener;
pub(in crate::engine) mod metadata;
pub mod port_map;
pub(in crate::engine) mod udp;
pub(in crate::engine) mod utp;
use crate::engine::{
    profile::Profile,
    storage::writer::RECHECK_CONCURRENCY,
    torrent::{
        Destination, Network, Networking, RunPolicy, TorrentHandle, TorrentOptions, TorrentSource,
    },
};
/// Explicit consent and persisted state for the session-owned DHT node.
pub struct DhtOptions {
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
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::unix::AsyncFd,
    net::{TcpListener, TcpStream},
    sync::{Mutex as AsyncMutex, Semaphore, mpsc, watch},
    task::{JoinHandle, JoinSet},
};

/// Network permissions and listeners. Defaults use an ephemeral IPv4 port and uTP,
/// with no LAN access, router mapping, or DHT.
pub struct NetworkOptions {
    /// Bind TCP and UDP to the same address and port; zero selects a free port.
    pub listen: SocketAddr,
    /// Accept uTP and try it before TCP when dialing.
    pub utp: bool,
    /// Permit outbound private, loopback, and link-local destinations.
    pub allow_local_network: bool,
    /// Opt into IPv4 NAT-PMP/UPnP mapping.
    pub port_mapping: Option<port_map::Gateway>,
    /// Opt into the IPv4 DHT with supplied routers and optional saved state.
    pub dht: Option<DhtOptions>,
}
impl Default for NetworkOptions {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 0)),
            utp: true,
            allow_local_network: false,
            port_mapping: None,
            dht: None,
        }
    }
}
/// Session-wide admission budgets. Payload reservations are not a total RSS cap;
/// transport buffers, hashing scratch space, and other bookkeeping are additional.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
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
    handle: TorrentHandle,
    profile: Profile,
    identity: Arc<()>,
}
pub(in crate::engine) struct Retirement {
    registry: std::sync::Weak<Mutex<Registry>>,
    routes: Routes,
    hash: InfoHashV1,
    identity: Arc<()>,
}
impl Retirement {
    pub(in crate::engine) fn retire(&self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut registry = registry.lock().unwrap();
            if registry
                .torrents
                .get(&self.hash)
                .is_some_and(|owner| Arc::ptr_eq(&owner.identity, &self.identity))
            {
                self.routes.lock().unwrap().remove(&self.hash);
                registry.torrents.remove(&self.hash);
            }
        }
    }
}
/// Clones share executions. Last-clone drop requests stop; shutdown joins cleanup.
#[derive(Clone)]
pub struct Session {
    shared: Arc<Shared>,
}
struct Registry {
    torrents: HashMap<InfoHashV1, Owner>,
    closed: bool,
}
impl Registry {
    fn is_closed(&mut self) -> bool {
        self.closed |= self
            .torrents
            .values()
            .any(|owner| owner.handle.cleanup_unproven());
        self.closed
    }
}
struct Tasks {
    mapping: Option<JoinHandle<()>>,
    udp: Option<JoinHandle<io::Result<()>>>,
    utp: Option<JoinHandle<()>>,
    utp_router: Option<JoinHandle<io::Result<()>>>,
    listener: Option<JoinHandle<io::Result<()>>>,
    tcp_handshakes: Arc<AsyncMutex<JoinSet<()>>>,
    utp_handshakes: Arc<AsyncMutex<JoinSet<()>>>,
    failure: Option<Arc<io::Error>>,
    joined: bool,
}
impl Tasks {
    fn record(&mut self, result: io::Result<()>) {
        if let Err(error) = result {
            self.failure.get_or_insert_with(|| Arc::new(error));
        }
    }
    fn result(&self) -> io::Result<()> {
        self.failure.as_ref().map_or(Ok(()), |error| {
            Err(io::Error::new(error.kind(), error.clone()))
        })
    }
}
struct Shared {
    bandwidth: crate::engine::bandwidth::Budget,
    rechecks: Arc<Semaphore>,
    address: SocketAddr,
    advertised_port: watch::Sender<u16>,
    mapping_status: watch::Receiver<port_map::Status>,
    limits: Limits,
    routes: Routes,
    dial_queue: dial::Queue,
    tracker_http: Option<reqwest::Client>,
    tracker_udp: Option<udp::Transport>,
    udp_stop: watch::Sender<bool>,
    utp: Option<Arc<Mutex<utp::Mux>>>,
    utp_stop: watch::Sender<bool>,
    pending: Pending,
    registry: Arc<Mutex<Registry>>,
    dht: Option<Arc<dht::Dht>>,
    allow_local_network: bool,
    tasks: AsyncMutex<Tasks>,
    stop: watch::Sender<bool>,
}
impl Session {
    /// Binding the caller-supplied address is explicit network consent.
    pub async fn bind(network: NetworkOptions, limits: Limits) -> io::Result<Self> {
        if let Some(dht) = &network.dht {
            dht.validate(network.listen)?;
        }
        let listener = TcpListener::bind(network.listen).await?;
        let address = listener.local_addr()?;
        let client = crate::engine::discovery::http_client(network.allow_local_network)?;
        let mut udp = udp::Transport::bind(address)?;
        let mut session = Shared::new(address, limits, network.allow_local_network, Some(listener));
        session.dht = network.dht.map(|options| {
            Arc::new(dht::Dht::new(
                options,
                udp.clone(),
                network.allow_local_network,
                Arc::downgrade(&session.registry),
                session.stop.subscribe(),
            ))
        });
        let (mux, incoming) = utp::Mux::new(
            udp.socket.clone(),
            session.pending.clone(),
            limits.peers + limits.pending_handshakes,
            network.utp,
        );
        session.tasks.get_mut().utp = Some(tokio::spawn(utp::run(
            mux.clone(),
            session.utp_stop.subscribe(),
        )));
        session.tasks.get_mut().utp_router = Some(tokio::spawn(route_utp(
            incoming,
            session.routes.clone(),
            limits,
            session.stop.subscribe(),
            session.tasks.get_mut().utp_handshakes.clone(),
        )));
        session.dial_queue.set_utp(network.utp.then(|| mux.clone()));
        session.utp = Some(mux.clone());
        udp.utp = Some(mux);
        session.tasks.get_mut().udp =
            Some(tokio::spawn(udp.clone().run(session.udp_stop.subscribe())));
        session.tracker_udp = Some(udp);
        session.tracker_http = Some(client);
        if let Some(gateway) = network.port_mapping {
            let (status, receiver) = watch::channel(port_map::Status {
                enabled: true,
                ..port_map::Status::default()
            });
            session.mapping_status = receiver;
            if address.is_ipv4() {
                session.tasks.get_mut().mapping = Some(tokio::spawn(port_map::run(
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
        Ok(Self {
            shared: Arc::new(session),
        })
    }
    /// Own rechecks without granting any network authority.
    pub fn offline(limits: Limits) -> Self {
        Self {
            shared: Arc::new(Shared::new(
                "0.0.0.0:0".parse().expect("fixed address"),
                limits,
                false,
                None,
            )),
        }
    }
}
impl Shared {
    fn new(
        address: SocketAddr,
        limits: Limits,
        allow_local_network: bool,
        listener: Option<TcpListener>,
    ) -> Self {
        let routes = Routes::default();
        let (stop, stopping) = watch::channel(false);
        let pending = Pending {
            counts: Arc::default(),
            limits,
        };
        let tcp_handshakes = Arc::new(AsyncMutex::new(JoinSet::new()));
        let task = listener.map(|listener| {
            tokio::spawn(route(
                listener,
                routes.clone(),
                pending.clone(),
                stopping,
                tcp_handshakes.clone(),
            ))
        });
        Self {
            bandwidth: crate::engine::bandwidth::Budget::default(),
            rechecks: Arc::new(Semaphore::new(RECHECK_CONCURRENCY)),
            address,
            advertised_port: watch::channel(address.port()).0,
            mapping_status: watch::channel(port_map::Status::default()).1,
            limits,
            routes,
            dial_queue: dial::Queue::new(limits.pending_dials, allow_local_network),
            tracker_http: None,
            tracker_udp: None,
            udp_stop: watch::channel(false).0,
            utp: None,
            utp_stop: watch::channel(false).0,
            pending,
            registry: Arc::new(Mutex::new(Registry {
                torrents: HashMap::new(),
                closed: false,
            })),
            dht: None,
            allow_local_network,
            tasks: AsyncMutex::new(Tasks {
                mapping: None,
                udp: None,
                utp: None,
                utp_router: None,
                listener: task,
                tcp_handshakes,
                utp_handshakes: Arc::new(AsyncMutex::new(JoinSet::new())),
                failure: None,
                joined: false,
            }),
            stop,
        }
    }
}
impl Session {
    pub fn port_mapping(&self) -> port_map::Status {
        self.shared.mapping_status.borrow().clone()
    }
    pub fn advertised_port(&self) -> u16 {
        *self.shared.advertised_port.borrow()
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.shared.address
    }
    /// True after shutdown, or permanently once an execution finished without
    /// cleanup proof: its payload authority stays quarantined until restart.
    pub fn admission_closed(&self) -> bool {
        self.shared.registry.lock().unwrap().is_closed()
    }
    pub fn torrent(&self, hash: InfoHashV1) -> Option<TorrentHandle> {
        self.shared
            .registry
            .lock()
            .unwrap()
            .torrents
            .get(&hash)
            .map(|owner| owner.handle.clone())
    }
    pub fn torrents(&self) -> Vec<TorrentHandle> {
        self.shared
            .registry
            .lock()
            .unwrap()
            .torrents
            .values()
            .map(|owner| owner.handle.clone())
            .collect()
    }
    /// Resolve magnet metadata without creating payload files. The returned
    /// handle supports metadata observation and joined cleanup, not content completion.
    pub fn inspect(
        &self,
        magnet: crate::protocol::magnet::Magnet,
        profile: Profile,
        settings: crate::engine::settings::TorrentSettings,
    ) -> io::Result<TorrentHandle> {
        let mut options = TorrentOptions::new(
            TorrentSource::Magnet(magnet),
            Destination::Open(Default::default()),
        );
        options.profile = profile;
        options.settings = settings;
        self.admit_execution(options, true)
    }
    /// Validate and reserve a torrent execution; preparation proceeds asynchronously.
    /// Returns `WouldBlock` when budgets are exhausted and `InvalidInput` for
    /// invalid options, duplicate torrents, or a closed session. Finished executions
    /// retire automatically; stopped handles cannot control a later replacement.
    pub fn admit(&self, options: TorrentOptions) -> io::Result<TorrentHandle> {
        self.admit_execution(options, false)
    }
    fn admit_execution(
        &self,
        options: TorrentOptions,
        inspection: bool,
    ) -> io::Result<TorrentHandle> {
        options.validate()?;
        let shared = &self.shared;
        let magnet = matches!(options.source, TorrentSource::Magnet(_));
        if magnet && shared.tracker_http.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offline verification requires metainfo",
            ));
        }
        let mut registry = shared.registry.lock().unwrap();
        let hash = options.source.info_hash();
        let profile = options.profile;
        let recheck = matches!(options.run, RunPolicy::Recheck);
        if registry.is_closed()
            || (shared.tracker_http.is_none() && !recheck)
            || registry.torrents.contains_key(&hash)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session stopped, duplicate torrent or networking unavailable",
            ));
        }
        let mut available_peers = shared.limits.peers;
        let mut available_payload = shared.limits.payload_bytes;
        for owner in registry.torrents.values() {
            available_peers -= owner.profile.peers;
            available_payload -= owner.profile.cache_bytes + owner.profile.write_queue_bytes;
        }
        if registry.torrents.len() >= shared.limits.torrents
            || profile.peers > available_peers
            || profile.cache_bytes + profile.write_queue_bytes > available_payload
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "session reservation limit",
            ));
        }
        // Absorb handshake bursts up to the torrent's admitted peer budget.
        let (sender, sockets) = mpsc::channel(profile.peers);
        let bandwidth = crate::engine::bandwidth::Throttle::new(
            shared.bandwidth.clone(),
            crate::engine::bandwidth::Budget::new(options.settings.bandwidth()),
        );
        let networking = if recheck {
            Networking::Recheck
        } else {
            Networking::Online(Network {
                advertised_port: shared.advertised_port.subscribe(),
                sockets,
                dial_queue: shared.dial_queue.clone(),
                tracker_http: shared.tracker_http.clone().expect("online session"),
                tracker_udp: shared.tracker_udp.clone().expect("online session"),
                allow_local_network: shared.allow_local_network,
                dht: shared.dht.clone(),
            })
        };
        let metadata = magnet.then(|| metadata::Network {
            port: shared.advertised_port.subscribe(),
            dial_queue: shared.dial_queue.clone(),
            tracker_http: shared.tracker_http.clone().expect("online session"),
            tracker_udp: shared.tracker_udp.clone().expect("online session"),
            allow_local_network: shared.allow_local_network,
            dht: shared.dht.clone(),
        });
        let identity = Arc::new(());
        let retirement = Retirement {
            registry: Arc::downgrade(&shared.registry),
            routes: shared.routes.clone(),
            hash,
            identity: identity.clone(),
        };
        let (handle, start) = TorrentHandle::spawn(
            options,
            bandwidth,
            networking,
            metadata,
            shared.rechecks.clone(),
            retirement,
            inspection,
        );
        if !recheck && !inspection {
            shared.routes.lock().unwrap().insert(hash, sender);
        }
        registry.torrents.insert(
            hash,
            Owner {
                handle: handle.clone(),
                profile,
                identity,
            },
        );
        start.send(()).expect("installed execution");
        Ok(handle)
    }
    /// Request stop without waiting. Use [`Self::shutdown`] to join cleanup.
    pub fn request_shutdown(&self) {
        self.shared.request_stop();
    }
    /// Stop and join owned work, including blocking I/O and persistence callbacks.
    /// There is no hard deadline. Cancelling this wait does not discard join state;
    /// another retained session clone can await it again. Drop all clones after
    /// joined shutdown to release the listener. Errors retain joined cleanup state.
    pub async fn shutdown(&self) -> io::Result<()> {
        self.request_shutdown();
        let shared = &self.shared;
        let mut tasks = shared.tasks.lock().await;
        if tasks.joined {
            return tasks.result();
        }
        if let Some(task) = tasks.utp_router.as_mut() {
            let result = task.await.map_err(io::Error::other).and_then(|r| r);
            tasks.utp_router = None;
            tasks.record(result);
        }
        if let Some(task) = tasks.mapping.as_mut() {
            let result = task.await.map_err(io::Error::other);
            tasks.mapping = None;
            tasks.record(result);
        }
        if let Some(task) = tasks.listener.as_mut() {
            let result = task.await.map_err(io::Error::other).and_then(|r| r);
            tasks.listener = None;
            tasks.record(result);
        }
        let result = drain_handshakes(&mut *tasks.tcp_handshakes.lock().await).await;
        tasks.record(result);
        let result = drain_handshakes(&mut *tasks.utp_handshakes.lock().await).await;
        tasks.record(result);
        let handles: Vec<_> = shared
            .registry
            .lock()
            .unwrap()
            .torrents
            .values()
            .map(|owner| owner.handle.clone())
            .collect();
        for handle in handles {
            tasks.record(handle.join().await);
        }
        shared.utp_stop.send_replace(true);
        if let Some(task) = tasks.utp.as_mut() {
            let result = task.await.map_err(io::Error::other);
            tasks.utp = None;
            tasks.record(result);
        }
        if let Some(dht) = &shared.dht {
            let result = dht.shutdown().await;
            tasks.record(result);
        }
        // Stopped announces still need replies while torrents join.
        shared.udp_stop.send_replace(true);
        if let Some(task) = tasks.udp.as_mut() {
            let result = task.await.map_err(io::Error::other).and_then(|r| r);
            tasks.udp = None;
            tasks.record(result);
        }
        tasks.joined = true;
        tasks.result()
    }
}
impl Shared {
    fn request_stop(&self) {
        let mut registry = self.registry.lock().unwrap();
        registry.closed = true;
        self.stop.send_replace(true);
        self.routes.lock().unwrap().clear();
        if let Some(mux) = &self.utp {
            mux.lock().unwrap().stop_accepting();
        }
        for owner in registry.torrents.values() {
            owner.handle.request_stop();
        }
    }
}
impl Drop for Shared {
    fn drop(&mut self) {
        self.request_stop();
        if let Some(dht) = &self.dht {
            dht.request_stop();
        }
        self.utp_stop.send_replace(true);
        self.udp_stop.send_replace(true);
    }
}
async fn route(
    listener: TcpListener,
    routes: Routes,
    admission: Pending,
    mut stop: watch::Receiver<bool>,
    pending: Arc<AsyncMutex<JoinSet<()>>>,
) -> io::Result<()> {
    let limits = admission.limits;
    let mut pending = pending.lock().await;
    let mut accepts = listener::AcceptBackoff::new("session");
    let result = loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break Ok(()),
            completed = pending.join_next(), if !pending.is_empty() => {
                if let Err(error) = completed.expect("pending handshake") {
                    break Err(io::Error::other(error));
                }
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
    result.and(drain_handshakes(&mut pending).await)
}
async fn route_utp(
    mut incoming: mpsc::Receiver<(utp::Stream, HandshakePermit)>,
    routes: Routes,
    limits: Limits,
    mut stop: watch::Receiver<bool>,
    pending: Arc<AsyncMutex<JoinSet<()>>>,
) -> io::Result<()> {
    let mut pending = pending.lock().await;
    let result = loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break Ok(()),
            completed = pending.join_next(), if !pending.is_empty() => {
                if let Err(error) = completed.expect("pending uTP handshake") {
                    break Err(io::Error::other(error));
                }
            },
            accepted = incoming.recv() => {
                let Some((socket, permit)) = accepted else { break Ok(()); };
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
    };
    result.and(drain_handshakes(&mut pending).await)
}
async fn drain_handshakes(pending: &mut JoinSet<()>) -> io::Result<()> {
    pending.abort_all();
    let mut result = Ok(());
    while let Some(joined) = pending.join_next().await {
        if let Err(error) = joined
            && !error.is_cancelled()
        {
            result = result.and(Err(io::Error::other(error)));
        }
    }
    result
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
