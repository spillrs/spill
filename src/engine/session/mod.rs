//! Shared networking, resource budgets, and execution ownership.
//!
//! Bind a [`Session`], then [`Session::admit`] torrents. Admission reserves budgets
//! through joined cleanup; [`Session::shutdown`] stops and joins owned work.
//! Unlike [`super::download`], default reusable sessions do not enable DHT.
pub mod dht;
pub(in crate::engine) mod dial;
pub(in crate::engine) mod incoming;
mod listener;
pub(in crate::engine) mod lsd;
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
    identity::TorrentIdentity,
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

/// Peer encryption policy for TCP and uTP. MSE obfuscates traffic; it does not
/// authenticate peers or provide modern transport security.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerEncryption {
    Allowed,
    #[default]
    Preferred,
    Required,
}
impl PeerEncryption {
    pub(in crate::engine) fn methods(self) -> crate::protocol::peer::mse::Methods {
        if self == Self::Required {
            crate::protocol::peer::mse::Methods::Rc4
        } else {
            crate::protocol::peer::mse::Methods::Both
        }
    }
}

/// Network permissions and listeners. Defaults use an ephemeral IPv4 port and uTP,
/// with no LAN access, router mapping, DHT, or local service discovery.
pub struct NetworkOptions {
    /// Bind TCP and UDP to the same address and port; zero selects a free port.
    pub listen: SocketAddr,
    /// Accept uTP and try it before TCP when dialing.
    pub utp: bool,
    /// Prefer encrypted peers by default, accepting plaintext for compatibility.
    pub peer_encryption: PeerEncryption,
    /// Permit outbound private, loopback, and link-local destinations.
    pub allow_local_network: bool,
    /// Opt into BEP 14 multicast discovery on UDP port 6771. Requires LAN access;
    /// each torrent's `dht` setting also gates LSD. Socket failures are nonfatal,
    /// reported in [`NetworkStatus::lsd`]. Interfaces are checked every minute;
    /// sockets rebuild only after changes or I/O failures.
    pub lsd: bool,
    /// Opt into IPv4 NAT-PMP/UPnP mapping.
    pub port_mapping: Option<port_map::Gateway>,
    /// Opt into DHT on the listener's address families, with supplied routers and optional saved state.
    pub dht: Option<DhtOptions>,
}
impl Default for NetworkOptions {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 0)),
            utp: true,
            peer_encryption: PeerEncryption::default(),
            allow_local_network: false,
            lsd: false,
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
    Tcp(std::net::TcpStream, crate::engine::peer_io::Prepared),
    Utp(utp::Stream, crate::engine::peer_io::Prepared),
}
#[derive(Default)]
struct RouteTable {
    plain: HashMap<[u8; 20], mpsc::Sender<Inbound>>,
    encrypted: HashMap<[u8; 20], [u8; 20]>,
}
impl RouteTable {
    fn insert(&mut self, key: [u8; 20], sender: mpsc::Sender<Inbound>) {
        self.encrypted
            .insert(crate::protocol::peer::mse::torrent_key(key), key);
        self.plain.insert(key, sender);
    }
    fn remove(&mut self, key: &[u8; 20]) {
        self.encrypted
            .remove(&crate::protocol::peer::mse::torrent_key(*key));
        self.plain.remove(key);
    }
    fn get(&self, key: &[u8; 20]) -> Option<&mpsc::Sender<Inbound>> {
        self.plain.get(key)
    }
    fn clear(&mut self) {
        self.plain.clear();
        self.encrypted.clear();
    }
}
type Routes = Arc<Mutex<RouteTable>>;
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
    known: TorrentIdentity,
    listener: Option<mpsc::Sender<Inbound>>,
    handle: TorrentHandle,
    profile: Profile,
    identity: Arc<()>,
}
#[derive(Clone)]
pub(in crate::engine) struct Retirement {
    registry: std::sync::Weak<Mutex<Registry>>,
    routes: Routes,
    hash: TorrentIdentity,
    identity: Arc<()>,
}
impl Retirement {
    pub(in crate::engine) fn resolve(
        &self,
        meta: &crate::protocol::metainfo::OwnedMetainfo,
        serving: bool,
    ) -> io::Result<()> {
        let identity = meta.info_hash();
        let registry = self
            .registry
            .upgrade()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "session stopped"))?;
        let mut registry = registry.lock().unwrap();
        if registry.torrents.values().any(|owner| {
            !Arc::ptr_eq(&owner.identity, &self.identity)
                && owner
                    .known
                    .wire_keys()
                    .any(|known| identity.wire_keys().any(|key| key == known))
        }) {
            return Err(crate::engine::profile::AdmissionError::Duplicate.into());
        }
        let owner = registry
            .torrents
            .get_mut(&self.hash.wire_key())
            .expect("registered execution");
        owner.known = identity;
        if !serving {
            let mut routes = self.routes.lock().unwrap();
            for key in owner.known.wire_keys() {
                routes.remove(&key);
            }
            owner.listener = None;
        }
        if let Some(sender) = &owner.listener {
            let mut routes = self.routes.lock().unwrap();
            for key in owner.known.wire_keys() {
                routes.insert(key, sender.clone());
            }
        }
        Ok(())
    }
    pub(in crate::engine) fn retire(&self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut registry = registry.lock().unwrap();
            if registry
                .torrents
                .get(&self.hash.wire_key())
                .is_some_and(|owner| Arc::ptr_eq(&owner.identity, &self.identity))
            {
                let mut routes = self.routes.lock().unwrap();
                for key in registry.torrents[&self.hash.wire_key()].known.wire_keys() {
                    routes.remove(&key);
                }
                registry.torrents.remove(&self.hash.wire_key());
            }
        }
    }
}
/// Passive observations of successful inbound BitTorrent handshakes since session creation.
/// Public peers exclude loopback, private, link-local and shared-address IPv4 ranges;
/// IPv4-mapped IPv6 addresses are classified as IPv4. These are observations, not
/// a firewall test. No observations does not imply an unreachable port.
#[derive(Clone, Debug, serde::Serialize)]
pub struct InboundStatus {
    /// Successful inbound transports still alive, including local peers.
    pub active: usize,
    /// Public subset of `active`.
    pub active_public: usize,
    /// Successful inbound handshakes, including local peers, even after disconnect/removal.
    pub total: u64,
    /// Public subset of `total`.
    pub public_total: u64,
    /// Age of the last successful public handshake, or none since session creation.
    pub last_public_ago_secs: Option<u64>,
}

/// Optional multicast discovery readiness, not proof that the LAN is reachable.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct LsdStatus {
    pub enabled: bool,
    /// At least one multicast socket is bound and joined.
    pub active: bool,
    /// Announce batches successfully sent on at least one interface since session creation.
    pub announces: u64,
    /// Valid announcements received from other sessions, including unregistered swarms.
    pub received: u64,
    /// Last startup or I/O failure; cleared after a successful announcement batch.
    pub last_error: Option<String>,
}

/// Local transport readiness, shared budget usage and passive inbound observations.
/// Counts are sampled under separate locks, not one atomic reservation view. No probes are sent.
#[derive(Clone, Debug, serde::Serialize)]
pub struct NetworkStatus {
    /// Includes completed attempts whose result has not been consumed.
    pub pending_dials: usize,
    pub pending_dials_limit: usize,
    /// Shared contact entries awaiting an attempt, including delayed retries.
    pub queued_contacts: usize,
    pub pending_handshakes: usize,
    pub pending_handshakes_limit: usize,
    pub pending_handshakes_per_ip_limit: usize,
    /// Includes handshakes and closing connections, not just payload peers.
    pub utp_connections: usize,
    pub utp_connections_limit: usize,
    /// Local address while the TCP listener task is running; none offline or stopped.
    pub tcp_listen: Option<SocketAddr>,
    /// Local address while the UDP receiver task is running; none offline or stopped.
    pub udp_listen: Option<SocketAddr>,
    /// Configured network permissions, not proof of external reachability.
    pub utp_enabled: bool,
    /// DHT permission, including a lazily initialized node that has not started yet.
    pub dht_enabled: bool,
    pub lsd: LsdStatus,
    pub inbound_tcp: InboundStatus,
    pub inbound_utp: InboundStatus,
}

/// Clones share executions. Last-clone drop requests stop; shutdown joins cleanup.
#[derive(Clone)]
pub struct Session {
    shared: Arc<Shared>,
}
struct Registry {
    torrents: HashMap<[u8; 20], Owner>,
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
    lsd: Option<JoinHandle<()>>,
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
    incoming: Arc<Mutex<incoming::Incoming>>,
    utp_enabled: bool,
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
    lsd: Option<lsd::Handle>,
    lsd_status: watch::Receiver<LsdStatus>,
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
        // An ephemeral TCP port may already be taken for UDP; pick another.
        let mut attempts = if network.listen.port() == 0 { 16 } else { 1 };
        let (listener, address, mut udp) = loop {
            let listener = TcpListener::bind(network.listen).await?;
            let address = listener.local_addr()?;
            attempts -= 1;
            match udp::Transport::bind(address) {
                Ok(udp) => break (listener, address, udp),
                Err(error) if attempts > 0 && error.kind() == io::ErrorKind::AddrInUse => {}
                Err(error) => return Err(error),
            }
        };
        let client = crate::engine::discovery::http_client(network.allow_local_network)?;
        let mut session = Shared::new(
            address,
            limits,
            network.allow_local_network,
            network.peer_encryption,
            Some(listener),
        );
        if network.lsd && network.allow_local_network {
            let (status, receiver) = watch::channel(LsdStatus {
                enabled: true,
                ..LsdStatus::default()
            });
            session.lsd_status = receiver;
            match lsd::Service::new(address, udp.families, status.clone()).await {
                Ok(lsd) => {
                    session.lsd = Some(lsd.handle());
                    session.tasks.get_mut().lsd =
                        Some(tokio::spawn(lsd.run(session.stop.subscribe())));
                }
                Err(error) => {
                    tracing::warn!(%error, "lsd_disabled");
                    status.send_modify(|s| s.last_error = Some(error.to_string()));
                }
            }
        }
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
            network.peer_encryption,
            session.stop.subscribe(),
            session.tasks.get_mut().utp_handshakes.clone(),
        )));
        session.utp_enabled = network.utp;
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
                PeerEncryption::default(),
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
        peer_encryption: PeerEncryption,
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
                peer_encryption,
                stopping,
                tcp_handshakes.clone(),
            ))
        });
        Self {
            bandwidth: crate::engine::bandwidth::Budget::default(),
            rechecks: Arc::new(Semaphore::new(RECHECK_CONCURRENCY)),
            address,
            incoming: Arc::default(),
            utp_enabled: false,
            advertised_port: watch::channel(address.port()).0,
            mapping_status: watch::channel(port_map::Status::default()).1,
            limits,
            routes,
            dial_queue: dial::Queue::new(
                limits.pending_dials,
                allow_local_network,
                peer_encryption,
            ),
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
            lsd: None,
            lsd_status: watch::channel(LsdStatus::default()).1,
            allow_local_network,
            tasks: AsyncMutex::new(Tasks {
                lsd: None,
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
    /// Snapshot listener-task readiness and inbound handshakes shared by all clones.
    /// Counters survive torrent removal and disconnects, but reset with a new session.
    /// Active counts end with the transport, including cancellation or panic.
    pub async fn network_status(&self) -> NetworkStatus {
        let tasks = self.shared.tasks.lock().await;
        let (inbound_tcp, inbound_utp) = self.shared.incoming.lock().unwrap().snapshot();
        let (pending_dials, pending_dials_limit, queued_contacts) =
            self.shared.dial_queue.snapshot();
        let pending_handshakes = self.shared.pending.counts.lock().unwrap().values().sum();
        let (utp_connections, utp_connections_limit) = self
            .shared
            .utp
            .as_ref()
            .map_or((0, 0), |mux| mux.lock().unwrap().capacity_status());
        NetworkStatus {
            pending_dials,
            pending_dials_limit,
            queued_contacts,
            pending_handshakes,
            pending_handshakes_limit: self.shared.limits.pending_handshakes,
            pending_handshakes_per_ip_limit: self.shared.limits.pending_handshakes_per_ip,
            utp_connections,
            utp_connections_limit,
            tcp_listen: tasks
                .listener
                .as_ref()
                .filter(|task| !task.is_finished())
                .map(|_| self.shared.address),
            udp_listen: tasks
                .udp
                .as_ref()
                .filter(|task| !task.is_finished())
                .map(|_| self.shared.address),
            utp_enabled: self.shared.utp_enabled,
            dht_enabled: self.shared.dht.is_some(),
            lsd: self.shared.lsd_status.borrow().clone(),
            inbound_tcp,
            inbound_utp,
        }
    }
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
    /// Look up a full identity, never a truncated v2 routing key. A single hash
    /// matches any known hash of the owner; a hybrid requires both to match.
    pub fn torrent(&self, hash: TorrentIdentity) -> Option<TorrentHandle> {
        self.shared
            .registry
            .lock()
            .unwrap()
            .torrents
            .values()
            .find(|owner| {
                let known = owner.known;
                hash.v1().is_none_or(|v1| known.v1() == Some(v1))
                    && hash.v2().is_none_or(|v2| known.v2() == Some(v2))
            })
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
            || registry.torrents.values().any(|owner| {
                owner
                    .known
                    .wire_keys()
                    .any(|known| hash.wire_keys().any(|key| key == known))
            })
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
        let online = !recheck || magnet;
        let networking = if !online {
            Networking::Recheck
        } else {
            Networking::Online(Network {
                incoming: shared.incoming.clone(),
                advertised_port: shared.advertised_port.subscribe(),
                mapping: shared.mapping_status.clone(),
                sockets,
                dial_queue: shared.dial_queue.clone(),
                utp: shared.utp_enabled,
                families: shared
                    .tracker_udp
                    .as_ref()
                    .expect("online UDP socket")
                    .families,
                tracker_http: shared.tracker_http.clone().expect("online session"),
                tracker_udp: shared.tracker_udp.clone().expect("online session"),
                allow_local_network: shared.allow_local_network,
                dht: shared.dht.clone(),
                lsd: shared.lsd.clone(),
            })
        };
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
            shared.rechecks.clone(),
            retirement,
            inspection,
        );
        if online {
            let mut routes = shared.routes.lock().unwrap();
            for key in hash.wire_keys() {
                routes.insert(key, sender.clone());
            }
        }
        registry.torrents.insert(
            hash.wire_key(),
            Owner {
                known: hash,
                listener: online.then_some(sender),
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
        if let Some(task) = tasks.lsd.as_mut() {
            let result = task.await.map_err(io::Error::other);
            tasks.lsd = None;
            tasks.record(result);
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
    encryption: PeerEncryption,
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
                let Some(permit) = admission.acquire(source.ip()) else {
                    tracing::trace!(transport="tcp",reason="handshake_limit","inbound_rejected");
                    continue;
                };
                let routes = routes.clone();
                pending.spawn(async move {
                    let _permit = permit;
                    let deadline = tokio::time::Instant::now() + limits.handshake_timeout;
                    let prepare = async {
                        let (prefix, mut socket) = peek::<20>(socket).await?;
                        let (hash, negotiated) = if prefix == *b"\x13BitTorrent protocol" {
                            if encryption == PeerEncryption::Required {
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "plaintext peer refused"));
                            }
                            let (bytes, stream) = peek::<HANDSHAKE_LEN>(socket).await?;
                            socket = stream;
                            let (hello, _) = Handshake::decode(&bytes).map_err(io::Error::other)?;
                            (hello.info_hash, None)
                        } else {
                            let codec = crate::engine::peer_io::encryption::responder(encryption.methods())?;
                            let negotiated = crate::engine::peer_io::encryption::exchange(&mut socket, codec, Vec::new(), |key| routes.lock().unwrap().encrypted.get(&key).copied()).await?;
                            (negotiated.wire_key, Some(Box::new(negotiated)))
                        };
                        Ok::<_, io::Error>((hash, socket, negotiated))
                    };
                    match tokio::time::timeout_at(deadline, prepare).await {
                        Ok(Ok((hash, socket, negotiated))) => {
                            let sender = routes.lock().unwrap().get(&hash).cloned();
                            if let Some(sender) = sender {
                                match socket.into_std() {
                                    Ok(socket) => {
                                        if let Err(error) = sender.try_send(Inbound::Tcp(socket, crate::engine::peer_io::Prepared { negotiated, deadline })) {
                                            tracing::trace!(transport="tcp",reason=if matches!(error,mpsc::error::TrySendError::Full(_)){"torrent_inbox_full"}else{"torrent_stopped"},"inbound_rejected");
                                        }
                                    }
                                    Err(error) => tracing::trace!(transport="tcp",reason="socket_registration",kind=?error.kind(),%error,"inbound_rejected"),
                                }
                            } else {
                                tracing::trace!(transport="tcp",reason="unknown_torrent","inbound_rejected");
                            }
                        }
                        Ok(Err(error)) => tracing::trace!(transport="tcp",reason="handshake_failed",kind=?error.kind(),%error,"inbound_rejected"),
                        Err(_) => tracing::trace!(transport="tcp",reason="handshake_timeout","inbound_rejected"),
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
    encryption: PeerEncryption,
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
                let Some((mut socket, permit)) = accepted else { break Ok(()); };
                let routes = routes.clone();
                pending.spawn(async move {
                    let _permit = permit;
                    let deadline = tokio::time::Instant::now() + limits.handshake_timeout;
                    let prepare = async {
                        let mut prefix = [0; 20];
                        if socket.peek(&mut prefix).await? != prefix.len() {
                            return Err(io::ErrorKind::UnexpectedEof.into());
                        }
                        if prefix == *b"\x13BitTorrent protocol" {
                            if encryption == PeerEncryption::Required {
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "plaintext peer refused"));
                            }
                            let mut bytes = [0; HANDSHAKE_LEN];
                            if socket.peek(&mut bytes).await? != bytes.len() {
                                return Err(io::ErrorKind::UnexpectedEof.into());
                            }
                            let (hello, _) = Handshake::decode(&bytes).map_err(io::Error::other)?;
                            Ok((hello.info_hash, None))
                        } else {
                            let codec = crate::engine::peer_io::encryption::responder(encryption.methods())?;
                            let negotiated = crate::engine::peer_io::encryption::exchange(&mut socket, codec, Vec::new(), |key| routes.lock().unwrap().encrypted.get(&key).copied()).await?;
                            Ok::<_, io::Error>((negotiated.wire_key, Some(Box::new(negotiated))))
                        }
                    };
                    match tokio::time::timeout_at(deadline, prepare).await {
                        Ok(Ok((hash, negotiated))) => {
                            let sender = routes.lock().unwrap().get(&hash).cloned();
                            if let Some(sender) = sender {
                                match sender.try_send(Inbound::Utp(socket, crate::engine::peer_io::Prepared { negotiated, deadline })) {
                                    Ok(()) => return,
                                    Err(error) => {
                                        tracing::trace!(transport="utp",reason=if matches!(error,mpsc::error::TrySendError::Full(_)){"torrent_inbox_full"}else{"torrent_stopped"},"inbound_rejected");
                                        if let Inbound::Utp(socket, _) = error.into_inner() { socket.reset(); }
                                        return;
                                    }
                                }
                            } else {
                                tracing::trace!(transport="utp",reason="unknown_torrent","inbound_rejected");
                            }
                        }
                        Ok(Err(error)) => tracing::trace!(transport="utp",reason="handshake_failed",kind=?error.kind(),%error,"inbound_rejected"),
                        Err(_) => tracing::trace!(transport="utp",reason="handshake_timeout","inbound_rejected"),
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
async fn peek<const N: usize>(socket: TcpStream) -> io::Result<([u8; N], TcpStream)> {
    // Own the readiness guard while routing: TcpStream::peek retains readable
    // readiness for an unchanged prefix and would spin on fragmented input.
    // Tokio's replacement registration API requires unsafe. The deprecated
    // safe constructor is sound for this owned std TcpStream: its descriptor
    // cannot be replaced or closed while registered, and we never use get_mut.
    #[allow(deprecated)]
    let socket = AsyncFd::new(socket.into_std()?)?;
    let mut bytes = [0; N];
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
            return Ok((bytes, TcpStream::from_std(socket.into_inner())?));
        }
        if length == 0 || closed {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
}
