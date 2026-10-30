//! Session-owned lazy initialization and node; both remain joinable after cancellation.
use super::{DhtOptions, Registry, Session, udp};
use crate::engine::dht::Node;
use crate::protocol::dht::{NodeId, krpc::NodeV4};
use std::{
    io,
    net::{SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Semaphore, mpsc, watch},
    task::JoinHandle,
};

/// Persist at most 300 IPv4 contacts, retaining only currently good nodes.
pub const MAX_SAVED_NODES: usize = 300;
#[derive(Clone, Debug)]
pub struct SavedState {
    pub id: NodeId,
    pub nodes: Vec<NodeV4>,
}
#[derive(Clone, Debug)]
pub(in crate::engine) struct Handle {
    pub(in crate::engine) allow_local_network: bool,
    pub(in crate::engine) commands: mpsc::Sender<crate::engine::dht::Command>,
    pub(in crate::engine) address: SocketAddrV4,
    pub(in crate::engine) id: NodeId,
    pub(in crate::engine) lookups: Arc<Semaphore>,
    pub(in crate::engine) lookup_queries: usize,
    pub(in crate::engine) max_peers: usize,
}
const DEFAULT_ROUTERS: [&str; 4] = [
    "dht.libtorrent.org:25401",
    "dht.transmissionbt.com:6881",
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
];
const MAX_ROUTERS: usize = 16;

pub fn default_routers() -> Vec<String> {
    DEFAULT_ROUTERS.iter().map(|s| (*s).to_owned()).collect()
}
/// Validate without DNS or network access.
pub fn validate_routers(routers: &[String]) -> io::Result<()> {
    if routers.len() > MAX_ROUTERS || routers.iter().any(|s| endpoint(s).is_err()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid DHT routers",
        ));
    }
    Ok(())
}
pub(in crate::engine) fn endpoint(value: &str) -> io::Result<(&str, u16)> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "DHT router requires host:port");
    let (host, port) = value.rsplit_once(':').ok_or_else(invalid)?;
    let port = port.parse::<u16>().map_err(|_| invalid())?;
    if host.is_empty()
        || host.len() > 253
        || port == 0
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| !crate::engine::dht::valid_address(SocketAddrV4::new(ip, port), false))
    {
        return Err(invalid());
    }
    Ok((host, port))
}
impl DhtOptions {
    pub(super) fn validate(&self, listen: SocketAddr) -> io::Result<()> {
        let SocketAddr::V4(address) = listen else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DHT requires an IPv4 session listener",
            ));
        };
        if !crate::engine::dht::valid_address(address, true) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DHT bind address",
            ));
        }
        validate_routers(&self.routers)?;
        if self.saved.as_ref().is_some_and(|s| {
            s.nodes.len() > MAX_SAVED_NODES
                || s.nodes
                    .iter()
                    .any(|n| !crate::engine::dht::valid_address(n.address, false))
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid saved DHT state",
            ));
        }
        Ok(())
    }
}
struct Initialization {
    closed: bool,
    options: Option<DhtOptions>,
    task: Option<JoinHandle<io::Result<Handle>>>,
    result: Option<Result<Handle, Arc<io::Error>>>,
}
impl Initialization {
    async fn join(&mut self) -> io::Result<Handle> {
        if let Some(task) = &mut self.task {
            let result = task.await.map_err(io::Error::other).and_then(|r| r);
            self.task = None;
            self.result = Some(result.map_err(Arc::new));
        }
        self.result
            .as_ref()
            .expect("DHT initialization started")
            .clone()
            .map_err(|e| io::Error::new(e.kind(), e))
    }
}
pub(in crate::engine) struct Dht {
    initialization: AsyncMutex<Initialization>,
    node: Arc<AsyncMutex<Option<OwnedDht>>>,
    udp: udp::Transport,
    allow_local_network: bool,
    registry: Weak<Mutex<Registry>>,
    stop: watch::Receiver<bool>,
    node_stop: watch::Sender<bool>,
}
impl Dht {
    pub(super) fn new(
        options: DhtOptions,
        udp: udp::Transport,
        allow_local_network: bool,
        registry: Weak<Mutex<Registry>>,
        stop: watch::Receiver<bool>,
    ) -> Self {
        Self {
            initialization: AsyncMutex::new(Initialization {
                closed: false,
                options: Some(options),
                task: None,
                result: None,
            }),
            node: Arc::new(AsyncMutex::new(None)),
            udp,
            allow_local_network,
            registry,
            stop,
            node_stop: watch::channel(false).0,
        }
    }
    pub(super) fn request_stop(&self) {
        self.node_stop.send_replace(true);
    }
    pub(in crate::engine) async fn get(&self) -> io::Result<Handle> {
        let mut initialization = self.initialization.lock().await;
        if initialization.closed {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if let Some(options) = initialization.options.take() {
            let udp = self.udp.clone();
            let allow_local_network = self.allow_local_network;
            let registry = self.registry.clone();
            let stop = self.stop.clone();
            let resources = self.node.clone();
            let node_stop = self.node_stop.clone();
            // The task owns only the node slot, not its own join state or Session.
            initialization.task = Some(tokio::spawn(async move {
                let mut resources = resources.lock().await;
                let registry = registry.upgrade().ok_or(io::ErrorKind::Interrupted)?;
                let (sender, incoming) = mpsc::channel(256);
                let (node, handle) = Node::with_state(
                    udp.socket.clone(),
                    incoming,
                    allow_local_network,
                    options.saved,
                    options.routers,
                )?;
                let mut registry = registry.lock().unwrap();
                if registry.is_closed() || *stop.borrow() {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "session stopped",
                    ));
                }
                udp.routes.lock().unwrap().dht = Some(sender);
                let task = tokio::spawn(node.run_with_state(node_stop.subscribe()));
                *resources = Some(OwnedDht {
                    handle: handle.clone(),
                    stop: node_stop,
                    task: Some(task),
                    saved: None,
                });
                Ok(handle)
            }));
        }
        initialization.join().await
    }
    pub(super) async fn snapshot(&self) -> io::Result<Option<SavedState>> {
        let mut initialization = self.initialization.lock().await;
        if initialization.options.is_some() {
            return Ok(None);
        }
        initialization.join().await?;
        drop(initialization);
        let node = self.node.lock().await;
        let node = node.as_ref().expect("initialized DHT node");
        if let Some(saved) = &node.saved {
            saved
                .clone()
                .map(Some)
                .map_err(|e| io::Error::new(e.kind(), e))
        } else {
            node.handle.snapshot().await.map(Some)
        }
    }
    pub(super) async fn shutdown(&self) -> io::Result<()> {
        let mut initialization = self.initialization.lock().await;
        initialization.closed = true;
        if initialization.options.is_some() {
            return Ok(());
        }
        let initialized = initialization.join().await.map(|_| ());
        drop(initialization);
        let mut node = self.node.lock().await;
        let joined = if let Some(node) = &mut *node {
            node.shutdown().await
        } else {
            Ok(())
        };
        self.udp.routes.lock().unwrap().dht = None;
        // Closing admission can win before initialization publishes a node.
        let initialized = initialized.or_else(|error| {
            if error.kind() == io::ErrorKind::Interrupted {
                Ok(())
            } else {
                Err(error)
            }
        });
        initialized.and(joined)
    }
}
struct OwnedDht {
    handle: Handle,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<io::Result<SavedState>>>,
    saved: Option<Result<SavedState, Arc<io::Error>>>,
}
impl Session {
    /// Does not initialize DHT. Retains the final snapshot after joined shutdown.
    pub async fn dht_state(&self) -> io::Result<Option<SavedState>> {
        match &self.shared.dht {
            Some(dht) => dht.snapshot().await,
            None => Ok(None),
        }
    }
}
impl OwnedDht {
    async fn shutdown(&mut self) -> io::Result<()> {
        self.stop.send_replace(true);
        if let Some(task) = &mut self.task {
            let result = task.await.map_err(io::Error::other).and_then(|r| r);
            self.task = None;
            self.saved = Some(result.map_err(Arc::new));
        }
        self.saved
            .as_ref()
            .expect("DHT joined")
            .as_ref()
            .map(|_| ())
            .map_err(|e| io::Error::new(e.kind(), e.clone()))
    }
}
impl Drop for OwnedDht {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
