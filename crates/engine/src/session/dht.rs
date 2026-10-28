//! Session owns the opt-in UDP task; stop it after torrent discovery has joined.
use super::{DhtOptions, Session};
use crate::dht::Node;
use protocol::dht::{NodeId, krpc::NodeV4};
use std::{
    io,
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::{
    sync::{Semaphore, mpsc, watch},
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
pub struct Handle {
    pub(crate) allow_local_network: bool,
    pub(crate) commands: mpsc::Sender<crate::dht::Command>,
    pub(crate) address: SocketAddrV4,
    pub(crate) id: NodeId,
    pub(crate) lookups: Arc<Semaphore>,
    pub(crate) lookup_queries: usize,
    pub(crate) max_peers: usize,
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
pub(crate) fn endpoint(value: &str) -> io::Result<(&str, u16)> {
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
            .is_ok_and(|ip| !crate::dht::valid_address(SocketAddrV4::new(ip, port), false))
    {
        return Err(invalid());
    }
    Ok((host, port))
}
pub(super) struct OwnedDht {
    pub(super) handle: Handle,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<io::Result<SavedState>>>,
    saved: Option<SavedState>,
}
impl Session {
    /// Start at most one DHT node on the TCP listener's port. This does not grant
    /// torrents DHT authority: pass the returned handle with TorrentOptions::dht.
    pub async fn enable_dht(&mut self, options: DhtOptions) -> io::Result<Handle> {
        if *self.stop.borrow() || self.listener.is_none() || self.dht.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session stopped or DHT already enabled",
            ));
        }
        let SocketAddr::V4(_) = self.address else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DHT requires an IPv4 session listener",
            ));
        };
        let udp = self.tracker_udp.as_ref().expect("online session");
        let (sender, incoming) = tokio::sync::mpsc::channel(256);
        let (node, handle) = Node::with_state(
            udp.socket.clone(),
            incoming,
            self.limits.allow_local_network,
            options.allow_network,
            options.saved,
            options.routers,
        )
        .await?;
        udp.routes.lock().unwrap().dht = Some(sender);
        let (stop, stopping) = watch::channel(false);
        let task = tokio::spawn(node.run_with_state(stopping));
        self.dht = Some(OwnedDht {
            handle: handle.clone(),
            stop,
            task: Some(task),
            saved: None,
        });
        Ok(handle)
    }
    /// Snapshot for catalog persistence, including the final state after shutdown.
    pub async fn dht_state(&self) -> io::Result<Option<SavedState>> {
        match &self.dht {
            Some(dht) => match &dht.saved {
                Some(saved) => Ok(Some(saved.clone())),
                None => dht.handle.snapshot().await.map(Some),
            },
            None => Ok(None),
        }
    }
}
impl OwnedDht {
    pub(super) async fn shutdown(&mut self) -> io::Result<()> {
        self.stop.send_replace(true);
        if let Some(task) = self.task.as_mut() {
            let result = task
                .await
                .map_err(io::Error::other)
                .and_then(|result| result);
            self.task = None;
            self.saved = Some(result?);
        }
        Ok(())
    }
}
impl Drop for OwnedDht {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
