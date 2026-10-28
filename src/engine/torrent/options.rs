//! Caller-facing builder, consumed into fully initialized private runtime inputs.
use super::{Networking, RunPolicy, RuntimeOptions, TorrentHandle, TorrentOptions};
use crate::engine::{profile::Profile, provisioning::FileStorage, storage::writer::Writer};
use crate::protocol::metainfo::OwnedMetainfo;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

impl TorrentOptions {
    pub fn new(metainfo: OwnedMetainfo, profile: Profile) -> Self {
        Self {
            settings: Default::default(),
            profile,
            metainfo,
            storage: None,
            resume: None,
            checkpoint: Box::new(|_| Ok(())),
            completed_announce: None,
            announce_checkpoint: Arc::new(|_| Ok(())),
            peers: Vec::new(),
            dht: None,
            run: RunPolicy::Persistent,
            log_peer_endpoints: false,
            no_progress_timeout: Duration::from_secs(60),
            diagnostics: false,
        }
    }
    pub fn settings(mut self, settings: crate::engine::settings::TorrentSettings) -> Self {
        self.settings = settings;
        self
    }
    pub fn storage(mut self, storage: FileStorage) -> Self {
        self.storage = Some(storage);
        self
    }
    /// Initial opaque checkpoint, validated against metainfo and file fingerprints.
    /// Explicit rechecks ignore it.
    pub fn resume(mut self, resume: Option<Vec<u8>>) -> Self {
        self.resume = resume;
        self
    }
    /// Publish checkpoints before reporting a flush or joined stop. An error
    /// from this writer-task callback stops the torrent.
    pub fn on_checkpoint(
        mut self,
        checkpoint: impl Fn(Vec<u8>) -> io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.checkpoint = Box::new(checkpoint);
        self
    }
    /// Restore completion delivery: None means no completion event yet, false
    /// means pending, true means acknowledged. Publish before advancing delivery.
    pub fn completion_announce(
        mut self,
        saved: Option<bool>,
        checkpoint: impl Fn(bool) -> io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.completed_announce = saved;
        self.announce_checkpoint = Arc::new(checkpoint);
        self
    }
    /// Explicit direct contacts, subject to the single cap in Profile::peers.
    pub fn peers(mut self, peers: Vec<SocketAddr>) -> Self {
        self.peers = peers;
        self
    }
    pub fn dht(mut self, dht: Option<crate::engine::session::dht::Handle>) -> Self {
        self.dht = dht;
        self
    }
    /// Detailed snapshots are opt-in; progress/lifecycle is always published.
    pub fn diagnostics(mut self, enabled: bool) -> Self {
        self.diagnostics = enabled;
        self
    }
    pub fn run(mut self, run: RunPolicy) -> Self {
        self.run = run;
        self
    }
    pub(in crate::engine) fn spawn_session(
        mut self,
        bandwidth: crate::engine::bandwidth::Throttle,
        networking: Networking,
        rechecks: Arc<Semaphore>,
    ) -> io::Result<TorrentHandle> {
        if self.metainfo.is_private() {
            self.settings.dht = false;
            self.settings.pex = false;
        }
        let selection = if self.settings.file_priorities.is_empty() {
            crate::engine::selection::FileSelection::all(self.metainfo.layout())
        } else {
            crate::engine::selection::FileSelection::new(
                self.metainfo.layout(),
                &self.settings.file_priorities,
            )?
        };
        let peer_id = crate::engine::identity::generate_peer_id()?;
        let storage = self
            .storage
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "storage is required"))?;
        let writer = Writer::new_with_recheck(
            storage,
            self.metainfo.clone(),
            self.profile,
            matches!(self.run, RunPolicy::Recheck),
            self.resume,
            self.checkpoint,
            rechecks,
        )?;
        let (reannounce, announce_requests) = tokio::sync::watch::channel(());
        let options = RuntimeOptions {
            reannounce: announce_requests,
            completed_announce: self.completed_announce,
            announce_checkpoint: self.announce_checkpoint,
            dht: self.dht.filter(|_| self.settings.dht),
            settings: self.settings,
            metainfo: self.metainfo,
            profile: self.profile,
            peers: self.peers,
            run: self.run,
            log_peer_endpoints: self.log_peer_endpoints,
            no_progress_timeout: self.no_progress_timeout,
            diagnostics: self.diagnostics,
            selection,
            peer_id,
            bandwidth,
        };
        Ok(TorrentHandle::spawn_storage(
            reannounce, writer, options, networking,
        ))
    }
}
