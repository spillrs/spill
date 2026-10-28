//! One task per torrent owns discovery, peers and the picker. Storage runs in
//! one separate bounded writer task. Network consent is explicit; stop then join
//! before releasing storage or starting a replacement torrent owner.

mod observation;
mod options;
mod runtime;
pub(crate) mod shell;
pub mod telemetry;

use crate::{
    profile::Profile,
    provisioning::FileStorage,
    storage::{Checkpoint, writer::Writer},
};
use protocol::metainfo::OwnedMetainfo;
use serde::{Serialize, Serializer};
use std::time::Instant;
use std::{future::Future, io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};
use tracing::Instrument;

/// Fluent caller-supplied admission options, consumed before runtime starts.
pub struct TorrentOptions {
    pub(crate) settings: crate::settings::TorrentSettings,
    pub(crate) metainfo: OwnedMetainfo,
    pub(crate) profile: Profile,
    storage: Option<FileStorage>,
    resume: Option<Vec<u8>>,
    checkpoint: Checkpoint,
    completed_announce: Option<bool>,
    announce_checkpoint: Arc<dyn Fn(bool) -> io::Result<()> + Send + Sync>,
    pub(crate) peers: Vec<SocketAddr>,
    pub(crate) dht: Option<crate::session::dht::Handle>,
    pub(crate) run: RunPolicy,
    pub(crate) log_peer_endpoints: bool,
    pub(crate) no_progress_timeout: Duration,
    pub(crate) diagnostics: bool,
}

/// Session authority is either fully online or absent for an offline recheck.
pub(crate) enum Networking {
    Online(Network),
    Recheck,
}
pub(crate) struct Network {
    pub(crate) advertised_port: watch::Receiver<u16>,
    pub(crate) sockets: tokio::sync::mpsc::Receiver<crate::session::Inbound>,
    pub(crate) dial_queue: crate::session::dial::Queue,
    pub(crate) tracker_http: reqwest::Client,
    pub(crate) tracker_udp: crate::session::udp::Transport,
    pub(crate) allow_local_network: bool,
}

/// Fully constructed runtime inputs; no builder placeholders or consumed storage.
pub(crate) struct RuntimeOptions {
    pub(crate) reannounce: watch::Receiver<()>,
    pub(crate) completed_announce: Option<bool>,
    pub(crate) announce_checkpoint: Arc<dyn Fn(bool) -> io::Result<()> + Send + Sync>,
    pub(crate) settings: crate::settings::TorrentSettings,
    pub(crate) metainfo: OwnedMetainfo,
    pub(crate) profile: Profile,
    pub(crate) peers: Vec<SocketAddr>,
    pub(crate) dht: Option<crate::session::dht::Handle>,
    run: RunPolicy,
    log_peer_endpoints: bool,
    no_progress_timeout: Duration,
    diagnostics: bool,
    selection: crate::selection::FileSelection,
    pub(crate) peer_id: [u8; 20],
    bandwidth: crate::bandwidth::Throttle,
}
/// Lifetime/completion policy, separate from backend durability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunPolicy {
    /// Explicit full verification and resume save, with no peer/discovery I/O.
    Recheck,
    /// Background service: keep serving after completion until stopped or failed.
    /// Incomplete downloads retain the configured no-progress recovery timeout.
    Persistent,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentPhase {
    #[default]
    Starting,
    /// Pre-network resume validation and changed-piece recheck.
    Recovering,
    Downloading,
    Seeding,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentExit {
    StopRequested,
    NoProgress,
    Complete,
    Unavailable,
}

/// Summary of piece-local retries. Backoff never prevents unrelated scheduling;
/// run and no-progress deadlines remain in force.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TorrentRetryStatus {
    #[default]
    Idle,
    Backoff {
        attempt: u32,
        /// Serialized as a nonnegative remaining delay, not a process-local clock.
        #[serde(rename = "retry_in_ms", serialize_with = "serialize_retry_delay")]
        retry_at: Instant,
    },
    Retrying {
        attempt: u32,
    },
}

fn serialize_retry_delay<S: Serializer>(
    deadline: &Instant,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let millis = deadline
        .saturating_duration_since(Instant::now())
        .as_millis();
    serializer.serialize_u64(millis.min(u128::from(u64::MAX)) as u64)
}

fn serialize_piece_bitfield<S: Serializer>(
    pieces: &[bool],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut bytes = vec![0u8; pieces.len().div_ceil(8)];
    for (index, &verified) in pieces.iter().enumerate() {
        if verified {
            bytes[index / 8] |= 0x80 >> (index % 8);
        }
    }
    bytes.serialize(serializer)
}

fn piece_map_empty(pieces: &std::sync::Arc<[bool]>) -> bool {
    pieces.is_empty()
}

/// Latest observations, not an event backlog or a durable resume record.
/// `written_payload_bytes` means socket-written bytes, not remote receipt.
/// Serialization uses snake-case phases/exits and relative retry delays in milliseconds.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TorrentStatus {
    pub progress: TorrentProgress,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<Arc<TorrentDiagnostics>>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TorrentDiagnostics {
    /// Observed uTP retransmits for this owner, retained at peer I/O close.
    pub utp_retransmits: u64,
    pub pending_contacts: usize,
    /// Free writer command slots, not bytes; zero when no writer is attached.
    pub writer_free_slots: usize,
    /// Core upload reads awaiting completion, including queued reads.
    pub upload_reads_in_flight: usize,
    /// Completed core upload reads retained until consumption or cancellation.
    pub upload_reads_finished: usize,
    pub peers: Vec<telemetry::PeerStatus>,
    pub trackers: Vec<telemetry::TrackerStatus>,
    pub web_seeds: Vec<telemetry::WebSeedStatus>,
    pub dht: Option<telemetry::DhtStatus>,
    pub retry: TorrentRetryStatus,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TorrentProgress {
    pub phase: TorrentPhase,
    pub verified_bytes: u64,
    pub total_bytes: u64,
    pub wanted_bytes: u64,
    pub wanted_verified_bytes: u64,
    pub processed_payload_bytes: u64,
    pub written_payload_bytes: u64,
    pub download_bytes_per_second: f64,
    pub upload_bytes_per_second: f64,
    /// True after writable payload files are data-synced and the completion
    /// resume checkpoint is saved. Read-only adopted seeds skip the data sync.
    pub completed: bool,
    /// Listener/discovery setup has finished; distinct from payload completion.
    pub ready: bool,
    pub verified_pieces: u32,
    /// Verified piece membership, indexed by metainfo piece number. Serialized
    /// only when present, as an MSB-first packed byte array (verified_piece_bitfield).
    /// Shared immutable observation, refreshed on reporting ticks, completion
    /// and shutdown; counters remain current per piece.
    #[serde(
        rename = "verified_piece_bitfield",
        serialize_with = "serialize_piece_bitfield",
        skip_serializing_if = "piece_map_empty"
    )]
    pub verified_piece_map: std::sync::Arc<[bool]>,
    pub active_peers: usize,
    pub exit: Option<TorrentExit>,
    /// All tracked peer, discovery and storage work has successfully joined.
    pub joined: bool,
}
/// Owns one running task. Status is a single latest-value slot; slow or absent
/// observers never block transfer or lose the final observation. Requires Tokio.
/// Session owns the handle; callers use TorrentOptions and TorrentObserver.
pub(crate) struct TorrentHandle {
    reannounce: watch::Sender<()>,
    stop: watch::Sender<bool>,
    status: watch::Receiver<TorrentStatus>,
    task: Option<JoinHandle<io::Result<()>>>,
}
/// Cloneable control/observation without ownership of the task's join barrier.
#[derive(Clone)]
pub struct TorrentObserver {
    reannounce: watch::Sender<()>,
    stop: watch::Sender<bool>,
    status: watch::Receiver<TorrentStatus>,
}
impl TorrentObserver {
    /// Request an announce at the next tracker-permitted time.
    pub fn reannounce(&self) {
        self.reannounce.send_replace(());
    }
    pub fn request_stop(&self) {
        self.stop.send_replace(true);
    }
    pub fn snapshot(&self) -> TorrentStatus {
        self.status.borrow().clone()
    }
    pub async fn changed(&mut self) -> bool {
        self.status.changed().await.is_ok()
    }
}

impl TorrentHandle {
    pub(crate) fn observer(&self) -> TorrentObserver {
        TorrentObserver {
            reannounce: self.reannounce.clone(),
            stop: self.stop.clone(),
            status: self.status.clone(),
        }
    }
    pub(crate) fn spawn_storage(
        reannounce: watch::Sender<()>,
        writer: Writer,
        options: RuntimeOptions,
        networking: Networking,
    ) -> Self {
        let initial = TorrentStatus {
            progress: TorrentProgress {
                total_bytes: options.metainfo.layout().total_length(),
                ..Default::default()
            },
            diagnostics: options
                .diagnostics
                .then(|| Arc::new(TorrentDiagnostics::default())),
        };
        Self::spawn_task(initial, reannounce, move |stop, status| {
            runtime::run(writer, options, networking, stop, status)
        })
    }

    fn spawn_task<F: Future<Output = io::Result<()>> + Send + 'static>(
        initial: TorrentStatus,
        reannounce: watch::Sender<()>,
        run: impl FnOnce(watch::Receiver<bool>, watch::Sender<TorrentStatus>) -> F,
    ) -> Self {
        let (stop, stopping) = watch::channel(false);
        let (status, observer) = watch::channel(initial);
        let task = tokio::spawn(run(stopping, status).instrument(tracing::Span::current()));
        Self {
            reannounce,
            stop,
            status: observer,
            task: Some(task),
        }
    }

    /// Idempotent, nonblocking stop intent. Completion requires [`Self::join`].
    pub(crate) fn request_stop(&self) {
        self.stop.send_replace(true);
    }

    /// Wait for task completion without requesting stop. Canceling this wait
    /// retains the join handle. May be called once successfully; errors retain
    /// their sources, and task panic is not a joined-cleanup result.
    pub(crate) async fn join(&mut self) -> io::Result<()> {
        let task = self
            .task
            .as_mut()
            .ok_or_else(|| io::Error::other("torrent task already joined"))?;
        let result = task.await.map_err(io::Error::other);
        self.task = None;
        result?
    }
}
impl Drop for TorrentHandle {
    fn drop(&mut self) {
        self.request_stop();
        // Dropping the join handle detaches orderly cleanup, not an abort of
        // the backend owner. Explicit join remains required for stopped proof.
    }
}
