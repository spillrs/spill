//! One task per torrent owns discovery, peers and the picker. Storage runs in
//! one separate bounded writer task. Network consent is explicit; stop then join
//! before releasing storage or starting a replacement torrent owner.

mod observation;
mod options;
mod runtime;
pub(in crate::engine) mod shell;
pub mod telemetry;

use crate::engine::{
    profile::Profile,
    provisioning::FileStorage,
    storage::{Checkpoint, writer::Writer},
};
use crate::protocol::metainfo::OwnedMetainfo;
use serde::{Serialize, Serializer};
use std::time::Instant;
use std::{future::Future, io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};
use tracing::Instrument;

/// Fluent caller-supplied admission options, consumed before runtime starts.
pub struct TorrentOptions {
    pub(in crate::engine) settings: crate::engine::settings::TorrentSettings,
    pub(in crate::engine) metainfo: OwnedMetainfo,
    pub(in crate::engine) profile: Profile,
    storage: Option<FileStorage>,
    resume: Option<Vec<u8>>,
    checkpoint: Checkpoint,
    completed_announce: Option<bool>,
    announce_checkpoint: Arc<dyn Fn(bool) -> io::Result<()> + Send + Sync>,
    pub(in crate::engine) peers: Vec<SocketAddr>,
    pub(in crate::engine) dht: Option<crate::engine::session::dht::Handle>,
    pub(in crate::engine) run: RunPolicy,
    pub(in crate::engine) log_peer_endpoints: bool,
    pub(in crate::engine) no_progress_timeout: Duration,
    pub(in crate::engine) diagnostics: bool,
}

/// Session authority is either fully online or absent for an offline recheck.
pub(in crate::engine) enum Networking {
    Online(Network),
    Recheck,
}
pub(in crate::engine) struct Network {
    pub(in crate::engine) advertised_port: watch::Receiver<u16>,
    pub(in crate::engine) sockets: tokio::sync::mpsc::Receiver<crate::engine::session::Inbound>,
    pub(in crate::engine) dial_queue: crate::engine::session::dial::Queue,
    pub(in crate::engine) tracker_http: reqwest::Client,
    pub(in crate::engine) tracker_udp: crate::engine::session::udp::Transport,
    pub(in crate::engine) allow_local_network: bool,
}

/// Fully constructed runtime inputs; no builder placeholders or consumed storage.
pub(in crate::engine) struct RuntimeOptions {
    pub(in crate::engine) reannounce: watch::Receiver<()>,
    pub(in crate::engine) completed_announce: Option<bool>,
    pub(in crate::engine) announce_checkpoint: Arc<dyn Fn(bool) -> io::Result<()> + Send + Sync>,
    pub(in crate::engine) settings: crate::engine::settings::TorrentSettings,
    pub(in crate::engine) metainfo: OwnedMetainfo,
    pub(in crate::engine) profile: Profile,
    pub(in crate::engine) peers: Vec<SocketAddr>,
    pub(in crate::engine) dht: Option<crate::engine::session::dht::Handle>,
    run: RunPolicy,
    log_peer_endpoints: bool,
    no_progress_timeout: Duration,
    diagnostics: bool,
    selection: crate::engine::selection::FileSelection,
    pub(in crate::engine) peer_id: [u8; 20],
    bandwidth: crate::engine::bandwidth::Throttle,
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
pub(in crate::engine) struct TorrentHandle {
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
    pub(in crate::engine) fn observer(&self) -> TorrentObserver {
        TorrentObserver {
            reannounce: self.reannounce.clone(),
            stop: self.stop.clone(),
            status: self.status.clone(),
        }
    }
    pub(in crate::engine) fn spawn_storage(
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
    pub(in crate::engine) fn request_stop(&self) {
        self.stop.send_replace(true);
    }

    /// Wait for task completion without requesting stop. Canceling this wait
    /// retains the join handle. May be called once successfully; errors retain
    /// their sources, and task panic is not a joined-cleanup result.
    pub(in crate::engine) async fn join(&mut self) -> io::Result<()> {
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
