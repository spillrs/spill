//! One task per torrent owns discovery, peers and the picker. Storage runs in
//! one separate bounded writer task. Network consent is explicit; stop then join
//! before releasing storage or starting a replacement torrent owner.

mod lifecycle;
mod observation;
mod options;
mod runtime;
pub(in crate::engine) mod shell;
pub mod telemetry;

use crate::engine::{profile::Profile, provisioning::DestinationBinding};
use crate::protocol::{identity::InfoHashV1, magnet::Magnet, metainfo::OwnedMetainfo};
use std::{
    io,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tracing::Instrument;

pub enum TorrentSource {
    Metainfo(OwnedMetainfo),
    Magnet(Magnet),
}

pub enum Destination {
    Open(PathBuf),
    Create(PathBuf),
    Adopt(PathBuf),
    Recover { root: PathBuf, state: RecoveryState },
}

#[derive(Clone)]
pub struct RecoveryState {
    pub binding: DestinationBinding,
    pub resume: Option<Vec<u8>>,
    pub completed_announce: Option<bool>,
}

/// Acknowledge an encoded resume checkpoint. Failure fails the execution.
pub type Checkpoint = Box<dyn Fn(Vec<u8>) -> io::Result<()> + Send + Sync>;

/// Acknowledgements run on blocking threads and remain owned through stop.
/// Callbacks must not synchronously wait on this execution or session shutdown.
#[derive(Default)]
pub struct Persistence {
    pub resolved: Option<Box<dyn Fn(OwnedMetainfo) -> io::Result<()> + Send + Sync>>,
    pub binding: Option<Box<dyn Fn(DestinationBinding) -> io::Result<()> + Send + Sync>>,
    pub checkpoint: Option<Checkpoint>,
    pub completion_announce: Option<Box<dyn Fn(bool) -> io::Result<()> + Send + Sync>>,
}

pub struct TorrentOptions {
    pub source: TorrentSource,
    pub destination: Destination,
    pub profile: Profile,
    pub settings: crate::engine::settings::TorrentSettings,
    pub run: RunPolicy,
    pub peers: Vec<SocketAddr>,
    pub diagnostics: bool,
    pub log_peer_endpoints: bool,
    pub no_progress_timeout: Duration,
    pub persistence: Persistence,
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
    pub(in crate::engine) dht: Option<Arc<crate::engine::session::dht::Dht>>,
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
    /// Stop after wanted completion, allowing five seconds for pending completed
    /// announces. This grace does not bound blocking callbacks or cleanup.
    UntilComplete,
    /// Explicit full verification and resume save, with no peer/discovery I/O.
    Recheck,
    /// Background service: keep serving after completion until stopped or failed.
    /// Incomplete downloads retain the configured no-progress recovery timeout.
    Persistent,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TorrentPhase {
    #[default]
    Starting,
    ResolvingMetadata,
    PreparingDestination,
    /// Pre-network resume validation and changed-piece recheck.
    Recovering,
    Downloading,
    Seeding,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TorrentExit {
    StopRequested,
    Complete,
    NoProgress,
    Rechecked,
    Unavailable,
    Failed,
    MetadataResolved,
}

#[derive(Debug)]
pub struct TorrentOutcome {
    pub termination: TorrentExit,
    pub wanted_complete: bool,
    pub full_complete: bool,
    pub recheck_finished: bool,
    pub cleanup_complete: bool,
    pub error: Option<Arc<io::Error>>,
}
impl TorrentOutcome {
    fn incomplete(&self, message: &'static str) -> io::Error {
        self.result().err().unwrap_or_else(|| {
            io::Error::new(
                match self.termination {
                    TorrentExit::StopRequested => io::ErrorKind::Interrupted,
                    TorrentExit::NoProgress => io::ErrorKind::TimedOut,
                    _ => io::ErrorKind::UnexpectedEof,
                },
                message,
            )
        })
    }
    fn result(&self) -> io::Result<()> {
        self.error.as_ref().map_or(Ok(()), |error| {
            Err(io::Error::new(error.kind(), error.clone()))
        })
    }
}

/// Summary of piece-local retries. Backoff never prevents unrelated scheduling;
/// run and no-progress deadlines remain in force.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TorrentRetryStatus {
    #[default]
    Idle,
    Backoff {
        attempt: u32,
        retry_at: Instant,
    },
    Retrying {
        attempt: u32,
    },
}

/// Latest observations, not an event backlog or a durable resume record.
/// `written_payload_bytes` means socket-written bytes, not remote receipt.
#[derive(Clone, Debug, Default)]
pub struct TorrentStatus {
    pub progress: TorrentProgress,
    pub diagnostics: Option<Arc<TorrentDiagnostics>>,
}

#[derive(Clone, Debug, Default)]
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

#[derive(Clone, Debug, Default)]
pub struct TorrentProgress {
    pub phase: TorrentPhase,
    pub verified_bytes: u64,
    pub total_bytes: u64,
    pub wanted_bytes: u64,
    pub wanted_verified_bytes: u64,
    pub processed_payload_bytes: u64,
    /// Download verification failures only; startup/recheck failures are excluded.
    pub hash_failed_bytes: u64,
    pub hash_failed_pieces: u64,
    pub written_payload_bytes: u64,
    pub download_bytes_per_second: f64,
    pub upload_bytes_per_second: f64,
    /// True after writable payload files are data-synced and the completion
    /// resume checkpoint is saved. Read-only adopted seeds skip the data sync.
    pub completed: bool,
    /// Listener/discovery setup has finished; distinct from payload completion.
    pub ready: bool,
    /// Recovery has established authoritative piece availability, before discovery.
    pub recovered: bool,
    pub verified_pieces: u32,
    /// Verified piece membership, indexed by metainfo piece number.
    /// Shared immutable observation, refreshed on reporting ticks, completion
    /// and shutdown; counters remain current per piece.
    pub verified_piece_map: Arc<[bool]>,
    pub active_peers: usize,
    pub exit: Option<TorrentExit>,
    /// All owned work joined, independently of content or operational success.
    pub joined: bool,
    pub full_complete: bool,
    pub seeding_duration: Duration,
}
/// Dropping a handle does not stop its session-owned execution.
#[derive(Clone)]
pub struct TorrentHandle {
    reannounce: watch::Sender<()>,
    stop: watch::Sender<bool>,
    status: watch::Receiver<TorrentStatus>,
    join: Arc<JoinBarrier>,
    hash: InfoHashV1,
    metadata: watch::Receiver<lifecycle::Metadata>,
    recovery: Arc<std::sync::Mutex<Option<RecoveryState>>>,
    inspection: bool,
}
impl std::fmt::Debug for TorrentHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentHandle")
            .field("info_hash", &self.hash)
            .finish_non_exhaustive()
    }
}
struct JoinBarrier {
    // Keep authority alive even if the raw-cleanup finalizer itself unwinds.
    _resources: Arc<Mutex<lifecycle::Resources>>,
    state: Mutex<JoinState>,
    finalized: Arc<AtomicBool>,
    finished: tokio::task::AbortHandle,
}
struct JoinState {
    task: Option<JoinHandle<TorrentOutcome>>,
    outcome: Option<Arc<TorrentOutcome>>,
    status: Option<watch::Sender<TorrentStatus>>,
}
impl TorrentHandle {
    pub fn info_hash(&self) -> InfoHashV1 {
        self.hash
    }
    pub fn metainfo(&self) -> Option<OwnedMetainfo> {
        self.metadata.borrow().metainfo.clone()
    }
    pub fn recovery(&self) -> Option<RecoveryState> {
        self.recovery.lock().unwrap().clone()
    }
    pub async fn wait_metadata(&self) -> io::Result<OwnedMetainfo> {
        let mut metadata = self.metadata.clone();
        loop {
            {
                let state = metadata.borrow();
                if let Some(error) = &state.error {
                    return Err(io::Error::new(error.kind(), error.clone()));
                }
                if state.acknowledged {
                    return Ok(state.metainfo.clone().expect("acknowledged metadata"));
                }
            }
            let outcome = tokio::select! {
                outcome = self.outcome() => outcome,
                changed = metadata.changed() => match changed {
                    Ok(()) => continue,
                    Err(_) => self.outcome().await,
                }
            };
            if metadata.borrow().acknowledged {
                continue;
            }
            return Err(outcome.incomplete("execution ended before metadata resolved"));
        }
    }
    pub fn reannounce(&self) {
        self.reannounce.send_replace(());
    }
    pub fn request_stop(&self) {
        self.stop.send_replace(true);
    }
    pub fn snapshot(&self) -> TorrentStatus {
        self.status.borrow().clone()
    }
    /// Observe the latest status; false means the final publication was consumed.
    pub async fn changed(&mut self) -> bool {
        let handle = self.clone();
        tokio::select! {
            changed = self.status.changed() => changed.is_ok(),
            _ = handle.outcome() => self.status.changed().await.is_ok(),
        }
    }
    pub fn is_finished(&self) -> bool {
        self.join.finished.is_finished()
    }
    pub(in crate::engine) fn cleanup_unproven(&self) -> bool {
        self.is_finished() && !self.join.finalized.load(Ordering::Acquire)
    }
    pub async fn wait_complete(&self) -> io::Result<()> {
        if self.inspection {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metadata inspection has no payload",
            ));
        }
        let mut status = self.status.clone();
        loop {
            if status.borrow().progress.completed {
                return Ok(());
            }
            let outcome = tokio::select! {
                outcome = self.outcome() => outcome,
                changed = status.changed() => match changed {
                    Ok(()) => continue,
                    Err(_) => self.outcome().await,
                }
            };
            if outcome.wanted_complete {
                return Ok(());
            }
            return Err(outcome.incomplete("execution ended before wanted content completed"));
        }
    }
    pub async fn outcome(&self) -> Arc<TorrentOutcome> {
        let mut state = self.join.state.lock().await;
        if state.outcome.is_none() {
            let result = state.task.as_mut().expect("execution task").await;
            state.task = None;
            let outcome = result.unwrap_or_else(|error| {
                let p = self.snapshot().progress;
                TorrentOutcome {
                    termination: TorrentExit::Failed,
                    wanted_complete: p.completed,
                    full_complete: p.full_complete,
                    recheck_finished: p.exit == Some(TorrentExit::Rechecked),
                    cleanup_complete: false,
                    error: Some(Arc::new(io::Error::other(error))),
                }
            });
            state
                .status
                .take()
                .expect("final status publisher")
                .send_modify(|s| {
                    s.progress.phase = TorrentPhase::Stopped;
                    s.progress.exit = Some(outcome.termination);
                    s.progress.joined = outcome.cleanup_complete;
                });
            state.outcome = Some(Arc::new(outcome));
        }
        state.outcome.as_ref().expect("terminal outcome").clone()
    }
    pub async fn join(&self) -> io::Result<()> {
        self.outcome().await.result()
    }
    pub async fn stop(&self) -> io::Result<()> {
        self.request_stop();
        self.join().await
    }
    pub(in crate::engine) fn spawn(
        options: TorrentOptions,
        bandwidth: crate::engine::bandwidth::Throttle,
        networking: Networking,
        metadata_network: Option<crate::engine::session::metadata::Network>,
        rechecks: Arc<tokio::sync::Semaphore>,
        retirement: crate::engine::session::Retirement,
        inspection: bool,
    ) -> (Self, tokio::sync::oneshot::Sender<()>) {
        let hash = options.source.info_hash();
        let metainfo = match &options.source {
            TorrentSource::Metainfo(meta) => Some(meta.clone()),
            _ => None,
        };
        let initial = TorrentStatus {
            progress: TorrentProgress {
                total_bytes: metainfo.as_ref().map_or(0, |m| m.layout().total_length()),
                ..Default::default()
            },
            diagnostics: options
                .diagnostics
                .then(|| Arc::new(TorrentDiagnostics::default())),
        };
        let recovery = Arc::new(std::sync::Mutex::new(match &options.destination {
            Destination::Recover { state, .. } => Some(state.clone()),
            _ => None,
        }));
        let (metadata, metadata_receiver) = watch::channel(lifecycle::Metadata {
            acknowledged: metainfo.is_some(),
            metainfo,
            error: None,
        });
        let (reannounce, requests) = watch::channel(());
        let (stop, stopping) = watch::channel(false);
        let (status, observer) = watch::channel(initial);
        let resources = Arc::new(Mutex::new(lifecycle::Resources::default()));
        let inner_resources = resources.clone();
        let inner_status = status.clone();
        let inner_recovery = recovery.clone();
        let (start, installed) = tokio::sync::oneshot::channel();
        let runtime = tokio::spawn(
            async move {
                installed.await.expect("execution installed");
                lifecycle::run(
                    &mut *inner_resources.lock().await,
                    options,
                    lifecycle::Context {
                        bandwidth,
                        networking,
                        metadata_network,
                        rechecks,
                        requests,
                        stop: stopping,
                        status: inner_status,
                        metadata,
                        recovery: inner_recovery,
                        inspection,
                    },
                )
                .await
            }
            .instrument(tracing::Span::current()),
        );
        let cleanup_resources = resources.clone();
        let terminal = status.clone();
        let finalized = Arc::new(AtomicBool::new(false));
        let finished_cleaning = finalized.clone();
        let task = tokio::spawn(async move {
            let result = runtime
                .await
                .unwrap_or_else(|error| Err(io::Error::other(error)));
            let drained = cleanup_resources.lock().await.drain().await;
            let result = result.and(drained);
            let p = terminal.borrow().progress.clone();
            let outcome = TorrentOutcome {
                termination: if result.is_err() {
                    TorrentExit::Failed
                } else {
                    p.exit.expect("terminal exit")
                },
                wanted_complete: p.completed,
                full_complete: p.full_complete,
                recheck_finished: p.exit == Some(TorrentExit::Rechecked),
                cleanup_complete: true,
                error: result.err().map(Arc::new),
            };
            terminal.send_modify(|s| {
                s.progress.phase = TorrentPhase::Stopped;
                s.progress.exit = Some(outcome.termination);
            });
            finished_cleaning.store(true, Ordering::Release);
            retirement.retire();
            outcome
        });
        let finished = task.abort_handle();
        (
            Self {
                hash,
                metadata: metadata_receiver,
                recovery,
                inspection,
                reannounce,
                stop,
                status: observer,
                join: Arc::new(JoinBarrier {
                    _resources: resources,
                    state: Mutex::new(JoinState {
                        task: Some(task),
                        outcome: None,
                        status: Some(status),
                    }),
                    finalized,
                    finished,
                }),
            },
            start,
        )
    }
}
