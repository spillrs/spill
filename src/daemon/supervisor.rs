//! Intent reconciliation on one current-thread runtime; every admitted owner is joined.
use crate::{
    catalog::{self, Catalog, Intent, Row},
    config::Config,
};
use serde_json::{Value, json};
use spill::engine::{
    session::Session,
    torrent::{
        Destination, Persistence, RecoveryState, RunPolicy, TorrentHandle, TorrentOptions,
        TorrentPhase, TorrentSource,
    },
};
use std::{collections::HashMap, io};
use tokio::{sync::watch, time::Instant};

struct Running {
    handle: TorrentHandle,
    recheck: bool,
    previous_seeding: f64,
    downloaded: u64,
    uploaded: u64,
    seed_seconds: f64,
    previous_downloaded: u64,
    hash_failed_bytes: u64,
    hash_failed_pieces: u64,
    previous_hash_failed_bytes: u64,
    previous_hash_failed_pieces: u64,
    previous_uploaded: u64,
    last_download_at: Option<u64>,
    last_upload_at: Option<u64>,
    saved: Instant,
    complete: bool,
    /// Cleanup was not proven; its failure is recorded once and the session
    /// admits nothing more until restart.
    quarantined: bool,
}
pub struct Supervisor {
    pub config: Config,
    pub db: Catalog,
    session: Session,
    dht_saved: Instant,
    running: HashMap<String, Running>,
    pub shutdown: watch::Sender<bool>,
}
fn io_error(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}
impl Supervisor {
    pub async fn new(config: Config, db: Catalog) -> io::Result<Self> {
        let total = config.active_downloads + config.active_seeds;
        let limits = spill::engine::session::Limits {
            torrents: total,
            peers: total * config.peers,
            payload_bytes: total * (config.cache_bytes + config.write_queue_bytes),
            ..Default::default()
        };
        let mapping = config.port_mapping.then(|| {
            config
                .port_mapping_gateway
                .clone()
                .unwrap_or(spill::engine::session::port_map::Gateway::Discover)
        });
        let network = spill::engine::session::NetworkOptions {
            listen: config.listen,
            utp: config.utp,
            allow_local_network: config.allow_local_network,
            port_mapping: mapping,
            dht: if config.listen.is_ipv4() {
                Some(spill::engine::session::DhtOptions {
                    saved: db.load_dht().map_err(io_error)?,
                    routers: config.dht_bootstrap.clone(),
                })
            } else {
                eprintln!("DHT disabled: DHT requires an IPv4 session listener");
                None
            },
        };
        let session = Session::bind(network, limits).await?;
        Ok(Self {
            config,
            db,
            session,
            dht_saved: Instant::now(),
            running: HashMap::new(),
            shutdown: watch::channel(false).0,
        })
    }
    pub async fn stop(&mut self, id: &str) -> io::Result<()> {
        if let Some(mut running) = self.running.remove(id) {
            let result = running.handle.stop().await;
            let outcome = running.handle.outcome().await;
            if !outcome.cleanup_complete {
                let first = !running.quarantined;
                running.quarantined = true;
                self.running.insert(id.to_owned(), running);
                if first {
                    let error = result.as_ref().expect_err("unproven cleanup must fail");
                    self.db.failure(id, &error.to_string()).map_err(io_error)?;
                }
                return result;
            }
            Self::sample(&self.db, id, &mut running, true)?;
            if running.recheck && outcome.recheck_finished {
                self.db
                    .checkpoint(id, |c| c.complete = outcome.wanted_complete)
                    .map_err(io_error)?;
                self.db.intent(id, Intent::Paused).map_err(io_error)?;
            }
            if let Err(error) = result {
                self.db.failure(id, &error.to_string()).map_err(io_error)?;
            }
        }
        Ok(())
    }
    fn sample(db: &Catalog, id: &str, running: &mut Running, stopping: bool) -> io::Result<()> {
        let p = running.handle.snapshot().progress;
        let complete = (p.recovered && (p.completed || p.wanted_verified_bytes < p.wanted_bytes))
            .then_some(p.completed);
        if p.ready {
            db.recovered(id).map_err(io_error)?;
        }
        let downloaded = p.processed_payload_bytes - running.previous_downloaded;
        let uploaded = p.written_payload_bytes - running.previous_uploaded;
        running.downloaded += downloaded;
        running.uploaded += uploaded;
        running.hash_failed_bytes += p.hash_failed_bytes - running.previous_hash_failed_bytes;
        running.hash_failed_pieces += p.hash_failed_pieces - running.previous_hash_failed_pieces;
        running.previous_hash_failed_bytes = p.hash_failed_bytes;
        running.previous_hash_failed_pieces = p.hash_failed_pieces;
        running.previous_downloaded = p.processed_payload_bytes;
        running.previous_uploaded = p.written_payload_bytes;
        if downloaded != 0 {
            running.last_download_at = Some(catalog::now());
        }
        if uploaded != 0 {
            running.last_upload_at = Some(catalog::now());
        }
        let seeding = p.seeding_duration.as_secs_f64();
        running.seed_seconds += seeding - running.previous_seeding;
        running.previous_seeding = seeding;
        if stopping
            || p.joined
            || running.saved.elapsed().as_secs() >= 60
            || complete.is_some_and(|complete| complete != running.complete)
        {
            db.checkpoint(id, |c| {
                c.downloaded = running.downloaded;
                c.hash_failed_bytes = running.hash_failed_bytes;
                c.hash_failed_pieces = running.hash_failed_pieces;
                c.uploaded = running.uploaded;
                c.last_download_at = running.last_download_at;
                c.last_upload_at = running.last_upload_at;
                c.seed_seconds = running.seed_seconds;
                if let Some(complete) = complete {
                    c.complete = complete;
                }
            })
            .map_err(io_error)?;
            running.saved = Instant::now();
            if let Some(complete) = complete {
                running.complete = complete;
            }
        }
        Ok(())
    }
    pub async fn remove(&mut self, id: &str, delete: bool) -> io::Result<()> {
        self.stop(id).await?;
        let row = self
            .db
            .row(id)
            .map_err(io_error)?
            .ok_or(io::ErrorKind::NotFound)?;
        if delete && let Some(binding) = row.created_files.clone() {
            tokio::task::spawn_blocking(move || {
                crate::storage::remove_payload(&row.output_dir, &binding)
            })
            .await
            .map_err(io_error)??;
        }
        self.db.delete(id).map_err(io_error)
    }
    pub fn view(&self, row: &Row) -> Value {
        let running = self.running.get(&row.id);
        let (phase, status) = running
            .map(|r| {
                let snapshot = r.handle.snapshot();
                (
                    crate::observation::phase(&snapshot, r.recheck),
                    (!matches!(
                        snapshot.progress.phase,
                        TorrentPhase::Starting
                            | TorrentPhase::ResolvingMetadata
                            | TorrentPhase::PreparingDestination
                    ))
                    .then_some(snapshot),
                )
            })
            .unwrap_or_else(|| {
                let phase = match row.intent {
                    Intent::Paused => "paused",
                    Intent::Run if row.retry_at > catalog::now() => "retrying",
                    _ => "queued",
                };
                (phase, None)
            });
        let selected = row.meta().ok().filter(|_| status.is_none()).map(|meta| {
            let selection = if row.settings.file_priorities.is_empty() {
                spill::engine::selection::FileSelection::all(meta.layout())
            } else {
                spill::engine::selection::FileSelection::new(
                    meta.layout(),
                    &row.settings.file_priorities,
                )
                .expect("validated file priorities")
            };
            let verified = spill::protocol::resume::decode(meta, &row.checkpoint.resume)
                .map(|resume| selection.verified_bytes(meta.layout(), &resume.verified))
                .unwrap_or(0);
            (selection.wanted_bytes(), verified)
        });
        let wanted_bytes = status
            .as_ref()
            .map(|s| s.progress.wanted_bytes)
            .or_else(|| selected.map(|p| p.0));
        let wanted_verified_bytes = status
            .as_ref()
            .map(|s| s.progress.wanted_verified_bytes)
            .or_else(|| selected.map(|p| p.1));
        let completed = status
            .as_ref()
            .and_then(|s| {
                let p = &s.progress;
                (p.recovered && (p.completed || p.wanted_verified_bytes < p.wanted_bytes))
                    .then_some(p.completed)
            })
            .unwrap_or(row.checkpoint.complete);
        let verified = status
            .as_ref()
            .map(|s| s.progress.verified_bytes)
            .unwrap_or_else(|| {
                row.meta()
                    .ok()
                    .and_then(|m| {
                        spill::protocol::resume::resume_progress(m, &row.checkpoint.resume).ok()
                    })
                    .map_or(0, |p| p.verified_bytes)
            });
        let meta = row.meta().ok();
        let (uploaded, seed_seconds) = running
            .map(|r| (r.uploaded, r.seed_seconds))
            .unwrap_or((row.checkpoint.uploaded, row.checkpoint.seed_seconds));
        let (downloaded, last_download_at, last_upload_at) = running
            .map(|r| (r.downloaded, r.last_download_at, r.last_upload_at))
            .unwrap_or((
                row.checkpoint.downloaded,
                row.checkpoint.last_download_at,
                row.checkpoint.last_upload_at,
            ));
        let (hash_failed_bytes, hash_failed_pieces) = running
            .map(|r| (r.hash_failed_bytes, r.hash_failed_pieces))
            .unwrap_or((
                row.checkpoint.hash_failed_bytes,
                row.checkpoint.hash_failed_pieces,
            ));
        let transfer = status.as_ref().map(crate::observation::transfer);
        let tracker_error = status
            .as_ref()
            .and_then(|s| s.diagnostics.as_ref())
            .and_then(|d| d.trackers.iter().find_map(|t| t.error.as_ref()));
        let tracker_message = status
            .as_ref()
            .and_then(|s| s.diagnostics.as_ref())
            .and_then(|d| d.trackers.iter().find_map(|t| t.warning.as_ref()));
        json!({"id":row.id,"name":meta.as_ref().map(|m|String::from_utf8_lossy(m.name()).into_owned()),
            "intent":row.intent,"state":phase,"output_dir":row.output_dir,"settings":row.settings,
            "private":meta.as_ref().map(|m| m.is_private()),
            "verified_bytes":verified,"total_bytes":meta.as_ref().map(|m|m.layout().total_length()),
            "downloaded_bytes":downloaded,"uploaded_bytes":uploaded,"seed_seconds":seed_seconds,
            "hash_failed_bytes":hash_failed_bytes,"hash_failed_pieces":hash_failed_pieces,
            "completed":completed,"wanted_bytes":wanted_bytes,"wanted_verified_bytes":wanted_verified_bytes,
            "added_at":row.added_at,"completed_at":row.checkpoint.completed_at,
            "last_download_at":last_download_at,"last_upload_at":last_upload_at,
            "completed_announce":row.checkpoint.completed_announce,
            "last_error":row.last_error.as_ref().or(tracker_error),
            "tracker_message":tracker_message,"retry_at":row.retry_at,"transfer":transfer})
    }
    pub async fn reannounce(&self, hash: spill::protocol::identity::InfoHashV1) -> io::Result<()> {
        let observer = self
            .session
            .torrent(hash)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "torrent is not running"))?;
        observer.reannounce();
        Ok(())
    }
    fn failure(&self, id: &str, error: io::Error) {
        if self.running.get(id).is_some_and(|r| r.quarantined) {
            eprintln!("torrent {id}: {error}");
        } else if let Err(e) = self.db.failure(id, &error.to_string()) {
            eprintln!("torrent {id}: {error}; recording failure: {e}");
        }
    }
    pub async fn reconcile(&mut self) -> io::Result<()> {
        let ids: Vec<_> = self.running.keys().cloned().collect();
        for id in ids {
            if self.running[&id].quarantined {
                continue;
            }
            if self.running[&id].handle.is_finished() {
                if let Err(e) = self.stop(&id).await {
                    self.failure(&id, e);
                }
            } else {
                let running = self.running.get_mut(&id).expect("running execution");
                if let Err(e) = Self::sample(&self.db, &id, running, false) {
                    self.failure(&id, e);
                }
            }
        }
        let rows = self.db.rows().map_err(io_error)?;
        if self.dht_saved.elapsed().as_secs() >= 300 {
            let result = async {
                if let Some(saved) = self.session.dht_state().await? {
                    self.db.save_dht(saved).map_err(io_error)?;
                }
                Ok::<_, io::Error>(())
            }
            .await;
            if let Err(e) = result {
                eprintln!("saving DHT state: {e}");
            }
            self.dht_saved = Instant::now();
        }
        let (running, queued): (Vec<_>, Vec<_>) = rows
            .iter()
            .partition(|row| self.running.contains_key(&row.id));
        let (mut downloads, mut seeds) = (0, 0);
        for row in running.into_iter().chain(queued) {
            // Re-recording would keep advancing the persisted retry backoff.
            if self.running.get(&row.id).is_some_and(|r| r.quarantined) {
                continue;
            }
            if matches!(row.intent, Intent::RemoveKeep | Intent::RemoveDelete) {
                if row.retry_at > catalog::now() {
                    continue;
                }
                if let Err(e) = self
                    .remove(&row.id, row.intent == Intent::RemoveDelete)
                    .await
                {
                    self.failure(&row.id, e);
                }
                continue;
            }
            let running = self.running.get(&row.id);
            let complete = running
                .map(|r| r.complete)
                .unwrap_or(row.checkpoint.complete);
            let (uploaded, seed_seconds) = running
                .map(|r| (r.uploaded, r.seed_seconds))
                .unwrap_or((row.checkpoint.uploaded, row.checkpoint.seed_seconds));
            let seed_goal = complete
                && ((row.settings.seed_time != 0 && seed_seconds >= row.settings.seed_time as f64)
                    || (row.settings.seed_ratio > 0.0
                        && row.meta().ok().is_some_and(|m| {
                            uploaded as f64
                                >= m.layout().total_length() as f64 * row.settings.seed_ratio
                        })));
            if row.intent == Intent::Paused || seed_goal {
                if let Err(e) = self.stop(&row.id).await {
                    self.failure(&row.id, e);
                }
                if seed_goal && let Err(e) = self.db.intent(&row.id, Intent::Paused) {
                    self.failure(&row.id, io_error(e));
                }
                continue;
            }
            if !self.running.contains_key(&row.id) && row.retry_at > catalog::now() {
                continue;
            }
            let (used, cap) = if complete && row.intent != Intent::Recheck {
                (&mut seeds, self.config.active_seeds)
            } else {
                (&mut downloads, self.config.active_downloads)
            };
            if *used >= cap {
                if let Err(e) = self.stop(&row.id).await {
                    self.failure(&row.id, e);
                }
                continue;
            }
            *used += 1;
            if !self.running.contains_key(&row.id) && row.retry_at <= catalog::now() {
                let handle = match self.start(row) {
                    Ok(handle) => handle,
                    Err(error) => {
                        self.failure(&row.id, error);
                        continue;
                    }
                };
                self.running.insert(
                    row.id.clone(),
                    Running {
                        handle,
                        recheck: row.intent == Intent::Recheck,
                        previous_seeding: 0.0,
                        downloaded: row.checkpoint.downloaded,
                        uploaded: row.checkpoint.uploaded,
                        seed_seconds: row.checkpoint.seed_seconds,
                        previous_downloaded: 0,
                        hash_failed_bytes: row.checkpoint.hash_failed_bytes,
                        hash_failed_pieces: row.checkpoint.hash_failed_pieces,
                        previous_hash_failed_bytes: 0,
                        previous_hash_failed_pieces: 0,
                        previous_uploaded: 0,
                        last_download_at: row.checkpoint.last_download_at,
                        last_upload_at: row.checkpoint.last_upload_at,
                        saved: Instant::now(),
                        complete: row.checkpoint.complete,
                        quarantined: false,
                    },
                );
            }
        }
        Ok(())
    }
    pub async fn joined_shutdown(&mut self) -> io::Result<()> {
        for running in self.running.values() {
            running.handle.request_stop();
        }
        let ids: Vec<_> = self.running.keys().cloned().collect();
        let mut result = Ok(());
        for id in ids {
            if let Err(e) = self.stop(&id).await {
                result = Err(e);
            }
        }
        if let Err(e) = self.session.shutdown().await {
            result = Err(e);
        }
        match self.session.dht_state().await {
            Ok(Some(saved)) => {
                if let Err(e) = self.db.save_dht(saved) {
                    eprintln!("saving DHT state: {e}");
                }
            }
            Ok(None) => {}
            Err(e) => eprintln!("saving DHT state: {e}"),
        }
        result
    }
    fn start(&self, row: &Row) -> io::Result<TorrentHandle> {
        let source = match &row.metainfo {
            Some(_) => TorrentSource::Metainfo(row.meta()?.clone()),
            None => TorrentSource::Magnet(
                spill::protocol::magnet::Magnet::parse(
                    row.magnet.as_deref().expect("catalog source"),
                    Default::default(),
                )
                .map_err(io_error)?,
            ),
        };
        let destination = match &row.created_files {
            Some(binding) => Destination::Recover {
                root: row.output_dir.clone(),
                state: RecoveryState {
                    binding: binding.clone(),
                    resume: (!row.checkpoint.resume.is_empty())
                        .then(|| row.checkpoint.resume.clone()),
                    completed_announce: row.checkpoint.completed_announce,
                },
            },
            None => Destination::Open(row.output_dir.clone()),
        };
        let mut options = TorrentOptions::new(source, destination);
        options.profile = self.config.profile();
        options.settings = row.settings.transfer_settings();
        options.diagnostics = true;
        options.run = if row.intent == Intent::Recheck {
            RunPolicy::Recheck
        } else {
            RunPolicy::Persistent
        };
        let resolved_db = self.db.clone();
        let resolved_id = row.id.clone();
        let binding_db = self.db.clone();
        let binding_id = row.id.clone();
        let checkpoint_db = self.db.clone();
        let checkpoint_id = row.id.clone();
        let announce_db = self.db.clone();
        let announce_id = row.id.clone();
        options.persistence = Persistence {
            resolved: Some(Box::new(move |meta| {
                resolved_db.resolved(&resolved_id, meta).map_err(io_error)
            })),
            binding: Some(Box::new(move |binding| {
                binding_db.bind(&binding_id, &binding).map_err(io_error)
            })),
            checkpoint: Some(Box::new(move |resume| {
                checkpoint_db
                    .checkpoint(&checkpoint_id, |c| c.resume = resume)
                    .map_err(io_error)
            })),
            completion_announce: Some(Box::new(move |delivered| {
                announce_db
                    .checkpoint(&announce_id, |c| c.completed_announce = Some(delivered))
                    .map_err(io_error)
            })),
        };
        self.session.admit(options)
    }
    pub fn health(&self) -> Value {
        json!({"pid":std::process::id(),"running":self.running.len(),"listen":self.session.local_addr(),
            "advertised_port":self.session.advertised_port(),
            "port_mapping":self.session.port_mapping(),"admission_closed":self.session.admission_closed()})
    }
}
