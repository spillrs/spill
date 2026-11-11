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
use spill::protocol::identity::TorrentIdentity;
use std::{
    collections::HashMap,
    io,
    sync::{Arc, atomic::Ordering},
};
use tokio::{sync::watch, time::Instant};

struct Running {
    handle: TorrentHandle,
    recheck: bool,
    counters: catalog::Counters,
    complete: bool,
    previous: spill::engine::torrent::TorrentProgress,
    saved: Instant,
    /// Cleanup was not proven; its failure is recorded once and the session
    /// admits nothing more until restart.
    quarantined: bool,
}
pub struct Supervisor {
    pub config: Config,
    pub db: Catalog,
    session: Session,
    logging: Arc<crate::logging::Health>,
    dht_saved: Instant,
    failure_recording_failed: bool,
    running: HashMap<String, Running>,
    pub shutdown: watch::Sender<bool>,
}
fn inferred_completion(p: &spill::engine::torrent::TorrentProgress) -> Option<bool> {
    (p.recovered && (p.completed || p.wanted_verified_bytes < p.wanted_bytes))
        .then_some(p.completed)
}
fn io_error(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}
impl Supervisor {
    pub async fn new(
        config: Config,
        db: Catalog,
        logging: Arc<crate::logging::Health>,
    ) -> io::Result<Self> {
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
            peer_encryption: config.peer_encryption,
            allow_local_network: config.allow_local_network,
            lsd: config.allow_local_network,
            port_mapping: mapping,
            dht: Some(spill::engine::session::DhtOptions {
                saved: db.load_dht().map_err(io_error)?,
                routers: config.dht_bootstrap.clone(),
            }),
        };
        let session = Session::bind(network, limits).await?;
        Ok(Self {
            config,
            db,
            session,
            logging,
            dht_saved: Instant::now(),
            failure_recording_failed: false,
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
                    self.db.failure(id, error).map_err(io_error)?;
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
                self.db.failure(id, &error).map_err(io_error)?;
            }
        }
        Ok(())
    }
    fn sample(db: &Catalog, id: &str, running: &mut Running, stopping: bool) -> io::Result<()> {
        let p = running.handle.snapshot().progress;
        let complete = inferred_completion(&p);
        if p.ready {
            db.recovered(id).map_err(io_error)?;
        }
        let c = &mut running.counters;
        let downloaded = p.processed_payload_bytes - running.previous.processed_payload_bytes;
        let uploaded = p.written_payload_bytes - running.previous.written_payload_bytes;
        c.downloaded += downloaded;
        c.uploaded += uploaded;
        c.hash_failed_bytes += p.hash_failed_bytes - running.previous.hash_failed_bytes;
        c.hash_failed_pieces += p.hash_failed_pieces - running.previous.hash_failed_pieces;
        if downloaded != 0 {
            c.last_download_at = Some(catalog::now());
        }
        if uploaded != 0 {
            c.last_upload_at = Some(catalog::now());
        }
        c.seed_seconds +=
            p.seeding_duration.as_secs_f64() - running.previous.seeding_duration.as_secs_f64();
        running.previous = p;
        if stopping
            || running.previous.joined
            || running.saved.elapsed().as_secs() >= 60
            || complete.is_some_and(|complete| complete != running.complete)
        {
            // Resume data and announce delivery belong to engine callbacks.
            db.checkpoint(id, |saved| {
                saved.counters = *c;
                if let Some(complete) = complete {
                    saved.complete = complete;
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
                spill::engine::provisioning::NamedDestination::remove(
                    std::fs::File::open(&row.output_dir)?,
                    &binding,
                )
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
                        TorrentPhase::Starting | TorrentPhase::PreparingDestination
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
            let selection = spill::engine::selection::FileSelection::from_priorities(
                meta.layout(),
                &row.settings.file_priorities,
            )
            .expect("validated file priorities");
            let verified = spill::protocol::resume::decode(meta, &row.checkpoint.resume)
                .map(|resume| selection.verified_bytes(meta.layout(), &resume.verified))
                .unwrap_or(0);
            (selection.wanted_bytes(), verified)
        });
        let wanted_bytes = status
            .as_ref()
            .filter(|s| s.progress.metadata_ready)
            .map(|s| s.progress.wanted_bytes)
            .or_else(|| selected.map(|p| p.0));
        let wanted_verified_bytes = status
            .as_ref()
            .filter(|s| s.progress.metadata_ready)
            .map(|s| s.progress.wanted_verified_bytes)
            .or_else(|| selected.map(|p| p.1));
        let completed = status
            .as_ref()
            .and_then(|s| inferred_completion(&s.progress))
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
        let acquisition = running.map(|r| r.handle.snapshot().progress);
        let meta = row.meta().ok();
        let counters = running.map_or(&row.checkpoint.counters, |r| &r.counters);
        let transfer = status.as_ref().map(crate::observation::transfer);
        let tracker_message = status
            .as_ref()
            .and_then(|s| s.diagnostics.as_ref())
            .and_then(|d| {
                d.trackers
                    .iter()
                    .filter(|t| t.active)
                    .find_map(|t| t.warning.as_ref())
            });
        let tracker_error = status
            .as_ref()
            .and_then(|s| s.diagnostics.as_ref())
            .and_then(|d| {
                d.trackers.iter().filter(|t| t.active).find_map(|active| {
                    if active.state == "Success" {
                        None
                    } else {
                        active.error.as_ref().or_else(|| {
                            d.trackers
                                .iter()
                                .filter(|t| t.swarm == active.swarm)
                                .find_map(|t| t.error.as_ref())
                        })
                    }
                })
            });
        json!({"id":row.id,"name":meta.as_ref().map(|m|String::from_utf8_lossy(m.name()).into_owned()),
            "intent":row.intent,"state":phase,"output_dir":row.output_dir,"settings":row.settings,
            "private":meta.as_ref().map(|m| m.is_private()),
            "verified_bytes":verified,"total_bytes":meta.as_ref().map(|m|m.layout().total_length()),
            "downloaded_bytes":counters.downloaded,"uploaded_bytes":counters.uploaded,"seed_seconds":counters.seed_seconds,
            "hash_failed_bytes":counters.hash_failed_bytes,"hash_failed_pieces":counters.hash_failed_pieces,
            "completed":completed,"wanted_bytes":wanted_bytes,"wanted_verified_bytes":wanted_verified_bytes,
            "added_at":row.added_at,"completed_at":row.checkpoint.completed_at,
            "last_download_at":counters.last_download_at,"last_upload_at":counters.last_upload_at,
            "format":match row.identity {
                TorrentIdentity::Hybrid(..) => "hybrid",
                TorrentIdentity::V1(_) => "v1",
                TorrentIdentity::V2(_) => "v2",
            },
            "v1":row.identity.v1().map(|hash| hash.to_string()),"v2":row.identity.v2().map(|hash| hash.to_string()),"metadata_ready":row.parsed.is_some(),
            "info_validated":row.parsed.is_some() || acquisition.as_ref().is_some_and(|p| p.info_validated),
            "hash_bytes":acquisition.as_ref().map_or(0, |p| p.hash_bytes),
            "hash_total_bytes":acquisition.as_ref().map_or(0, |p| p.hash_total_bytes),
            "layers_pending":meta.is_some_and(|meta| meta.layers_pending()),
            "completed_announces":row.checkpoint.completed_announces.iter().map(|(key, delivered)|
                json!({"swarm":crate::encoding::hex_key(*key),"delivered":delivered})).collect::<Vec<_>>(),
            "last_error":row.last_error,
            "tracker_error":tracker_error,"tracker_message":tracker_message,
            "retry_at":row.retry_at,"transfer":transfer})
    }
    pub async fn reannounce(
        &self,
        hash: spill::protocol::identity::TorrentIdentity,
    ) -> io::Result<()> {
        let observer = self
            .session
            .torrent(hash)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "torrent is not running"))?;
        observer.reannounce();
        Ok(())
    }
    fn failure(&mut self, id: &str, error: io::Error) {
        if self.running.get(id).is_some_and(|r| r.quarantined) {
            tracing::error!(torrent_id = id, %error, "torrent_quarantined");
        } else {
            match self.db.failure(id, &error) {
                Ok(()) => {
                    if self.failure_recording_failed {
                        tracing::info!("torrent_failure_record_recovered");
                    }
                    self.failure_recording_failed = false;
                }
                Err(catalog_error) => {
                    if !self.failure_recording_failed {
                        tracing::error!(torrent_id = id, %error, %catalog_error, "torrent_failure_record_failed");
                    }
                    self.failure_recording_failed = true;
                }
            }
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
                tracing::warn!(error = %e, "dht_state_save_failed");
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
            let complete = running.map_or(row.checkpoint.complete, |r| r.complete);
            let counters = running.map_or(&row.checkpoint.counters, |r| &r.counters);
            let seed_goal = complete
                && ((row.settings.seed_time != 0
                    && counters.seed_seconds >= row.settings.seed_time as f64)
                    || (row.settings.seed_ratio > 0.0
                        && row.meta().ok().is_some_and(|m| {
                            counters.uploaded as f64
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
                        counters: row.checkpoint.counters,
                        complete: row.checkpoint.complete,
                        previous: Default::default(),
                        saved: Instant::now(),
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
                    tracing::warn!(error = %e, "dht_state_save_failed");
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "dht_state_save_failed"),
        }
        result
    }
    fn start(&self, row: &Row) -> io::Result<TorrentHandle> {
        let span = tracing::info_span!("torrent", torrent_id = %row.id);
        let _entered = span.enter();
        tracing::info!(retry = row.retry_at != 0, "torrent_starting");
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
                    completed_announces: row.checkpoint.completed_announces.clone(),
                },
            },
            None => Destination::Open(row.output_dir.clone()),
        };
        let mut options = TorrentOptions::new(source, destination);
        options.profile = self.config.profile();
        options.settings = row.settings.transfer_settings();
        options.diagnostics = true;
        options.log_peer_endpoints = self.config.logging.peer_endpoints;
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
                resolved_db
                    .resolved(&resolved_id, meta)
                    .map_err(|error| match error {
                        catalog::Error::Duplicate => {
                            spill::engine::profile::AdmissionError::Duplicate.into()
                        }
                        _ => io_error(error),
                    })
            })),
            binding: Some(Box::new(move |binding| {
                binding_db.bind(&binding_id, &binding).map_err(io_error)
            })),
            checkpoint: Some(Box::new(move |resume| {
                checkpoint_db
                    .checkpoint(&checkpoint_id, |c| c.resume = resume)
                    .map_err(io_error)
            })),
            completion_announce: Some(Box::new(move |key, delivered| {
                announce_db
                    .checkpoint(&announce_id, |c| {
                        if let Some((_, state)) = c
                            .completed_announces
                            .iter_mut()
                            .find(|(swarm, _)| *swarm == key)
                        {
                            *state = delivered;
                        } else {
                            c.completed_announces.push((key, delivered));
                        }
                    })
                    .map_err(io_error)
            })),
        };
        self.session.admit(options)
    }
    pub async fn health(&self) -> Value {
        json!({"version":env!("CARGO_PKG_VERSION"),"pid":std::process::id(),"running":self.running.len(),"listen":self.session.local_addr(),
            "advertised_port":self.session.advertised_port(),
            "port_mapping":self.session.port_mapping(),"admission_closed":self.session.admission_closed(),
            "network":self.session.network_status().await,
            "logging":{"file_enabled":self.config.logging.file_enabled,
                "events_dropped":self.logging.events_dropped.load(Ordering::Relaxed),
                "write_failed":self.logging.write_failed.load(Ordering::Relaxed)}})
    }
}
