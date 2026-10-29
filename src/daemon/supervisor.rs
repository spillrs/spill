//! Intent reconciliation on one current-thread runtime; every admitted owner is joined.
use crate::{
    catalog::{self, Catalog, Intent, Row},
    config::Config,
};
use serde_json::{Value, json};
use spill::engine::{
    provisioning::NamedDestination,
    session::Session,
    torrent::{RunPolicy, TorrentExit, TorrentOptions, TorrentStatus},
};
use std::{
    collections::HashMap,
    fs::File,
    io,
    sync::{Arc, Mutex as StdMutex},
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::Instant,
};

type SharedSession = Arc<Mutex<Session>>;
#[derive(Default)]
struct Live {
    phase: String,
    transfer: Option<TorrentStatus>,
}
struct Running {
    stop: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
    live: Arc<StdMutex<Live>>,
    last: Instant,
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
    seed_active: bool,
}
pub struct Supervisor {
    pub config: Config,
    pub db: Catalog,
    session: SharedSession,
    dht: Option<spill::engine::session::dht::Handle>,
    dht_saved: Instant,
    dht_attempted: bool,
    running: HashMap<String, Running>,
    pub shutdown: watch::Sender<bool>,
}
fn io_error(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}
fn completion(status: &TorrentStatus) -> Option<bool> {
    let p = &status.progress;
    // Fully verified bytes still await the durability barrier on seed recovery.
    (p.ready && (p.completed || p.wanted_verified_bytes < p.wanted_bytes)).then_some(p.completed)
}
impl Supervisor {
    pub async fn new(config: Config, db: Catalog) -> io::Result<Self> {
        let total = config.active_downloads + config.active_seeds;
        let limits = spill::engine::session::Limits {
            allow_local_network: config.allow_local_network,
            utp: config.utp,
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
        let mut session = Session::bind(config.listen, limits, mapping).await?;
        let dht = if config.defaults.dht {
            Some(
                session
                    .enable_dht(spill::engine::session::DhtOptions {
                        allow_network: true,
                        saved: db.load_dht().map_err(io_error)?,
                        routers: config.dht_bootstrap.clone(),
                    })
                    .await?,
            )
        } else {
            None
        };
        Ok(Self {
            dht_attempted: config.defaults.dht,
            config,
            db,
            session: Arc::new(Mutex::new(session)),
            dht,
            dht_saved: Instant::now(),
            running: HashMap::new(),
            shutdown: watch::channel(false).0,
        })
    }
    pub async fn stop(&mut self, id: &str) -> io::Result<()> {
        if let Some(mut running) = self.running.remove(id) {
            running.stop.send_replace(true);
            let result = (&mut running.task).await.map_err(io_error)?;
            self.sample(id, &mut running, true)?;
            if let Err(error) = result
                && error.kind() != io::ErrorKind::Interrupted
            {
                self.db.failure(id, &error.to_string()).map_err(io_error)?;
            }
        }
        Ok(())
    }
    fn sample(&self, id: &str, running: &mut Running, stopping: bool) -> io::Result<()> {
        let live = running.live.lock().unwrap();
        if let Some(s) = &live.transfer {
            let complete = completion(s);
            if s.progress.ready {
                self.db.recovered(id).map_err(io_error)?;
            }
            let downloaded = s.progress.processed_payload_bytes - running.previous_downloaded;
            let uploaded = s.progress.written_payload_bytes - running.previous_uploaded;
            running.downloaded += downloaded;
            running.hash_failed_bytes +=
                s.progress.hash_failed_bytes - running.previous_hash_failed_bytes;
            running.hash_failed_pieces +=
                s.progress.hash_failed_pieces - running.previous_hash_failed_pieces;
            running.previous_hash_failed_bytes = s.progress.hash_failed_bytes;
            running.previous_hash_failed_pieces = s.progress.hash_failed_pieces;
            running.uploaded += uploaded;
            if downloaded != 0 {
                running.last_download_at = Some(catalog::now());
            }
            if uploaded != 0 {
                running.last_upload_at = Some(catalog::now());
            }
            running.previous_downloaded = s.progress.processed_payload_bytes;
            running.previous_uploaded = s.progress.written_payload_bytes;
            if running.seed_active {
                running.seed_seconds += running.last.elapsed().as_secs_f64();
            }
            running.seed_active = s.progress.ready && s.progress.completed && !s.progress.joined;
            if stopping
                || s.progress.joined
                || running.saved.elapsed().as_secs() >= 60
                || complete.is_some_and(|complete| complete != running.complete)
            {
                self.db
                    .checkpoint(id, |c| {
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
        }
        running.last = Instant::now();
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
        let (phase, status) = self
            .running
            .get(&row.id)
            .map(|r| {
                let live = r.live.lock().unwrap();
                (live.phase.clone(), live.transfer.clone())
            })
            .unwrap_or_else(|| {
                let phase = match row.intent {
                    Intent::Paused => "paused",
                    Intent::Run if row.retry_at > catalog::now() => "retrying",
                    _ => "queued",
                };
                (phase.into(), None)
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
            .and_then(completion)
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
        let (uploaded, seed_seconds) = self
            .running
            .get(&row.id)
            .map(|r| (r.uploaded, r.seed_seconds))
            .unwrap_or((row.checkpoint.uploaded, row.checkpoint.seed_seconds));
        let (downloaded, last_download_at, last_upload_at) = self
            .running
            .get(&row.id)
            .map(|r| (r.downloaded, r.last_download_at, r.last_upload_at))
            .unwrap_or((
                row.checkpoint.downloaded,
                row.checkpoint.last_download_at,
                row.checkpoint.last_upload_at,
            ));
        let (hash_failed_bytes, hash_failed_pieces) = self
            .running
            .get(&row.id)
            .map(|r| (r.hash_failed_bytes, r.hash_failed_pieces))
            .unwrap_or((
                row.checkpoint.hash_failed_bytes,
                row.checkpoint.hash_failed_pieces,
            ));
        let transfer = status.as_ref().map(|s| {
            json!({"ready":s.progress.ready,"completed":s.progress.completed,
            "active_peers":s.progress.active_peers,"downloaded_bytes":s.progress.processed_payload_bytes,
            "uploaded_bytes":s.progress.written_payload_bytes,
            "hash_failed_bytes":s.progress.hash_failed_bytes,"hash_failed_pieces":s.progress.hash_failed_pieces,
            "download_bytes_per_second":s.progress.download_bytes_per_second,
            "upload_bytes_per_second":s.progress.upload_bytes_per_second,
            "utp_retransmits":s.diagnostics.as_ref().map(|d| d.utp_retransmits),
            "pending_contacts":s.diagnostics.as_ref().map(|d| d.pending_contacts),
            "writer_free_slots":s.diagnostics.as_ref().map(|d| d.writer_free_slots),
            "upload_reads_in_flight":s.diagnostics.as_ref().map(|d| d.upload_reads_in_flight),
            "upload_reads_finished":s.diagnostics.as_ref().map(|d| d.upload_reads_finished),
            "peers":s.diagnostics.as_ref().map(|d| d.peers.as_slice()).unwrap_or_default(),
            "trackers":s.diagnostics.as_ref().map(|d| d.trackers.as_slice()).unwrap_or_default()})
        });
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
        let session = self.session.lock().await;
        let observer = session
            .torrent(hash)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "torrent is not running"))?;
        observer.reannounce();
        Ok(())
    }
    fn failure(&self, id: &str, error: io::Error) {
        if let Err(e) = self.db.failure(id, &error.to_string()) {
            eprintln!("torrent {id}: {error}; recording failure: {e}");
        }
    }
    pub async fn reconcile(&mut self) -> io::Result<()> {
        let ids: Vec<_> = self.running.keys().cloned().collect();
        for id in ids {
            if self.running[&id].task.is_finished() {
                if let Err(e) = self.stop(&id).await {
                    self.failure(&id, e);
                }
            } else {
                let mut running = self.running.remove(&id).unwrap();
                if let Err(e) = self.sample(&id, &mut running, false) {
                    self.failure(&id, e);
                }
                self.running.insert(id, running);
            }
        }
        let rows = self.db.rows().map_err(io_error)?;
        if !self.dht_attempted
            && rows
                .iter()
                .any(|r| r.settings.dht && r.intent == Intent::Run)
        {
            self.dht_attempted = true;
            let result = async {
                self.session
                    .lock()
                    .await
                    .enable_dht(spill::engine::session::DhtOptions {
                        allow_network: true,
                        saved: self.db.load_dht().map_err(io_error)?,
                        routers: self.config.dht_bootstrap.clone(),
                    })
                    .await
            }
            .await;
            match result {
                Ok(handle) => self.dht = Some(handle),
                Err(e) => eprintln!("DHT disabled: {e}"),
            }
        }
        if self.dht_saved.elapsed().as_secs() >= 300 {
            let result = async {
                if let Some(saved) = self.session.lock().await.dht_state().await? {
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
                .and_then(|r| {
                    r.live
                        .lock()
                        .unwrap()
                        .transfer
                        .as_ref()
                        .and_then(completion)
                })
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
                let (stop, stopping) = watch::channel(false);
                let live = Arc::new(StdMutex::new(Live {
                    phase: "starting".into(),
                    transfer: None,
                }));
                let task = tokio::spawn(run(
                    row.clone(),
                    self.config.clone(),
                    self.db.clone(),
                    self.session.clone(),
                    self.dht.clone(),
                    live.clone(),
                    stopping,
                ));
                self.running.insert(
                    row.id.clone(),
                    Running {
                        stop,
                        task,
                        live,
                        last: Instant::now(),
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
                        seed_active: false,
                    },
                );
            }
        }
        Ok(())
    }
    pub async fn joined_shutdown(&mut self) -> io::Result<()> {
        for running in self.running.values() {
            running.stop.send_replace(true);
        }
        let ids: Vec<_> = self.running.keys().cloned().collect();
        let mut result = Ok(());
        for id in ids {
            if let Err(e) = self.stop(&id).await {
                result = Err(e);
            }
        }
        let mut session = self.session.lock().await;
        if let Err(e) = session.shutdown().await {
            result = Err(e);
        }
        match session.dht_state().await {
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
    pub fn health(&self) -> Value {
        let session = self.session.try_lock().ok();
        json!({"pid":std::process::id(),"running":self.running.len(),"listen":session.as_ref().map(|s|s.local_addr()),
            "advertised_port":session.as_ref().map(|s|s.advertised_port()),
            "port_mapping":session.as_ref().map(|s|s.port_mapping())})
    }
}
async fn run(
    mut row: Arc<Row>,
    config: Config,
    db: Catalog,
    session: SharedSession,
    dht: Option<spill::engine::session::dht::Handle>,
    live: Arc<StdMutex<Live>>,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    if row.metainfo.is_none() {
        live.lock().unwrap().phase = "metadata".into();
        let magnet = spill::protocol::magnet::Magnet::parse(
            row.magnet.as_deref().ok_or(io::ErrorKind::InvalidData)?,
            Default::default(),
        )
        .map_err(io_error)?;
        let mut network = session.lock().await.metadata_network()?;
        let meta = match network
            .resolve(
                &magnet,
                row.settings.trackers,
                row.settings.dht,
                config.profile(),
                stop.clone(),
            )
            .await
        {
            Err(_) if *stop.borrow() => return Ok(()),
            other => other?,
        };
        db.resolved(&row.id, meta).map_err(io_error)?;
        row = db
            .row(&row.id)
            .map_err(io_error)?
            .ok_or(io::ErrorKind::NotFound)?;
    }
    if *stop.borrow() {
        return Ok(());
    }
    let meta = row.meta()?.clone();
    if meta.info_hash().to_string() != row.id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metainfo identity mismatch",
        ));
    }
    live.lock().unwrap().phase = "recovering".into();
    let output = row.output_dir.clone();
    let binding = row.created_files.clone();
    let prepared_meta = meta.clone();
    let canceled = stop.clone();
    let profile = config.profile();
    let binding_db = db.clone();
    let binding_id = row.id.clone();
    let sink = tokio::task::spawn_blocking(move || {
        let root = File::open(&output)?;
        if let Some(binding) = binding {
            NamedDestination::reopen_for_recovery(root, &prepared_meta, &binding, || {
                *canceled.borrow()
            })
        } else {
            NamedDestination::open_cancellable(
                root,
                &prepared_meta,
                |binding| binding_db.bind(&binding_id, binding).map_err(io_error),
                || *canceled.borrow(),
            )
        }
    })
    .await
    .map_err(io_error)??;
    let resume = sink
        .verified_resume()
        .map(|b| b.to_vec())
        .or_else(|| (!row.checkpoint.resume.is_empty()).then(|| row.checkpoint.resume.clone()));
    if *stop.borrow() {
        return Ok(());
    }
    let recheck = row.intent == Intent::Recheck;
    let checkpoint_db = db.clone();
    let checkpoint_id = row.id.clone();
    let announce_db = db.clone();
    let announce_id = row.id.clone();
    let builder = TorrentOptions::new(meta, profile)
        .settings(row.settings.clone())
        .storage(sink)
        .completion_announce(row.checkpoint.completed_announce, move |acknowledged| {
            announce_db
                .checkpoint(&announce_id, |c| c.completed_announce = Some(acknowledged))
                .map_err(io_error)
        })
        .resume(resume)
        .on_checkpoint(move |resume| {
            checkpoint_db
                .checkpoint(&checkpoint_id, |c| c.resume = resume)
                .map_err(io_error)
        })
        .dht(dht.filter(|_| !recheck && row.settings.dht))
        .diagnostics(true)
        .run(if recheck {
            RunPolicy::Recheck
        } else {
            RunPolicy::Persistent
        });
    let (hash, mut observer) = {
        let mut session = session.lock().await;
        let hash = session.admit(builder).map_err(io_error)?;
        (hash, session.torrent(hash).unwrap())
    };
    loop {
        let snapshot = observer.snapshot();
        let joined = snapshot.progress.joined;
        {
            let mut live = live.lock().unwrap();
            live.phase = if recheck {
                "checking"
            } else if snapshot.progress.completed {
                "seeding"
            } else {
                "downloading"
            }
            .into();
            live.transfer = Some(snapshot);
        }
        if *stop.borrow() || joined {
            break;
        }
        tokio::select! { _ = stop.changed() => {}, changed = observer.changed() => { if !changed { break; } } }
    }
    let owner = session.lock().await.take(hash).expect("owned torrent");
    let joined = owner.join().await;
    session.lock().await.finish_remove(&owner);
    let snapshot = observer.snapshot();
    if recheck && joined.is_ok() {
        // A completed scan is authoritative even though it never sets network readiness.
        if snapshot.progress.exit == Some(TorrentExit::Complete) {
            db.checkpoint(&row.id, |c| c.complete = snapshot.progress.completed)
                .map_err(io_error)?;
        }
        db.intent(&row.id, Intent::Paused).map_err(io_error)?;
    }
    live.lock().unwrap().transfer = Some(snapshot);
    joined.map_err(io_error)
}
