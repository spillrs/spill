use super::*;
use crate::engine::{
    provisioning::{FileStorage, NamedDestination},
    storage::writer::Writer,
};
use std::fs::File;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub(super) struct Metadata {
    pub metainfo: Option<OwnedMetainfo>,
    pub acknowledged: bool,
    pub error: Option<Arc<io::Error>>,
}

#[derive(Default)]
pub(super) struct Resources {
    metadata: metadata::Resources,
    preparation: Option<JoinHandle<io::Result<Option<FileStorage>>>>,
    acknowledgement: Option<JoinHandle<io::Result<()>>>,
    runtime: runtime::Resources,
}
impl Resources {
    pub(super) async fn drain(&mut self) -> io::Result<()> {
        let metadata = self.metadata.drain().await;
        let acknowledgement = crate::engine::join_blocking(&mut self.acknowledgement).await;
        let preparation = crate::engine::join_blocking(&mut self.preparation).await;
        let runtime = self.runtime.drain().await;
        metadata.and(acknowledgement).and(preparation).and(runtime)
    }
}

pub(super) struct Context {
    pub bandwidth: crate::engine::bandwidth::Throttle,
    pub networking: Networking,
    pub rechecks: Arc<Semaphore>,
    pub requests: watch::Receiver<()>,
    pub stop: watch::Receiver<bool>,
    pub status: watch::Sender<TorrentStatus>,
    pub metadata: watch::Sender<Metadata>,
    pub recovery: Arc<std::sync::Mutex<Option<RecoveryState>>>,
    pub inspection: bool,
    pub retirement: crate::engine::session::Retirement,
}

pub(super) async fn run(
    resources: &mut Resources,
    mut options: TorrentOptions,
    mut context: Context,
) -> io::Result<()> {
    let stopped = || {
        context
            .status
            .send_modify(|s| s.progress.exit = Some(TorrentExit::StopRequested));
        Ok(())
    };
    if *context.stop.borrow() {
        return stopped();
    }
    let callback = options.persistence.resolved.take();
    let metadata = context.metadata.clone();
    let status = context.status.clone();
    let resolved: Arc<dyn Fn(OwnedMetainfo) -> io::Result<()> + Send + Sync> =
        Arc::new(move |meta| {
            if let Some(callback) = &callback {
                callback(meta.clone())?;
            }
            status.send_modify(|s| s.progress.layers_pending = meta.layers_pending());
            metadata.send_modify(|m| {
                m.metainfo = Some(meta);
                m.acknowledged = true;
            });
            Ok(())
        });
    let (meta, selection) = match &options.source {
        TorrentSource::Metainfo(meta) => (
            meta.clone(),
            crate::engine::selection::FileSelection::from_priorities(
                meta.layout(),
                &options.settings.file_priorities,
            )?,
        ),
        TorrentSource::Magnet(magnet) => {
            context
                .status
                .send_modify(|s| s.progress.phase = TorrentPhase::ResolvingMetadata);
            let Networking::Online(network) = &mut context.networking else {
                unreachable!("online magnet");
            };
            let result = network
                .resolve(
                    &mut resources.metadata,
                    magnet,
                    &options,
                    context.bandwidth.clone(),
                    context.stop.clone(),
                    &context.status,
                )
                .await;
            let meta = match result {
                Err(error)
                    if error.kind() == io::ErrorKind::Interrupted && *context.stop.borrow() =>
                {
                    return stopped();
                }
                other => other?,
            };
            assert!(magnet.info_hash().matches_info_bytes(meta.info_bytes()));
            let serving = options.run != RunPolicy::Recheck && !context.inspection;
            if !serving {
                resources.metadata.drain().await?;
                context.networking = Networking::Recheck;
            }
            context.retirement.resolve(&meta, serving)?;
            if options.settings.file_priorities.is_empty()
                && let Some(ranges) = magnet.select_only()
            {
                options.settings.file_priorities =
                    crate::engine::selection::FileSelection::select_only(meta.layout(), ranges)
                        .file_priorities()
                        .to_vec();
            }
            let selection = crate::engine::selection::FileSelection::from_priorities(
                meta.layout(),
                &options.settings.file_priorities,
            )?;
            context
                .metadata
                .send_modify(|m| m.metainfo = Some(meta.clone()));
            let callback = resolved.clone();
            let pending = meta.clone();
            let result = crate::engine::run_blocking(&mut resources.acknowledgement, move || {
                callback(pending)
            })
            .await;
            if let Err(error) = result {
                let error = Arc::new(error);
                context
                    .metadata
                    .send_modify(|m| m.error = Some(error.clone()));
                return Err(io::Error::new(error.kind(), error));
            }
            (meta, selection)
        }
    };
    context.status.send_modify(|s| {
        s.identity = meta.info_hash();
        s.progress.metadata_ready = true;
        s.progress.info_validated = true;
        s.progress.layers_pending = meta.layers_pending();
        s.progress.total_bytes = meta.layout().total_length();
    });
    if context.inspection {
        context
            .status
            .send_modify(|s| s.progress.exit = Some(TorrentExit::MetadataResolved));
        return Ok(());
    }
    if *context.stop.borrow() {
        return stopped();
    }
    if meta.is_private() {
        options.settings.dht = false;
        options.settings.pex = false;
    }
    let peer_id = match resources.metadata.peer_id {
        Some(peer_id) => peer_id,
        None => crate::engine::identity::generate_peer_id()?,
    };
    context
        .status
        .send_modify(|s| s.progress.phase = TorrentPhase::PreparingDestination);
    let destination = options.destination;
    let prepared_meta = meta.clone();
    let recovery = context.recovery.clone();
    let binding_callback = options.persistence.binding.take();
    let stopping = context.stop.clone();
    let prepared = crate::engine::run_blocking(&mut resources.preparation, move || {
        let cancelled = std::cell::Cell::new(false);
        let canceled = || {
            let stop = *stopping.borrow();
            if stop {
                cancelled.set(true);
            }
            stop
        };
        let record = |binding: &DestinationBinding| {
            *recovery.lock().unwrap() = Some(RecoveryState {
                binding: binding.clone(),
                resume: None,
                completed_announces: Vec::new(),
            });
            if let Some(callback) = binding_callback {
                callback(binding.clone())?;
            }
            Ok(())
        };
        let result = (|| match destination {
            Destination::Open(root) => NamedDestination::open_cancellable(
                File::open(root)?,
                &prepared_meta,
                record,
                canceled,
            ),
            Destination::Create(root) => {
                NamedDestination::create(File::open(root)?, &prepared_meta, record, canceled)
            }
            Destination::Adopt(root) => {
                let storage = NamedDestination::adopt_complete_cancellable(
                    File::open(root)?,
                    &prepared_meta,
                    canceled,
                )?
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "seed payload is incomplete")
                })?;
                record(storage.binding())?;
                Ok(storage)
            }
            Destination::Recover { root, state } => NamedDestination::reopen_for_recovery(
                File::open(root)?,
                &prepared_meta,
                &state.binding,
                canceled,
            ),
        })();
        // Only the provisioning cancellation probe marks this, not callback errors.
        match result {
            Err(_) if cancelled.get() => Ok(None),
            other => other.map(Some),
        }
    })
    .await;
    let Some(storage) = prepared? else {
        return stopped();
    };
    let (resume, completed_announces) = {
        let recovery = context.recovery.lock().unwrap();
        let state = recovery.as_ref().expect("recorded destination");
        (
            storage
                .verified_resume()
                .map(<[u8]>::to_vec)
                .or_else(|| state.resume.clone()),
            state.completed_announces.clone(),
        )
    };
    if *context.stop.borrow() {
        drop(storage);
        return stopped();
    }
    let recovery = context.recovery.clone();
    let checkpoint = options.persistence.checkpoint;
    resources.runtime.writer = Some(Writer::spawn(
        storage,
        meta.clone(),
        options.profile,
        options.run == RunPolicy::Recheck,
        resume,
        Persistence {
            checkpoint: Some(Box::new(move |resume| {
                if let Some(callback) = &checkpoint {
                    callback(resume.clone())?;
                }
                recovery
                    .lock()
                    .unwrap()
                    .as_mut()
                    .expect("recorded destination")
                    .resume = Some(resume);
                Ok(())
            })),
            resolved: Some(Box::new(move |meta| resolved(meta))),
            ..Default::default()
        },
        context.rechecks,
    ));
    let recovery = context.recovery.clone();
    let announce = options.persistence.completion_announce;
    let runtime_options = RuntimeOptions {
        reannounce: context.requests,
        completed_announces,
        announce_checkpoint: Arc::new(move |hash, delivered| {
            if let Some(callback) = &announce {
                callback(hash, delivered)?;
            }
            let mut recovery = recovery.lock().unwrap();
            let states = &mut recovery
                .as_mut()
                .expect("recorded destination")
                .completed_announces;
            if let Some((_, state)) = states.iter_mut().find(|(key, _)| *key == hash) {
                *state = delivered;
            } else {
                states.push((hash, delivered));
            }
            Ok(())
        }),
        settings: options.settings,
        metainfo: meta,
        profile: options.profile,
        peers: options.peers,
        lsd_peers: std::mem::take(&mut resources.metadata.lsd_peers),
        dht: None,
        run: options.run,
        log_peer_endpoints: options.log_peer_endpoints,
        no_progress_timeout: options.no_progress_timeout,
        diagnostics: options.diagnostics,
        selection,
        peer_id,
        bandwidth: context.bandwidth,
    };
    resources.runtime.transfer = resources.metadata.shell.take();
    resources.runtime.progress = resources.metadata.progress.take();
    runtime::run(
        &mut resources.runtime,
        runtime_options,
        context.networking,
        context.stop,
        context.status,
    )
    .await
}
