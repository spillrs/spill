//! One file-storage-owning task and a byte-bounded block queue. Commands run to
//! completion even if their observers disappear. Closing then joining drains I/O.
use crate::engine::core::file_progress::FileProgress;

use super::cache::{self, CacheView};
use crate::engine::profile::{MAX_UPLOAD_READS, Profile};
use crate::engine::{
    core::{SharedBlock, live},
    provisioning::FileStorage,
};
use crate::protocol::{metainfo::OwnedMetainfo, resume};
use sha1::{Digest, Sha1};
use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{Mutex, Notify, Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

// Preserve two-way write/hash overlap for common pieces without letting the
// metainfo's admitted piece size determine each job's allocation.
const MAX_BUFFERED_HASH_BYTES: usize = 4 * 1024 * 1024;
pub(in crate::engine) const RECHECK_CONCURRENCY: usize = 4;

struct ReadResult {
    index: u32,
    offset: u32,
    length: u32,
    start: u32,
    bytes: io::Result<Vec<u8>>,
}
enum Command {
    Write(u32, u32, Arc<()>, SharedBlock),
    Hash(u32, Arc<()>),
    Hashed(u32, bool),
    Read(u32, u32, u32),
    ReadDone(ReadResult),
    Flush,
}
pub(in crate::engine) enum Output {
    Written(Vec<u32>),
    Hashed {
        index: u32,
        verified: bool,
    },
    Read {
        index: u32,
        offset: u32,
        length: u32,
        block: Option<SharedBlock>,
    },
    Flushed,
    Error(io::Error),
}
#[derive(Default)]
struct Jobs {
    inline: Option<JoinHandle<io::Result<()>>>,
    fingerprint: Option<JoinHandle<io::Result<Vec<u8>>>>,
    changed: Option<JoinHandle<io::Result<super::PieceSet>>>,
    recovery: JoinSet<(u32, io::Result<Option<bool>>)>,
    hashes: JoinSet<io::Result<(u32, bool)>>,
    reads: JoinSet<ReadResult>,
}
impl Jobs {
    async fn drain(&mut self) -> io::Result<()> {
        let mut result = crate::engine::join_blocking(&mut self.inline).await;
        result = result.and(crate::engine::join_blocking(&mut self.fingerprint).await);
        result = result.and(crate::engine::join_blocking(&mut self.changed).await);
        while let Some(job) = self.recovery.join_next().await {
            result = result.and(job.map_err(io::Error::other).and_then(
                |(_, passed)| match passed {
                    Err(e)
                        if !matches!(
                            e.kind(),
                            io::ErrorKind::UnexpectedEof | io::ErrorKind::NotFound
                        ) =>
                    {
                        Err(e)
                    }
                    _ => Ok(()),
                },
            ));
        }
        while let Some(job) = self.hashes.join_next().await {
            result = result.and(job.map_err(io::Error::other).and_then(|r| r).map(|_| ()));
        }
        while let Some(job) = self.reads.join_next().await {
            result = result.and(
                job.map_err(io::Error::other)
                    .and_then(|read| read.bytes.map(|_| ())),
            );
        }
        result
    }
}
pub(in crate::engine) struct Writer {
    commands: Option<mpsc::Sender<Command>>,
    outputs: mpsc::Receiver<Output>,
    ready: Option<oneshot::Receiver<io::Result<Vec<u8>>>>,
    task: Option<JoinHandle<io::Result<()>>>,
    error: Option<Arc<io::Error>>,
    jobs: Arc<Mutex<Jobs>>,
    pending: usize,
    epochs: HashMap<u32, Arc<()>>,
    cancel_recovery: Arc<AtomicBool>,
    capacity_changed: Arc<Notify>,
}
impl Writer {
    pub(in crate::engine) fn spawn(
        storage: FileStorage,
        meta: OwnedMetainfo,
        profile: Profile,
        recheck: bool,
        initial_resume: Option<Vec<u8>>,
        checkpoint: crate::engine::torrent::Checkpoint,
        rechecks: Arc<Semaphore>,
    ) -> Self {
        let capacity = profile.write_queue_bytes / crate::engine::profile::BLOCK_LEN as usize;
        let (commands, mut incoming) = mpsc::channel(capacity);
        let (results, outputs) = mpsc::channel(8);
        let (ready_tx, ready) = oneshot::channel();
        let view = CacheView::default();
        let cancel_recovery = Arc::new(AtomicBool::new(false));
        let canceled = cancel_recovery.clone();
        let capacity_changed = Arc::new(Notify::new());
        let changed = capacity_changed.clone();
        // Immutable descriptors are shared only with owned blocking jobs. The
        // writer awaits each mutation before starting the next command batch.
        let storage = Arc::new(storage);
        let checkpoint: Arc<dyn Fn(Vec<u8>) -> io::Result<()> + Send + Sync> = checkpoint.into();
        let jobs = Arc::new(Mutex::new(Jobs::default()));
        let worker_jobs = jobs.clone();
        let task = tokio::spawn(async move {
            // The handle retains these jobs if this worker unwinds.
            let mut jobs = worker_jobs.lock().await;
            let mut result: io::Result<()> = async {
                let Some((mut verified, mut dirty)) = recover(
                    &storage, &meta, recheck, initial_resume, &canceled, &rechecks, &mut jobs,
                ).await? else {
                    // Do not checkpoint a partial scan: unchecked candidates must
                    // remain subject to the previous fingerprint on the next run.
                    return Ok(());
                };
                let Jobs { inline, fingerprint, hashes, reads, .. } = &mut *jobs;
                let mut live = live::Live::new(&meta);
                let mut files = FileProgress::new(&meta, &verified);
                for index in files.complete() {
                    if canceled.load(Ordering::Relaxed) { return Ok(()); }
                    let storage = storage.clone();
                    crate::engine::run_blocking(inline, move || storage.finish_file(index)).await?;
                }
                let _ = ready_tx.send(Ok(verified.clone()));
                let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + crate::engine::profile::RESUME_INTERVAL, crate::engine::profile::RESUME_INTERVAL);
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut closing = false;
                let mut pending_hash = None;
                let mut pending_read = None;
                let mut pending_command = None;
                loop {
                    if closing && hashes.is_empty() && reads.is_empty() { break; }
                    let command = tokio::select! {
                        biased;
                        result = hashes.join_next(), if !hashes.is_empty() => {
                            let (index, passed) = result.expect("hash job").map_err(io::Error::other)??;
                            Command::Hashed(index, passed)
                        },
                        result = reads.join_next(), if !reads.is_empty() => {
                            Command::ReadDone(result.expect("read job").map_err(io::Error::other)?)
                        },
                        _ = timer.tick() => {
                            if dirty { save(&storage, &meta, &verified, &checkpoint, fingerprint, inline).await?; dirty = false; }
                            continue;
                        }
                        () = async {}, if pending_hash.is_some() && hashes.len() < 2 => {
                            let (index, epoch) = pending_hash.take().expect("pending hash");
                            Command::Hash(index, epoch)
                        }
                        () = async {}, if pending_read.is_some() && reads.len() < MAX_UPLOAD_READS => {
                            let (index, offset, length) = pending_read.take().expect("pending read");
                            Command::Read(index, offset, length)
                        }
                        () = async {}, if pending_command.is_some() && pending_hash.is_none() && pending_read.is_none() => {
                            pending_command.take().expect("batched command")
                        }
                        command = incoming.recv(), if !closing && pending_hash.is_none() && pending_read.is_none() => match command {
                            Some(command) => { changed.notify_one(); command },
                            None => { closing = true; continue; }
                        },
                    };
                    live.prune();
                    let output = match command {
                        Command::Write(index, offset, epoch, bytes) => {
                            let mut blocks = vec![(index, offset, epoch, bytes)];
                            while blocks.len() < capacity.min(64) {
                                match incoming.try_recv() {
                                    Ok(Command::Write(index, offset, epoch, bytes)) => blocks.push((index, offset, epoch, bytes)),
                                    Ok(command) => { pending_command = Some(command); break; }
                                    Err(_) => break,
                                }
                            }
                            changed.notify_one();
                            let writes = blocks.iter().map(|(index, offset, _, bytes)| (*index, *offset, bytes.clone())).collect();
                            let destination = storage.clone();
                            crate::engine::run_blocking(inline, move || destination.write_batch(writes)).await?;
                            // Retain only successfully written blocks; Hash is a
                            // FIFO barrier behind all writes of this attempt.
                            let mut written = Vec::with_capacity(blocks.len());
                            for (index, offset, epoch, bytes) in blocks {
                                written.push(index);
                                live.record(&meta, index, offset, &epoch, bytes);
                            }
                            dirty = true;
                            Some(Output::Written(written))
                        }
                        Command::Hash(index, epoch) if hashes.len() == 2 => {
                            // Preserve FIFO without buffering a third hash. Writes
                            // ahead of this command can run with both hash jobs.
                            pending_hash = Some((index, epoch));
                            None
                        }
                        Command::Hash(index, epoch) => {
                            let range = meta.layout().piece_range(index)
                                .ok_or_else(|| io::Error::other("invalid hash piece"))?;
                            let len = (range.end - range.start) as usize;
                            let expected = *meta.piece_hash(index).expect("piece hash");
                            if let Some(buffered) = live.take(&meta, index, &epoch) {
                                hashes.spawn_blocking(move || Ok::<_, io::Error>((index, buffered.digest()[..] == expected)));
                            } else if len <= MAX_BUFFERED_HASH_BYTES {
                                live.discard(index);
                                let source = storage.clone();
                                hashes.spawn_blocking(move || {
                                    let bytes = source.read(index, 0, len)?;
                                    Ok((index, Sha1::digest(&bytes)[..] == expected))
                                });
                            } else {
                                live.discard(index);
                                let source = storage.clone();
                                let metainfo = meta.clone();
                                hashes.spawn_blocking(move || {
                                    let hash = fold_piece(&source, &metainfo, index, crate::engine::profile::HASH_CHUNK_LEN, None)?
                                        .expect("joined download hash");
                                    Ok((index, hash.finalize()[..] == expected))
                                });
                            }
                            None
                        }
                        Command::Hashed(index, passed) => {
                            live.complete(index);
                            if passed && !has(&verified, index) {
                                set(&mut verified, index, true);
                                for file in files.changed(&meta, index) {
                                    let storage = storage.clone();
                                    crate::engine::run_blocking(inline, move || storage.finish_file(file)).await?;
                                }
                            }
                            dirty = true;
                            Some(Output::Hashed { index, verified: passed })
                        }
                        Command::Read(index, offset, length) if reads.len() == MAX_UPLOAD_READS => {
                            pending_read = Some((index, offset, length));
                            None
                        }
                        Command::Read(index, offset, length) => {
                            let window = meta.layout().piece_range(index).and_then(|range| {
                                cache::window((range.end - range.start) as usize, profile.cache_bytes, offset, length)
                            }).filter(|_| has(&verified, index));
                            let available = window.is_some();
                            let block = view.block(index, offset, length)?;
                            if let Some(window) = window {
                                if block.is_none() {
                                    let storage = storage.clone();
                                    reads.spawn_blocking(move || {
                                        let bytes = storage.read(index, window.start as u32, window.len());
                                        ReadResult { index, offset, length, start: window.start as u32, bytes }
                                    });
                                    continue;
                                }
                            } else {
                                // Availability may change while uploads are queued.
                                // Reject this load, not the whole storage owner.
                                tracing::debug!(index, offset, length, "upload_read_rejected");
                            }
                            Some(Output::Read { index, offset, length, block: block.filter(|_| available).map(|b| SharedBlock::from_shared(b.bytes, b.range)) })
                        }
                        Command::ReadDone(ReadResult { index, offset, length, start, bytes }) => {
                            view.insert(index, start, bytes?, profile.cache_bytes)?;
                            // Own the result before another load can evict its window.
                            let block = view.block(index, offset, length)?.expect("loaded upload window");
                            Some(Output::Read { index, offset, length, block: Some(SharedBlock::from_shared(block.bytes, block.range)) })
                        }
                        Command::Flush => {
                            // The core sends this once at wanted completion, after
                            // all required write/hash results. Publish completion
                            // only after payload data is durable and resume is saved.
                            let destination = storage.clone();
                            crate::engine::run_blocking(inline, move || destination.sync_data()).await?;
                            save(&storage, &meta, &verified, &checkpoint, fingerprint, inline).await?;
                            dirty = false;
                            Some(Output::Flushed)
                        }
                    };
                    if let Some(output) = output {
                        let _ = results.send(output).await;
                    }
                }
                save(&storage, &meta, &verified, &checkpoint, fingerprint, inline).await?;
                Ok(())
            }
            .await;
            result = result.and(jobs.drain().await);
            changed.notify_one();
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    let error = Arc::new(e);
                    let _ = results
                        .send(Output::Error(io::Error::new(error.kind(), error.clone())))
                        .await;
                    Err(io::Error::new(error.kind(), error))
                }
            }
        });
        Self {
            commands: Some(commands),
            outputs,
            ready: Some(ready),
            task: Some(task),
            error: None,
            jobs,
            pending: 0,
            epochs: HashMap::new(),
            cancel_recovery,
            capacity_changed,
        }
    }
    pub(in crate::engine) async fn ready(&mut self) -> io::Result<Vec<u8>> {
        match self.ready.as_mut().expect("one startup").await {
            Ok(result) => result,
            Err(_) => match self.next().await {
                Err(error) => Err(error),
                Ok(_) => Err(io::Error::other("storage startup failed")),
            },
        }
    }
    pub(in crate::engine) fn capacity(&self) -> usize {
        self.commands.as_ref().map_or(0, mpsc::Sender::capacity)
    }
    pub(in crate::engine) fn healthy(&self) -> bool {
        self.commands
            .as_ref()
            .is_some_and(|commands| !commands.is_closed())
    }
    pub(in crate::engine) fn has_job(&self) -> bool {
        self.pending != 0
    }
    fn enqueue(&mut self, command: Command, response: bool) -> io::Result<()> {
        let sender = self
            .commands
            .as_ref()
            .ok_or_else(|| io::Error::other("writer stopped"))?;
        match sender.try_send(command) {
            Ok(()) => {
                self.pending += usize::from(response);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(io::Error::other("writer failed")),
            Err(mpsc::error::TrySendError::Full(_)) => {
                unreachable!("core writer queue reservation");
            }
        }
    }
    pub(in crate::engine) fn write_shared(
        &mut self,
        index: u32,
        offset: u32,
        bytes: SharedBlock,
    ) -> io::Result<()> {
        let epoch = self
            .epochs
            .entry(index)
            .or_insert_with(|| Arc::new(()))
            .clone();
        self.enqueue(Command::Write(index, offset, epoch, bytes), true)
    }
    pub(in crate::engine) fn discard(&mut self, index: u32) {
        self.epochs.remove(&index);
    }
    pub(in crate::engine) fn hash(&mut self, index: u32) -> io::Result<()> {
        let epoch = self
            .epochs
            .entry(index)
            .or_insert_with(|| Arc::new(()))
            .clone();
        self.enqueue(Command::Hash(index, epoch), true)
    }
    pub(in crate::engine) fn read(
        &mut self,
        index: u32,
        offset: u32,
        length: u32,
    ) -> io::Result<()> {
        self.enqueue(Command::Read(index, offset, length), true)
    }
    pub(in crate::engine) fn flush(&mut self) -> io::Result<()> {
        self.enqueue(Command::Flush, true)
    }
    pub(in crate::engine) fn capacity_changed(&self) -> Arc<Notify> {
        self.capacity_changed.clone()
    }
    pub(in crate::engine) async fn next(&mut self) -> io::Result<Output> {
        let output = self
            .outputs
            .recv()
            .await
            .ok_or_else(|| io::Error::other("writer output closed"))?;
        if let Output::Error(e) = output {
            return Err(e);
        }
        if let Output::Hashed { index, .. } = &output {
            self.epochs.remove(index);
        }
        let completed = match &output {
            Output::Written(blocks) => blocks.len(),
            _ => 1,
        };
        assert!(self.pending >= completed, "writer response count");
        self.pending -= completed;
        Ok(output)
    }
    pub(in crate::engine) async fn shutdown(&mut self) -> io::Result<()> {
        self.cancel_recovery.store(true, Ordering::Relaxed);
        self.commands.take();
        self.outputs.close();
        if let Some(task) = &mut self.task {
            let result = task.await.map_err(io::Error::other).and_then(|r| r);
            self.task = None;
            self.error = result.err().map(Arc::new);
        }
        let drained = self.jobs.lock().await.drain().await;
        if self.error.is_none() {
            self.error = drained.err().map(Arc::new);
        }
        self.error
            .as_ref()
            .map_or(Ok(()), |e| Err(io::Error::new(e.kind(), e.clone())))
    }
}
fn has(bits: &[u8], index: u32) -> bool {
    bits[index as usize / 8] & (0x80 >> (index % 8)) != 0
}
fn set(bits: &mut [u8], index: u32, value: bool) {
    let mask = 0x80 >> (index % 8);
    if value {
        bits[index as usize / 8] |= mask;
    } else {
        bits[index as usize / 8] &= !mask;
    }
}
async fn save(
    storage: &Arc<FileStorage>,
    meta: &OwnedMetainfo,
    verified: &[u8],
    checkpoint: &Arc<dyn Fn(Vec<u8>) -> io::Result<()> + Send + Sync>,
    job: &mut Option<JoinHandle<io::Result<Vec<u8>>>>,
    callback: &mut Option<JoinHandle<io::Result<()>>>,
) -> io::Result<()> {
    let storage = storage.clone();
    let fingerprint = crate::engine::run_blocking(job, move || storage.fingerprint()).await?;
    let resume = resume::encode(meta, verified, &fingerprint)?;
    let checkpoint = checkpoint.clone();
    crate::engine::run_blocking(callback, move || checkpoint(resume)).await?;
    tracing::debug!("resume_saved");
    Ok(())
}
async fn recover(
    storage: &Arc<FileStorage>,
    meta: &OwnedMetainfo,
    recheck: bool,
    initial_resume: Option<Vec<u8>>,
    canceled: &Arc<AtomicBool>,
    rechecks: &Arc<Semaphore>,
    jobs: &mut Jobs,
) -> io::Result<Option<(Vec<u8>, bool)>> {
    let Jobs {
        changed: changed_job,
        recovery: jobs,
        ..
    } = jobs;
    if canceled.load(Ordering::Relaxed) {
        return Ok(None);
    }
    let count = meta.layout().piece_count();
    let saved = if recheck { None } else { initial_resume }.and_then(|blob| {
        match resume::decode(meta, &blob) {
            Ok(record) => Some(record),
            Err(_) => {
                tracing::warn!("resume_rejected");
                None
            }
        }
    });
    let (mut verified, candidates) = if let Some(saved) = saved {
        let storage = storage.clone();
        let changed = crate::engine::run_blocking(changed_job, move || {
            storage.changed_since(&saved.fingerprint)
        })
        .await?;
        (saved.verified, changed)
    } else {
        (vec![0; count.div_ceil(8) as usize], (0..count).collect())
    };
    for &index in &candidates {
        set(&mut verified, index, false);
    }
    let trusted: u32 = verified.iter().map(|b| b.count_ones()).sum();
    // Acquire session-wide capacity before spawning, never by blocking a pool
    // thread. Each job holds its permit through the entire piece and checks
    // cancellation between bounded reads. Also bound this torrent's job set.
    let result: io::Result<Option<()>> = async {
        let mut pending = candidates.iter().copied();
        loop {
            while jobs.len() < RECHECK_CONCURRENCY {
                if canceled.load(Ordering::Relaxed) {
                    return Ok(None);
                }
                let Some(index) = pending.next() else { break };
                let permit = rechecks
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(io::Error::other)?;
                if canceled.load(Ordering::Relaxed) {
                    return Ok(None);
                }
                let storage = storage.clone();
                let meta = meta.clone();
                let canceled = canceled.clone();
                jobs.spawn_blocking(move || {
                    let _permit = permit;
                    let passed = fold_piece(
                        &storage,
                        &meta,
                        index,
                        crate::engine::profile::HASH_CHUNK_LEN,
                        Some(&canceled),
                    )
                    .map(|hash| {
                        hash.map(|hash| {
                            hash.finalize()[..] == meta.piece_hash(index).expect("piece hash")[..]
                        })
                    });
                    (index, passed)
                });
            }
            let Some(result) = jobs.join_next().await else {
                return Ok(Some(()));
            };
            match result.map_err(io::Error::other)? {
                (index, Ok(Some(passed))) => set(&mut verified, index, passed),
                (_, Ok(None)) => return Ok(None),
                (_, Err(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::NotFound
                    ) => {}
                (_, Err(e)) => return Err(e),
            }
        }
    }
    .await;
    if result.is_err() {
        canceled.store(true, Ordering::Relaxed);
    }
    if result?.is_none() || canceled.load(Ordering::Relaxed) {
        return Ok(None);
    }
    tracing::info!(
        rechecked_pieces = candidates.len(),
        trusted_pieces = trusted,
        "storage_recovered"
    );
    Ok(Some((verified, !candidates.is_empty())))
}
fn fold_piece(
    storage: &FileStorage,
    meta: &OwnedMetainfo,
    index: u32,
    chunk_len: usize,
    canceled: Option<&AtomicBool>,
) -> io::Result<Option<Sha1>> {
    let range = meta
        .layout()
        .piece_range(index)
        .ok_or_else(|| io::Error::other("invalid hash piece"))?;
    let len = (range.end - range.start) as usize;
    let mut hash = Sha1::new();
    let mut offset = 0;
    while offset < len {
        if canceled.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
            return Ok(None);
        }
        let count = chunk_len.min(len - offset);
        let bytes = storage.read(index, offset as u32, count)?;
        hash.update(&bytes);
        offset += count;
    }
    Ok(Some(hash))
}
