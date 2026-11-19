//! Session-wide file handles for verified, immutable seed payloads.
use super::paths::PayloadPaths;
use std::{
    collections::VecDeque,
    fs::{File, Metadata},
    io,
    os::unix::fs::{FileExt, MetadataExt},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

const SHARDS: usize = 16;
const FILES_PER_SHARD: usize = 4;

#[derive(Default)]
pub(in crate::engine) struct Pool {
    next_seed: AtomicU64,
    shards: [Mutex<VecDeque<Entry>>; SHARDS],
}
struct Entry {
    key: (u64, usize),
    file: File,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Identity {
    pub(super) fingerprint: [u8; 40],
    ctime: i64,
    ctime_nsec: i64,
}
impl Identity {
    pub(super) fn from_metadata(m: &Metadata) -> Self {
        let mut fingerprint = [0; 40];
        for (slot, value) in fingerprint.as_chunks_mut::<8>().0.iter_mut().zip([
            m.len().to_le_bytes(),
            m.mtime().to_le_bytes(),
            m.mtime_nsec().to_le_bytes(),
            m.dev().to_le_bytes(),
            m.ino().to_le_bytes(),
        ]) {
            *slot = value;
        }
        Self {
            fingerprint,
            ctime: m.ctime(),
            ctime_nsec: m.ctime_nsec(),
        }
    }
}

pub(super) struct SeedFiles {
    pool: Arc<Pool>,
    seed: u64,
    root: File,
    paths: PayloadPaths,
    pub(super) identities: Vec<Option<Identity>>,
}
impl SeedFiles {
    pub(super) fn new(
        pool: Arc<Pool>,
        root: File,
        paths: PayloadPaths,
        identities: Vec<Option<Identity>>,
    ) -> Self {
        let seed = pool.next_seed.fetch_add(1, Ordering::Relaxed);
        Self {
            pool,
            seed,
            root,
            paths,
            identities,
        }
    }
    pub(super) fn read(&self, index: usize, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        let Some(identity) = self.identities[index] else {
            return Ok(());
        };
        let key = (self.seed, index);
        let mut shard = self.pool.shards[(self.seed as usize ^ index) % SHARDS]
            .lock()
            .unwrap();
        let entry = if let Some(position) = shard.iter().position(|entry| entry.key == key) {
            shard.remove(position).expect("cached file")
        } else {
            // Evict before opening so reads cannot exceed the descriptor budget.
            if shard.len() == FILES_PER_SHARD {
                shard.pop_front();
            }
            let file = self.paths.open_seed_file(&self.root, index)?;
            super::lock(&file)?;
            if Identity::from_metadata(&file.metadata()?) != identity {
                return Err(io::Error::other("seed payload changed; recheck required"));
            }
            Entry { key, file }
        };
        // Keep the shard locked through pread so in-flight handles count too.
        let result = entry.file.read_exact_at(bytes, offset);
        shard.push_back(entry);
        result
    }
}
impl Drop for SeedFiles {
    fn drop(&mut self) {
        for shard in &self.pool.shards {
            shard
                .lock()
                .unwrap()
                .retain(|entry| entry.key.0 != self.seed);
        }
    }
}
