//! Per-torrent resource settings, separate from fixed protocol policy.
mod admission;
use std::{error::Error, fmt, io, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionLimit {
    Pieces,
    PieceBytes,
    Files,
}
impl fmt::Display for AdmissionLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pieces => "too many pieces",
            Self::PieceBytes => "piece size is too large",
            Self::Files => "too many files",
        })
    }
}
#[derive(Debug)]
pub enum AdmissionError {
    Metainfo(crate::protocol::metainfo::Error),
    ResourceLimit(AdmissionLimit),
}
impl AdmissionError {
    pub(in crate::engine) fn of(error: &io::Error) -> Option<&Self> {
        error.get_ref().and_then(|e| e.downcast_ref::<Self>())
    }
}
impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metainfo(_) => f.write_str("invalid or unsupported v1 metainfo"),
            Self::ResourceLimit(limit) => write!(f, "unsupported torrent: {limit}"),
        }
    }
}
impl Error for AdmissionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Metainfo(error) => Some(error),
            _ => None,
        }
    }
}
impl From<AdmissionError> for io::Error {
    fn from(error: AdmissionError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Profile {
    /// Per-torrent connection cap, including connecting and not-yet-joined peers.
    pub peers: usize,
    pub cache_bytes: usize,
    pub write_queue_bytes: usize,
}
impl Default for Profile {
    fn default() -> Self {
        Self::DEFAULT
    }
}
impl Profile {
    pub fn validate(&self) -> io::Result<()> {
        let buffer_range = BLOCK_LEN as usize..=1024 * 1024 * 1024;
        if !(1..=4096).contains(&self.peers)
            || !buffer_range.contains(&self.cache_bytes)
            || !buffer_range.contains(&self.write_queue_bytes)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid torrent resource limits",
            ));
        }
        Ok(())
    }

    pub const DEFAULT: Self = Self {
        peers: 50,
        cache_bytes: 16 * 1024 * 1024,
        write_queue_bytes: 16 * 1024 * 1024,
    };
}

pub const MAX_META: usize = 16 * 1024 * 1024;
pub(in crate::engine) const MAX_PIECES: u32 = 1 << 20;
pub const BLOCK_LEN: u32 = 16 * 1024;
pub(in crate::engine) const MAX_REQUEST_QUEUE: usize = 32;
pub(in crate::engine) const MAX_PIECE_RETRIES: u32 = 2;
// A whole maximum-size bitfield must fit in one frame. Frames allocate only
// their actual lengths; block/layout validation still bounds Piece messages.
pub(in crate::engine) const MAX_FRAME_LENGTH: u32 = MAX_PIECES / 8 + 1;
pub(in crate::engine) const MAX_PIECE_LEN: u32 = 512 * 1024 * 1024;
pub(in crate::engine) const MAX_FILES: usize = crate::protocol::resume::MAX_FILES;
pub(in crate::engine) const RESUME_INTERVAL: Duration = Duration::from_secs(120);
pub(in crate::engine) const MAX_REASSIGNMENTS: u32 = 2;
pub(in crate::engine) const PEER_IGNORED_FRAMES: u64 = 1024;
pub(in crate::engine) const PEER_IGNORED_BYTES: u64 = 4 * 1024 * 1024;
pub(in crate::engine) const OUTBOUND_QUEUE: usize = 64;
pub(in crate::engine) const EVENT_QUEUE: usize = 64;
pub(in crate::engine) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
pub(in crate::engine) const IDLE_TIMEOUT: Duration = Duration::from_secs(180);
pub(in crate::engine) const FRAME_TIMEOUT: Duration = Duration::from_secs(30);
pub(in crate::engine) const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
pub(in crate::engine) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
pub(in crate::engine) const MAX_URL_BYTES: usize = 2048;
pub(in crate::engine) const MAX_TOTAL_URL_BYTES: usize = 16384;
pub(in crate::engine) const TOTAL_TIMEOUT: Duration = Duration::from_secs(45);
pub(in crate::engine) const MAX_QUEUED: usize = 128;
pub(in crate::engine) const MAX_UPLOAD_REQUESTS: usize = 4096;
pub(in crate::engine) const MAX_UPLOAD_READS: usize = 4;
pub(in crate::engine) const UPLOAD_SLOTS: usize = 8;
pub(in crate::engine) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(in crate::engine) const HASH_CHUNK_LEN: usize = 65536;
pub(in crate::engine) const DNS_TIMEOUT: Duration = Duration::from_secs(5);
pub(in crate::engine) const MAX_DNS_ANSWERS: usize = 16;
pub(in crate::engine) const MAX_WEB_SEEDS: usize = 32;
pub(in crate::engine) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(in crate::engine) const STALL_RETRY_INITIAL: Duration = Duration::from_secs(1);
pub(in crate::engine) const STALL_RETRY_MAX: Duration = Duration::from_secs(30);
