//! Bounded latest observations. Rates/totals belong to the current owner, not
//! durable torrent history. No observer drives network work.
use std::{net::SocketAddr, time::Instant};

/// Latest storage-worker activity, not a timeout or proof of an I/O hang.
#[derive(Clone, Copy, Debug)]
pub struct StorageStatus {
    /// Worker stage, including blocking-pool queue and persistence waits.
    pub operation: &'static str,
    /// Start of the unchanged stage label, not an individual job's start time.
    pub since: Instant,
    /// Submitted payload hash jobs, including finished but uncollected results.
    pub hashes_in_flight: usize,
    /// Submitted payload block/leaf reads; excludes startup recovery jobs.
    pub reads_in_flight: usize,
}

/// Cumulative coordination outcomes for one torrent execution.
#[derive(Clone, Copy, Debug, Default)]
pub struct HolepunchStatus {
    /// Queued rendezvous messages, not confirmed wire sends.
    pub rendezvous_started: u64,
    /// Parsed BEP 55 error frames, including unsolicited errors.
    pub remote_errors: u64,
    pub rendezvous_expired: u64,
    /// Created/upgraded dial leases, excluding duplicates.
    pub punches_queued: u64,
    /// Leases expired before admission, including restored ordinary contacts.
    pub punches_expired: u64,
}

#[derive(Clone, Debug)]
pub struct PeerStatus {
    pub connection: u64,
    pub endpoint: Option<SocketAddr>,
    pub client: Option<String>,
    pub direction: &'static str,
    pub transport: &'static str,
    /// Peer application-stream encoding: `plaintext` or `rc4`. None for web seeds
    /// or when no live negotiated stream is available; not HTTP TLS status.
    pub encryption: Option<&'static str>,
    pub utp_retransmits: Option<u64>,
    pub source: &'static str,
    /// Present only for HTTP payload connections.
    pub web_seed_url: Option<String>,
    pub download_bytes_per_second: f64,
    pub upload_bytes_per_second: f64,
    pub detail: PeerDiagnostics,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerDiagnostics {
    pub downloaded_bytes: u64,
    pub uploaded_bytes: Option<u64>,
    pub connection: u64,
    pub established: bool,
    pub fast_extension: bool,
    pub remote_choking: bool,
    pub local_interested: bool,
    pub remote_interested: bool,
    pub local_choking: bool,
    pub upload_slot: bool,
    pub pending_upload_requests: usize,
    pub has_needed_pieces: bool,
    pub retiring: bool,
    pub output_blocked: bool,
    pub assigned_requests: usize,
    pub authorized_requests: usize,
    pub request_queue: u32,
    pub ignored_frames_remaining: u64,
    pub ignored_bytes_remaining: u64,
}

#[derive(Clone, Debug)]
pub struct TrackerStatus {
    /// Twenty-byte swarm key (SHA-1 or the SHA-256 prefix).
    pub swarm: [u8; 20],
    pub url: String,
    /// Zero-based original metainfo tier/slot, not scheduler preference order.
    pub tier: usize,
    pub slot: usize,
    pub state: &'static str,
    /// Announce attempts (UDP connect plus announce count as one attempt).
    pub exchanges: u32,
    pub failures: u32,
    pub last_reply_ago_ms: Option<u64>,
    /// Time until the next sequential announce round; later trackers are fallbacks.
    pub next_announce_ms: Option<u64>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    pub peers_returned: Option<usize>,
    /// Tracker failure reason or local exchange error.
    pub error: Option<String>,
    pub warning: Option<String>,
}

#[derive(Clone, Debug)]
pub struct WebSeedStatus {
    pub url: String,
    pub state: &'static str,
    pub last_event_ago_ms: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DhtStatus {
    /// Twenty-byte swarm key (SHA-1 or the SHA-256 prefix).
    pub swarm: [u8; 20],
    pub lookup_pending: bool,
    pub next_lookup_ms: u64,
    pub last_lookup_ago_ms: Option<u64>,
    pub queried: usize,
    pub announced: usize,
    pub peers_returned: usize,
    pub error: Option<String>,
}
