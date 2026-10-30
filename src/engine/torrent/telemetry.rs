//! Bounded latest observations. Rates/totals belong to the current owner, not
//! durable torrent history. No observer drives network work.
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct PeerStatus {
    pub connection: u64,
    pub endpoint: Option<SocketAddr>,
    pub client: Option<String>,
    pub direction: &'static str,
    pub transport: &'static str,
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
    pub lookup_pending: bool,
    pub next_lookup_ms: u64,
    pub last_lookup_ago_ms: Option<u64>,
    pub queried: usize,
    pub announced: usize,
    pub peers_returned: usize,
    pub error: Option<String>,
}
