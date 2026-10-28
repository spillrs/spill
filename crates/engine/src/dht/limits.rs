//! Fixed node-wide budgets, independent of any torrent.
use std::time::Duration;
#[derive(Clone, Copy, Debug)]
pub(super) struct Limits {
    pub(crate) max_buckets: usize,
    pub(crate) max_pending: usize,
    pub(crate) max_lookups: usize,
    pub(crate) max_lookup_queries: usize,
    pub(crate) command_queue: usize,
    pub(crate) max_torrents: usize,
    pub(crate) max_peers_per_torrent: usize,
    pub(crate) max_peers_reply: usize,
    pub(crate) max_rate_sources: usize,
    pub(crate) queries_per_second: usize,
    pub(crate) response_bytes_per_second: usize,
    pub(crate) query_timeout: Duration,
}
impl Limits {
    pub(crate) const DEFAULT: Self = Self {
        max_buckets: 161,
        max_pending: 64,
        max_lookups: 8,
        max_lookup_queries: 64,
        command_queue: 64,
        max_torrents: 2000,
        max_peers_per_torrent: 500,
        max_peers_reply: 100,
        max_rate_sources: 512,
        queries_per_second: 5,
        response_bytes_per_second: 8000,
        query_timeout: Duration::from_secs(5),
    };
}
