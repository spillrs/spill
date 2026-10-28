//! Fixed node-wide budgets, independent of any torrent.
use std::time::Duration;
#[derive(Clone, Copy, Debug)]
pub(super) struct Limits {
    pub(in crate::engine) max_buckets: usize,
    pub(in crate::engine) max_pending: usize,
    pub(in crate::engine) max_lookups: usize,
    pub(in crate::engine) max_lookup_queries: usize,
    pub(in crate::engine) command_queue: usize,
    pub(in crate::engine) max_torrents: usize,
    pub(in crate::engine) max_peers_per_torrent: usize,
    pub(in crate::engine) max_peers_reply: usize,
    pub(in crate::engine) max_rate_sources: usize,
    pub(in crate::engine) queries_per_second: usize,
    pub(in crate::engine) response_bytes_per_second: usize,
    pub(in crate::engine) query_timeout: Duration,
}
impl Limits {
    pub(in crate::engine) const DEFAULT: Self = Self {
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
