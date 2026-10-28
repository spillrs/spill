//! Per-connection wire state, owned and mutated by the transfer coordinator.
//! Transport tasks remain in peer_io; this value has no runtime or lifecycle.

use super::{ConnectionId, UnsolicitedBudget};
use crate::engine::core::requests;
use crate::protocol::peer::message::BlockRequest;
use std::time::Instant;
use std::{collections::VecDeque, io};

pub(in crate::engine) const MAX_REQUESTS: usize = 500;

/// Three seconds of useful payload, bounded independently for each connection.
pub(in crate::engine) fn request_window(rate: u64, block_len: u32, reqq: u32) -> usize {
    (rate.saturating_mul(3) / u64::from(block_len))
        .clamp(4, MAX_REQUESTS as u64)
        .min(u64::from(reqq)) as usize
}

#[derive(Default)]
pub(in crate::engine) struct DownloadRate {
    since: Option<Instant>,
    bytes: u64,
    total: u64,
    rate: u64,
}
impl DownloadRate {
    pub(in crate::engine) fn start(&mut self, now: Instant) {
        self.since.get_or_insert(now);
    }
    pub(in crate::engine) fn received(&mut self, now: Instant, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes as u64);
        self.total = self.total.saturating_add(bytes as u64);
        let since = self.since.get_or_insert(now);
        let elapsed = now.duration_since(*since).as_secs_f64();
        if elapsed >= 0.25 {
            self.rate = (self.bytes as f64 / elapsed) as u64;
            self.bytes = 0;
            *since = now;
        }
    }
    pub(in crate::engine) fn total(&self) -> u64 {
        self.total
    }
    pub(in crate::engine) fn window(&self, block_len: u32, reqq: u32) -> usize {
        request_window(self.rate, block_len, reqq)
    }
}

pub(in crate::engine) struct PeerState {
    pub(in crate::engine) connection: ConnectionId,
    pub(in crate::engine) peer_id: Option<[u8; 20]>,
    pub(in crate::engine) established: bool,
    pub(in crate::engine) last_useful: Instant,
    pub(in crate::engine) last_uploaded: u64,
    pub(in crate::engine) uninteresting_since: Option<Instant>,
    pub(in crate::engine) extensions: crate::engine::core::extensions::Extensions,
    pub(in crate::engine) first_message: bool,
    pub(in crate::engine) choked: bool,
    pub(in crate::engine) fast: bool,
    pub(in crate::engine) remote_dht: bool,
    pub(in crate::engine) send_dht_port: bool,
    pub(in crate::engine) dht_port_seen: bool,
    pub(in crate::engine) received_dht_port: Option<u16>,
    pub(in crate::engine) allowed_fast: Vec<u8>,
    pub(in crate::engine) interested: bool,
    pub(in crate::engine) remote_interested: bool,
    pub(in crate::engine) upload_choked: bool,
    pub(in crate::engine) upload_granted: bool,
    pub(in crate::engine) uploads: requests::Uploads,
    pub(in crate::engine) dirty: bool,
    pub(in crate::engine) cancels: VecDeque<BlockRequest>,
    pub(in crate::engine) rejects: VecDeque<BlockRequest>,
    pub(in crate::engine) send_bitfield: bool,
    pub(in crate::engine) output_blocked: bool,
    pub(in crate::engine) retiring: bool,
    pub(in crate::engine) retirement_reason: Option<io::Error>,
    /// Any request admitted for the current piece attempt, not a blame record.
    pub(in crate::engine) pieces: Vec<u8>,
    pub(in crate::engine) have_count: u32,
    pub(in crate::engine) announced: Vec<u8>,
    /// Lowest advertised unverified piece, independent of choke/output state.
    pub(in crate::engine) first_needed: Option<u32>,
    /// Lowest verified piece not yet announced; no per-piece HAVE queue.
    pub(in crate::engine) pending_have: Option<u32>,
    /// Authorization for still-unfulfilled blocks of this piece attempt. Bounded
    /// by MAX_REQUESTS; retained across Choke, cleared on fulfillment/restart.
    pub(in crate::engine) asked: requests::Asked,
    pub(in crate::engine) download_rate: DownloadRate,
    pub(in crate::engine) uploaded: u64,
    pub(in crate::engine) output_capacity: usize,
    pub(in crate::engine) web_seed: bool,
    pub(in crate::engine) unsolicited: UnsolicitedBudget,
}
