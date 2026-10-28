//! Per-connection wire state, owned and mutated by the transfer coordinator.
//! Transport tasks remain in peer_io; this value has no runtime or lifecycle.

use super::{ConnectionId, UnsolicitedBudget};
use crate::core::requests;
use protocol::peer::message::BlockRequest;
use std::time::Instant;
use std::{collections::VecDeque, io};

pub(crate) const MAX_REQUESTS: usize = 500;

/// Three seconds of useful payload, bounded independently for each connection.
pub(crate) fn request_window(rate: u64, block_len: u32, reqq: u32) -> usize {
    (rate.saturating_mul(3) / u64::from(block_len))
        .clamp(4, MAX_REQUESTS as u64)
        .min(u64::from(reqq)) as usize
}

#[derive(Default)]
pub(crate) struct DownloadRate {
    since: Option<Instant>,
    bytes: u64,
    total: u64,
    rate: u64,
}
impl DownloadRate {
    pub(crate) fn start(&mut self, now: Instant) {
        self.since.get_or_insert(now);
    }
    pub(crate) fn received(&mut self, now: Instant, bytes: usize) {
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
    pub(crate) fn total(&self) -> u64 {
        self.total
    }
    pub(crate) fn window(&self, block_len: u32, reqq: u32) -> usize {
        request_window(self.rate, block_len, reqq)
    }
}

pub(crate) struct PeerState {
    pub(crate) connection: ConnectionId,
    pub(crate) peer_id: Option<[u8; 20]>,
    pub(crate) established: bool,
    pub(crate) last_useful: Instant,
    pub(crate) last_uploaded: u64,
    pub(crate) uninteresting_since: Option<Instant>,
    pub(crate) extensions: crate::core::extensions::Extensions,
    pub(crate) first_message: bool,
    pub(crate) choked: bool,
    pub(crate) fast: bool,
    pub(crate) remote_dht: bool,
    pub(crate) send_dht_port: bool,
    pub(crate) dht_port_seen: bool,
    pub(crate) received_dht_port: Option<u16>,
    pub(crate) allowed_fast: Vec<u8>,
    pub(crate) interested: bool,
    pub(crate) remote_interested: bool,
    pub(crate) upload_choked: bool,
    pub(crate) upload_granted: bool,
    pub(crate) uploads: requests::Uploads,
    pub(crate) dirty: bool,
    pub(crate) cancels: VecDeque<BlockRequest>,
    pub(crate) rejects: VecDeque<BlockRequest>,
    pub(crate) send_bitfield: bool,
    pub(crate) output_blocked: bool,
    pub(crate) retiring: bool,
    pub(crate) retirement_reason: Option<io::Error>,
    /// Any request admitted for the current piece attempt, not a blame record.
    pub(crate) pieces: Vec<u8>,
    pub(crate) have_count: u32,
    pub(crate) announced: Vec<u8>,
    /// Lowest advertised unverified piece, independent of choke/output state.
    pub(crate) first_needed: Option<u32>,
    /// Lowest verified piece not yet announced; no per-piece HAVE queue.
    pub(crate) pending_have: Option<u32>,
    /// Authorization for still-unfulfilled blocks of this piece attempt. Bounded
    /// by MAX_REQUESTS; retained across Choke, cleared on fulfillment/restart.
    pub(crate) asked: requests::Asked,
    pub(crate) download_rate: DownloadRate,
    pub(crate) uploaded: u64,
    pub(crate) output_capacity: usize,
    pub(crate) web_seed: bool,
    pub(crate) unsolicited: UnsolicitedBudget,
}
