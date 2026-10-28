use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stall {
    NoProgress,
    NoPeers,
    AwaitingPeers,
}

#[derive(Debug)]
pub(crate) enum Event {
    Stalled(Stall),
    PeerReady(ConnectionId),
    Block {
        index: u32,
        begin: u32,
        length: u32,
    },
    IgnoredBlock {
        index: u32,
        begin: u32,
    },
    Verified(u32),
    PieceRetry {
        index: u32,
        retry: u32,
    },
    PeerClosed {
        connection: ConnectionId,
        result: io::Result<()>,
    },
    Complete,
}
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Progress {
    pub(super) total_bytes: u64,
    pub(super) initial_verified_bytes: u64,
    pub(super) verified_bytes: u64,
    pub(super) wanted_bytes: u64,
    pub(super) wanted_verified_bytes: u64,
    pub(super) received_payload_bytes: u64,
}
impl Progress {
    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
    pub(crate) fn verified_bytes(&self) -> u64 {
        self.verified_bytes
    }
    pub(crate) fn wanted_bytes(&self) -> u64 {
        self.wanted_bytes
    }
    pub(crate) fn wanted_verified_bytes(&self) -> u64 {
        self.wanted_verified_bytes
    }
    pub(crate) fn remaining_bytes(&self) -> u64 {
        self.total_bytes - self.verified_bytes
    }
    pub(crate) fn received_payload_bytes(&self) -> u64 {
        self.received_payload_bytes
    }
}
impl fmt::Debug for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Progress")
            .field("total_bytes", &self.total_bytes)
            .field("initial_verified_bytes", &self.initial_verified_bytes)
            .field("verified_bytes", &self.verified_bytes)
            .field("received_payload_bytes", &self.received_payload_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Diagnostics {
    pub(crate) peers: usize,
    pub(crate) established_peers: usize,
    pub(crate) unchoked_peers: usize,
    pub(crate) interested_peers: usize,
    pub(crate) remote_interested_peers: usize,
    pub(crate) retiring_peers: usize,
    pub(crate) output_blocked_peers: usize,
    pub(crate) upload_unchoked_peers: usize,
    pub(crate) pending_upload_requests: usize,
    pub(crate) outstanding_requests: usize,
    pub(crate) assigned_requests: usize,
    pub(crate) waiting_requests: usize,
    pub(crate) requests_at_retry_limit: usize,
    pub(crate) current_piece: Option<u32>,
    pub(crate) piece_retries_remaining: Option<u32>,
    pub(crate) hashing: bool,
    pub(crate) stalled: Option<Stall>,
    pub(crate) failed: bool,
}
