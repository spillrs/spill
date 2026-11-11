use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::engine) enum Stall {
    NoProgress,
    NoPeers,
    AwaitingPeers,
}

#[derive(Debug)]
pub(in crate::engine) enum Event {
    Stalled(Stall),
    PeerReady(ConnectionId),
    Block {
        connection: ConnectionId,
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
        corrupt: bool,
        source: Option<ConnectionId>,
    },
    PeerReplaced {
        connection: ConnectionId,
        replacement: ConnectionId,
        rank: u32,
        replacement_rank: u32,
    },
    PieceWithdrawn {
        connection: ConnectionId,
        index: u32,
    },
    PeerClosed {
        connection: ConnectionId,
        result: io::Result<()>,
    },
    Complete,
}
#[derive(Clone, PartialEq, Eq)]
pub(in crate::engine) struct Progress {
    pub(in crate::engine) total_bytes: u64,
    pub(in crate::engine) initial_verified_bytes: u64,
    pub(in crate::engine) verified_bytes: u64,
    pub(in crate::engine) wanted_bytes: u64,
    pub(in crate::engine) wanted_verified_bytes: u64,
    pub(in crate::engine) received_payload_bytes: u64,
    pub(in crate::engine) hash_failed_bytes: u64,
    pub(in crate::engine) hash_failed_pieces: u64,
}
impl Progress {
    pub(in crate::engine) fn remaining_bytes(&self) -> u64 {
        self.total_bytes - self.verified_bytes
    }
    pub(in crate::engine) fn partial_seed(&self) -> bool {
        self.wanted_verified_bytes == self.wanted_bytes && self.verified_bytes != self.total_bytes
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
