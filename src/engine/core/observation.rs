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
pub(in crate::engine) struct Progress {
    pub(super) total_bytes: u64,
    pub(super) initial_verified_bytes: u64,
    pub(super) verified_bytes: u64,
    pub(super) wanted_bytes: u64,
    pub(super) wanted_verified_bytes: u64,
    pub(super) received_payload_bytes: u64,
    pub(super) hash_failed_bytes: u64,
    pub(super) hash_failed_pieces: u64,
}
impl Progress {
    pub(in crate::engine) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
    pub(in crate::engine) fn verified_bytes(&self) -> u64 {
        self.verified_bytes
    }
    pub(in crate::engine) fn wanted_bytes(&self) -> u64 {
        self.wanted_bytes
    }
    pub(in crate::engine) fn wanted_verified_bytes(&self) -> u64 {
        self.wanted_verified_bytes
    }
    pub(in crate::engine) fn remaining_bytes(&self) -> u64 {
        self.total_bytes - self.verified_bytes
    }
    pub(in crate::engine) fn hash_failed_bytes(&self) -> u64 {
        self.hash_failed_bytes
    }
    pub(in crate::engine) fn hash_failed_pieces(&self) -> u64 {
        self.hash_failed_pieces
    }
    pub(in crate::engine) fn received_payload_bytes(&self) -> u64 {
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
