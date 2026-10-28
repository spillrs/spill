use std::io;

/// Handler policy for ignored traffic over one connection's lifetime, not a
/// transport rate limit. Both ceilings apply; zero tolerates no ignored frames.
/// Bytes count the entire encoded frame, including its four-byte prefix. Useful
/// traffic, elapsed time and torrent retry authorization never replenish this
/// allowance. Fresh explicitly attached connections get a fresh allowance.
/// Small ceilings can retire honest peers with benign duplicates/late cancels;
/// exhausting caller resource policy is not proof of malicious intent.
///
/// Handlers charge after decoding/classification. A crossing frame and bounded
/// transport read-ahead may already have been read/allocated; max_frame_length
/// and the existing transport queues still bound those buffers. This does not
/// limit accepted data/control traffic, OS buffers or repeated caller admissions.
pub(in crate::engine) struct UnsolicitedBudget {
    frames: u64,
    bytes: u64,
}
impl UnsolicitedBudget {
    pub(in crate::engine) fn new() -> Self {
        Self {
            frames: crate::engine::profile::PEER_IGNORED_FRAMES,
            bytes: crate::engine::profile::PEER_IGNORED_BYTES,
        }
    }
    pub(in crate::engine) fn remaining(&self) -> (u64, u64) {
        (self.frames, self.bytes)
    }
    pub(in crate::engine) fn charge(&mut self, wire_bytes: usize) -> io::Result<()> {
        let remaining = u64::try_from(wire_bytes)
            .ok()
            .and_then(|bytes| self.bytes.checked_sub(bytes))
            .zip(self.frames.checked_sub(1));
        let Some((bytes, frames)) = remaining else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsolicited peer traffic limit exceeded",
            ));
        };
        self.bytes = bytes;
        self.frames = frames;
        Ok(())
    }
}

/// Local connection identity, distinct from the twenty-byte wire peer ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
pub(in crate::engine) struct ConnectionId(pub(in crate::engine) u64);
