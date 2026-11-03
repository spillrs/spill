//! Incremental piece verification over logical bytes, with v2 file-tail limits.
use super::{OwnedMetainfo, PieceVerification};
use crate::protocol::merkle;
use sha1::{Digest, Sha1};
use sha2::Sha256;

pub(crate) struct PieceAccumulator {
    sha1: Option<Sha1>,
    block: Sha256,
    block_len: usize,
    remaining: usize,
    leaves: Vec<[u8; 32]>,
}
impl PieceAccumulator {
    pub(crate) fn new(data_length: usize, sha1: bool, sha256: bool) -> Self {
        Self {
            sha1: sha1.then(Sha1::new),
            block: Sha256::new(),
            block_len: 0,
            remaining: if sha256 { data_length } else { 0 },
            leaves: Vec::new(),
        }
    }
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        if let Some(sha1) = &mut self.sha1 {
            sha1.update(bytes);
        }
        let mut data = &bytes[..bytes.len().min(self.remaining)];
        self.remaining -= data.len();
        while !data.is_empty() {
            let length = data.len().min(merkle::BLOCK_LENGTH - self.block_len);
            self.block.update(&data[..length]);
            self.block_len += length;
            data = &data[length..];
            if self.block_len == merkle::BLOCK_LENGTH {
                self.leaves.push(self.block.finalize_reset().into());
                self.block_len = 0;
            }
        }
    }
    pub(crate) fn finish(mut self) -> (Option<[u8; 20]>, Vec<[u8; 32]>) {
        assert_eq!(self.remaining, 0, "complete piece data");
        if self.block_len != 0 {
            self.leaves.push(self.block.finalize().into());
        }
        (self.sha1.map(|hash| hash.finalize().into()), self.leaves)
    }
}

pub(crate) struct PieceHasher {
    expected: PieceVerification,
    accumulator: PieceAccumulator,
}
impl PieceHasher {
    pub(crate) fn new(meta: &OwnedMetainfo, index: u32) -> Self {
        let expected = meta.verification(index).expect("piece verification");
        Self {
            expected,
            accumulator: PieceAccumulator::new(
                if matches!(expected, PieceVerification::V1(_)) {
                    0
                } else {
                    meta.layout().piece_data_end(index).expect("piece data end") as usize
                },
                !matches!(expected, PieceVerification::V2(_)),
                !matches!(expected, PieceVerification::V1(_)),
            ),
        }
    }
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.accumulator.update(bytes);
    }
    pub(crate) fn finish(self) -> Result<(bool, Vec<[u8; 32]>), super::Error> {
        let (sha1, leaves) = self.accumulator.finish();
        let check_v2 = |v2: super::V2Verification| {
            merkle::piece_hash(&leaves, v2.blocks * merkle::BLOCK_LENGTH as u32) == v2.layer_hash
        };
        let passed = match self.expected {
            PieceVerification::V1(expected) => sha1.expect("v1 digest") == expected,
            PieceVerification::V2(v2) => check_v2(v2),
            PieceVerification::Hybrid { sha1: expected, v2 } => {
                let v1_passed = sha1.expect("hybrid digest") == expected;
                let v2_passed = check_v2(v2);
                if v1_passed != v2_passed {
                    return Err(super::Error::InvalidField(
                        "inconsistent hybrid piece commitments",
                    ));
                }
                v1_passed && v2_passed
            }
        };
        Ok((passed, leaves))
    }
}
