//! BEP 52 hash-range geometry and authenticated responses. No request policy.

use crate::protocol::merkle;
use std::fmt;

/// Shared binary header of hash request, hashes and hash reject messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashRequest {
    pub pieces_root: [u8; 32],
    pub base_layer: u32,
    pub index: u32,
    pub length: u32,
    pub proof_layers: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Length,
    Alignment,
    Layer,
    Range,
    HashCount,
    IncompleteProof,
    RootMismatch,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hash exchange {self:?}")
    }
}
impl std::error::Error for Error {}

impl HashRequest {
    /// Number of response hashes, including only non-redundant proof nodes.
    /// Checks context-free geometry, not file membership or a serving limit.
    /// Singletons follow libtorrent's proof-count convention despite BEP 52's
    /// minimum of two; spill's acquisition policy still requests padded pairs.
    pub fn response_hash_count(&self) -> Result<u32, Error> {
        if !self.length.is_power_of_two() {
            return Err(Error::Length);
        }
        if !self.index.is_multiple_of(self.length) {
            return Err(Error::Alignment);
        }
        // A u64 file length needs at most 50 levels above 16 KiB blocks.
        if self.base_layer > 50 || self.proof_layers > 50 {
            return Err(Error::Layer);
        }
        Ok(self.length + self.proof_hash_count())
    }

    fn proof_hash_count(&self) -> u32 {
        if self.length == 1 {
            // No sibling is implied; libtorrent includes the base-layer sibling.
            self.proof_layers + 1
        } else {
            self.proof_layers.saturating_sub(self.length.ilog2() - 1)
        }
    }

    /// Validate a remote range against a nonempty file and caller-selected cap.
    /// Unsupported base layers (other than leaves/pieces) are serving policy.
    pub fn validate(&self, file_length: u64, max_hashes: u32) -> Result<(), Error> {
        self.response_hash_count()?;
        if self.length > max_hashes {
            return Err(Error::Length);
        }
        if file_length == 0 {
            return Err(Error::Range);
        }
        let blocks = file_length.div_ceil(merkle::BLOCK_LENGTH as u64);
        let height = u64::BITS - (blocks - 1).leading_zeros();
        if self.base_layer >= height || self.proof_layers >= height - self.base_layer {
            return Err(Error::Layer);
        }
        let width = 1u64 << (height - self.base_layer);
        if u64::from(self.index) + u64::from(self.length) > width {
            return Err(Error::Range);
        }
        Ok(())
    }

    /// Construct a response from an authenticated piece layer and verified leaves.
    /// Geometry and the serving cap must already have been validated. Leaves
    /// start at the piece boundary containing `index` and include all requested
    /// pieces (padded to piece width); no leaves are needed for piece-layer work.
    /// Small files use their file root as a one-element layer at file-tree height.
    pub(crate) fn response(
        &self,
        piece_layer: &[[u8; 32]],
        piece_height: u32,
        tree: &[Vec<[u8; 32]>],
        leaves: &[[u8; 32]],
    ) -> Vec<u8> {
        let mut padding = [[0; 32]; 51];
        for height in 1..=piece_height as usize + tree.len() {
            padding[height] = merkle::parent(&padding[height - 1], &padding[height - 1]);
        }
        let leaf_start = u64::from(self.index) / (1u64 << piece_height) * (1u64 << piece_height);
        let node = |height: u32, index: u64| {
            if height >= piece_height {
                let layer = if height == piece_height {
                    piece_layer
                } else {
                    tree.get((height - piece_height - 1) as usize)
                        .map_or(&[][..], Vec::as_slice)
                };
                layer
                    .get(index as usize)
                    .copied()
                    .unwrap_or(padding[height as usize])
            } else {
                merkle::subtree(leaves, 0, height, index - (leaf_start >> height))
            }
        };
        let mut hashes = Vec::with_capacity(
            self.response_hash_count().expect("validated hash geometry") as usize * 32,
        );
        for index in u64::from(self.index)..u64::from(self.index) + u64::from(self.length) {
            hashes.extend_from_slice(&node(self.base_layer, index));
        }
        let subtree_height = self.length.ilog2();
        let height = self.base_layer + subtree_height;
        let mut index = u64::from(self.index / self.length);
        for height in height..height + self.proof_hash_count() {
            hashes.extend_from_slice(&node(height, index ^ 1));
            index >>= 1;
        }
        hashes
    }

    /// Authenticate a correlated response all the way to this file's root.
    /// `hashes` is the concatenated binary response payload, not its header.
    /// Partial proofs are valid wire messages but cannot authenticate a layer
    /// without previously trusted intermediate nodes; this function rejects them.
    pub fn verify(&self, file_length: u64, max_hashes: u32, hashes: &[u8]) -> Result<(), Error> {
        self.validate(file_length, max_hashes)?;
        if hashes.len() as u64 != u64::from(self.response_hash_count()?) * 32 {
            return Err(Error::HashCount);
        }
        let blocks = file_length.div_ceil(merkle::BLOCK_LENGTH as u64);
        let height = u64::BITS - (blocks - 1).leading_zeros();
        let subtree_height = self.length.ilog2();
        let proofs = self.proof_hash_count();
        if self.base_layer + subtree_height + proofs != height {
            return Err(Error::IncompleteProof);
        }
        let base_bytes = self.length as usize * 32;
        let nodes = hashes[..base_bytes].as_chunks::<32>().0;
        let mut root = merkle::root_from_layer(nodes, self.base_layer);
        let mut index = self.index / self.length;
        for sibling in hashes[base_bytes..].as_chunks::<32>().0 {
            root = if index & 1 == 0 {
                merkle::parent(&root, sibling)
            } else {
                merkle::parent(sibling, &root)
            };
            index >>= 1;
        }
        if root != self.pieces_root {
            return Err(Error::RootMismatch);
        }
        Ok(())
    }
}
