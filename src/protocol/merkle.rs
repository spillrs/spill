//! BEP 52 SHA-256 trees. Missing leaves are zero hashes, not zero-filled blocks.
//! Callers validate metainfo and remote proof geometry before reducing trees.

use sha2::{Digest, Sha256};

pub const BLOCK_LENGTH: usize = 16 * 1024;

/// Hash an actual file block, including an unpadded short final block.
///
/// # Panics
/// If the block is empty or larger than 16 KiB.
pub fn block_hash(bytes: &[u8]) -> [u8; 32] {
    assert!(!bytes.is_empty() && bytes.len() <= BLOCK_LENGTH);
    Sha256::digest(bytes).into()
}

pub fn parent(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(left);
    hash.update(right);
    hash.finalize().into()
}

/// Root of an absent subtree at `layer` above the 16 KiB leaf layer.
///
/// # Panics
/// If `layer` exceeds 50, the maximum height for a u64 file length.
pub fn padding_hash(layer: u32) -> [u8; 32] {
    assert!(layer <= 50);
    let mut hash = [0; 32];
    for _ in 0..layer {
        hash = parent(&hash, &hash);
    }
    hash
}

/// Reduce a nonempty layer, padding to its next power-of-two width.
/// `base_layer` identifies the layer above the 16 KiB leaves, so piece layers
/// are padded with subtree roots rather than zero hashes.
/// Allocates at most one hash per supplied hash; never materializes absent leaves.
///
/// # Panics
/// If the layer is empty or the resulting tree height exceeds 50.
pub fn root_from_layer(hashes: &[[u8; 32]], base_layer: u32) -> [u8; 32] {
    assert!(!hashes.is_empty());
    let height = usize::BITS - (hashes.len() - 1).leading_zeros();
    assert!(base_layer <= 50 && height <= 50 - base_layer);
    let mut padding = padding_hash(base_layer);
    let mut nodes = hashes.to_vec();
    while nodes.len() > 1 {
        let count = nodes.len();
        for index in 0..count.div_ceil(2) {
            let left = nodes[index * 2];
            let right = nodes.get(index * 2 + 1).copied().unwrap_or(padding);
            nodes[index] = parent(&left, &right);
        }
        nodes.truncate(count.div_ceil(2));
        padding = parent(&padding, &padding);
    }
    nodes[0]
}

/// One node above an authenticated layer, including absent subtrees.
/// `index` is at the requested height relative to this layer.
///
/// # Panics
/// If the requested height exceeds the u64 file-tree limit.
pub(crate) fn subtree(hashes: &[[u8; 32]], base_layer: u32, height: u32, index: u64) -> [u8; 32] {
    assert!(base_layer + height <= 50);
    let start = index * (1u64 << height);
    if start >= hashes.len() as u64 {
        return padding_hash(base_layer + height);
    }
    let end = (start + (1u64 << height)).min(hashes.len() as u64);
    let nodes = &hashes[start as usize..end as usize];
    let mut root = root_from_layer(nodes, base_layer);
    let used_height = usize::BITS - (nodes.len() - 1).leading_zeros();
    for level in used_height..height {
        root = parent(&root, &padding_hash(base_layer + level));
    }
    root
}

/// Reduce block hashes for one piece of a file larger than the piece length.
/// Storage can compute these hashes while streaming, retaining them for proofs.
/// Short final pieces are extended with absent leaves to the full piece height.
/// For a file no larger than one piece, use `root_from_layer` over its block
/// hashes instead: that file's root has only its actual tree height.
///
/// # Panics
/// If `piece_length` is not a power of two of at least 16 KiB, or `blocks` is
/// empty or contains more hashes than a piece can hold.
pub fn piece_hash(blocks: &[[u8; 32]], piece_length: u32) -> [u8; 32] {
    assert!(piece_length >= BLOCK_LENGTH as u32 && piece_length.is_power_of_two());
    assert!(!blocks.is_empty() && blocks.len() <= (piece_length / BLOCK_LENGTH as u32) as usize);
    let mut root = root_from_layer(blocks, 0);
    let height = usize::BITS - (blocks.len() - 1).leading_zeros();
    let piece_height = (piece_length / BLOCK_LENGTH as u32).ilog2();
    let mut padding = padding_hash(height);
    for _ in height..piece_height {
        root = parent(&root, &padding);
        padding = parent(&padding, &padding);
    }
    root
}
