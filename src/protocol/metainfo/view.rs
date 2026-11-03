use super::*;

/// Allocation-free borrowed content view of a validated borrowed or owned
/// catalog. Its private fields cannot be assembled from an unvalidated identity,
/// layout or hash array. Copying the view does not extend its owner's lifetime.
/// Outer/unknown fields remain on the catalog, not in this storage-facing view.
#[derive(Clone, Copy, Debug)]
pub struct MetainfoView<'a> {
    pub(super) info_hash: TorrentIdentity,
    pub(super) layout: &'a Layout<'a>,
    pub(super) hashes: &'a [u8],
    pub(super) v2_hashes: &'a [[u8; 32]],
    pub(super) layers_pending: bool,
}
impl<'a> MetainfoView<'a> {
    pub fn info_hash(self) -> TorrentIdentity {
        self.info_hash
    }
    pub fn layout(self) -> &'a Layout<'a> {
        self.layout
    }
    pub fn piece_hash(self, index: u32) -> Option<&'a [u8; 20]> {
        hash_at(self.hashes, index, self.layout.piece_count())
    }
    /// Authenticated v2 commitments in logical piece order. Empty for v1 or pending hybrids.
    /// Small files contribute their file root; larger files contribute their
    /// piece layer. Virtual padding contributes no additional hashes.
    pub(crate) fn v2_hashes(self) -> &'a [[u8; 32]] {
        self.v2_hashes
    }
    pub fn verification(self, index: u32) -> Option<PieceVerification> {
        let range = self.layout.piece_range(index)?;
        let sha1 = self.piece_hash(index).copied();
        if self.info_hash.v2().is_none() || self.layers_pending {
            return Some(PieceVerification::V1(sha1.expect("v1 piece hash")));
        }
        let file = &self.layout.files[self
            .layout
            .files
            .partition_point(|file| file.offset + file.length <= range.start)];
        let block_length = crate::protocol::merkle::BLOCK_LENGTH as u64;
        let blocks = file
            .length
            .min(u64::from(self.layout.piece_length))
            .div_ceil(block_length)
            .next_power_of_two() as u32;
        let v2 = V2Verification {
            pieces_root: file.pieces_root.expect("v2 data file"),
            layer_hash: self.v2_hashes[index as usize],
            blocks,
        };
        Some(match sha1 {
            Some(sha1) => PieceVerification::Hybrid { sha1, v2 },
            None => PieceVerification::V2(v2),
        })
    }
}
impl<'a> From<&'a Metainfo<'_>> for MetainfoView<'a> {
    fn from(meta: &'a Metainfo<'_>) -> Self {
        meta.view()
    }
}
impl<'a> From<&'a OwnedMetainfo> for MetainfoView<'a> {
    fn from(meta: &'a OwnedMetainfo) -> Self {
        meta.view()
    }
}
