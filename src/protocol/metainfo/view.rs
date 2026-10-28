use super::*;

/// Allocation-free borrowed content view of a validated borrowed or owned
/// catalog. Its private fields cannot be assembled from an unvalidated identity,
/// layout or hash array. Copying the view does not extend its owner's lifetime.
/// Outer/unknown fields remain on the catalog, not in this storage-facing view.
#[derive(Clone, Copy, Debug)]
pub struct MetainfoView<'a> {
    pub(super) info_hash: InfoHashV1,
    pub(super) layout: &'a Layout<'a>,
    pub(super) hashes: &'a [u8],
}
impl<'a> MetainfoView<'a> {
    pub fn info_hash(self) -> InfoHashV1 {
        self.info_hash
    }
    pub fn layout(self) -> &'a Layout<'a> {
        self.layout
    }
    pub fn piece_hash(self, index: u32) -> Option<&'a [u8; 20]> {
        hash_at(self.hashes, index, self.layout.piece_count())
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
