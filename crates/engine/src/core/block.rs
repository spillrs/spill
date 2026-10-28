//! Immutable payload views shared across the core/storage/transport boundary.
use std::{
    ops::{Deref, Range},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub(crate) struct SharedBlock {
    bytes: Arc<Vec<u8>>,
    range: Range<usize>,
}
impl SharedBlock {
    pub(crate) fn from_shared(bytes: Arc<Vec<u8>>, range: Range<usize>) -> Self {
        assert!(range.start <= range.end && range.end <= bytes.len());
        Self { bytes, range }
    }
}
impl Deref for SharedBlock {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes[self.range.clone()]
    }
}
