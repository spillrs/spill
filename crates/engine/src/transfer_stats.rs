//! Shared payload observations, independent of a standalone upload owner.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
/// Payload bytes accepted by successful AsyncWrite::write calls. Retained by the
/// transport owner across task reaping/barriers; no frames, sockets or payloads.
#[derive(Default)]
pub(crate) struct PayloadWrites {
    bytes: AtomicU64,
    parent: Option<Arc<PayloadWrites>>,
}
impl PayloadWrites {
    pub(crate) fn child(parent: Arc<Self>) -> Self {
        Self {
            parent: Some(parent),
            ..Self::default()
        }
    }
    pub(crate) fn add(&self, bytes: usize) {
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        if let Some(parent) = &self.parent {
            parent.add(bytes);
        }
    }
    pub(crate) fn snapshot(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}
