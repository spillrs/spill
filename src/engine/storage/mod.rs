//! File-backed piece/block storage. One bounded writer task owns each destination;
//! dropping a response waiter never cancels I/O. Only hash matches after completed
//! writes become available.

pub(in crate::engine) mod cache;
pub(in crate::engine) mod writer;
use std::io;

/// Piece indices whose stored bytes may have changed. Sorted, without duplicates.
pub(in crate::engine) type PieceSet = Vec<u32>;

/// Publish an encoded resume checkpoint. Failure stops the storage owner.
pub(in crate::engine) type Checkpoint = Box<dyn Fn(Vec<u8>) -> io::Result<()> + Send + Sync>;
