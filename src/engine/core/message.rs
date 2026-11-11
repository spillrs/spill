//! Owned, validated protocol values; no transport authority.
use super::SharedBlock;
use crate::protocol::peer::message::Message;
use std::{fmt, io, sync::Arc};

#[derive(Clone)]
pub(in crate::engine) struct Frame {
    pub(in crate::engine) bytes: Arc<Vec<u8>>,
}
impl Frame {
    pub(in crate::engine) fn from_message(message: Message<'_>, limit: u32) -> io::Result<Self> {
        let len = message.encoded_len(limit).map_err(io::Error::other)?;
        let mut bytes = vec![0; len];
        message
            .encode(&mut bytes, limit)
            .map_err(io::Error::other)?;
        Ok(Self {
            bytes: Arc::new(bytes),
        })
    }
    pub(in crate::engine) fn wire_len(&self) -> usize {
        self.bytes.len()
    }
    pub(in crate::engine) fn piece_block(&self) -> SharedBlock {
        let Message::Piece { block, .. } = self.message() else {
            unreachable!("Piece frame")
        };
        SharedBlock::from_shared(
            self.bytes.clone(),
            self.bytes.len() - block.len()..self.bytes.len(),
        )
    }
    pub(in crate::engine) fn message(&self) -> Message<'_> {
        Message::decode(&self.bytes, u32::MAX)
            .expect("validated frame")
            .0
    }
}
impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Frame")
            .field("wire_bytes", &self.bytes.len())
            .finish()
    }
}
