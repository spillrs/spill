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

use crate::engine::core::fault::peer::invalid_message;
pub(in crate::engine) enum Outbound {
    Encoded(Vec<u8>),
    Piece {
        header: [u8; 13],
        block: crate::engine::core::SharedBlock,
    },
}
impl From<Vec<u8>> for Outbound {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Encoded(bytes)
    }
}
impl Outbound {
    pub(in crate::engine) fn header(&self) -> &[u8] {
        match self {
            Self::Encoded(bytes) => bytes,
            Self::Piece { header, .. } => header,
        }
    }
    pub(in crate::engine) fn payload(&self) -> &[u8] {
        match self {
            Self::Encoded(_) => &[],
            Self::Piece { block, .. } => block,
        }
    }
    pub(in crate::engine) fn len(&self) -> usize {
        self.header().len() + self.payload().len()
    }
    pub(in crate::engine) fn message(&self, limit: u32) -> io::Result<Message<'_>> {
        match self {
            Self::Encoded(bytes) => Message::decode(bytes, limit)
                .map(|(m, _)| m)
                .map_err(invalid_message),
            Self::Piece { header, block } => Ok(Message::Piece {
                index: u32::from_be_bytes(header[5..9].try_into().expect("piece index")),
                begin: u32::from_be_bytes(header[9..13].try_into().expect("piece begin")),
                block,
            }),
        }
    }
    pub(in crate::engine) fn payload_start(&self) -> usize {
        match self.message(u32::MAX) {
            Ok(Message::Piece { block, .. }) => self.len() - block.len(),
            _ => self.len(),
        }
    }
}
