//! Length-prefixed peer frames (BEP 3, port from BEP 5, envelope from BEP 10).
//!
//! Structural decoding is separate from layout validation and connection policy.
//! Unknown message/extension payloads are preserved, not advertised as supported.
//! No allocation, streaming buffer, I/O, capability negotiation or bdecode here.

use std::fmt;

use crate::protocol::metainfo::Layout;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRequest {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl BlockRequest {
    /// Validate against the actual piece size, including the final short piece.
    /// The caller chooses its accepted block limit; there is no alignment rule.
    pub fn validate(&self, layout: &Layout<'_>, max_block_length: u32) -> Result<(), Error> {
        let piece = layout
            .piece_range(self.index)
            .ok_or(Error::PieceIndex(self.index))?;
        if self.length == 0 || self.length > max_block_length {
            return Err(Error::BlockLength {
                length: self.length,
                limit: max_block_length,
            });
        }
        let end = self
            .begin
            .checked_add(self.length)
            .ok_or(Error::BlockRange)?;
        if u64::from(end) > piece.end - piece.start {
            return Err(Error::BlockRange);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message<'a> {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(&'a [u8]),
    Request(BlockRequest),
    Piece {
        index: u32,
        begin: u32,
        block: &'a [u8],
    },
    Cancel(BlockRequest),
    Port(u16),
    SuggestPiece(u32),
    HaveAll,
    HaveNone,
    Reject(BlockRequest),
    AllowedFast(u32),
    /// Opaque BEP 10 payload, including handshake id=0. IDs are directional:
    /// sending uses the remote advertised mapping; receiving uses our mapping.
    /// This enum does not negotiate or interpret either mapping.
    Extended {
        id: u8,
        payload: &'a [u8],
    },
    /// Unknown outer ID.
    /// The engine must decide whether to reject, ignore or handle it.
    Unknown {
        id: u8,
        payload: &'a [u8],
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Incomplete {
        additional: usize,
    },
    FrameLimit {
        length: u32,
        limit: u32,
    },
    LengthOverflow,
    InvalidLength {
        id: u8,
        length: u32,
    },
    OutputTooSmall {
        needed: usize,
    },
    /// Unknown cannot be used to bypass a known variant's structural checks.
    KnownMessageId(u8),
    PieceIndex(u32),
    BlockLength {
        length: u32,
        limit: u32,
    },
    BlockRange,
    BitfieldLength {
        expected: usize,
        actual: usize,
    },
    BitfieldSpareBits,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peer message {self:?}")
    }
}
impl std::error::Error for Error {}

/// Frame-body budget derived from known layout and caller-selected payload
/// limits. Includes message/extension IDs, excludes the four-byte length prefix.
/// This is a size bound, NOT a promise to accept/advertise any extension.
pub fn frame_limit(
    piece_count: u32,
    max_block_length: u32,
    max_extension_payload: u32,
) -> Result<u32, Error> {
    let bitfield = piece_count.div_ceil(8) + 1;
    let piece = max_block_length
        .checked_add(9)
        .ok_or(Error::LengthOverflow)?;
    let extension = max_extension_payload
        .checked_add(2)
        .ok_or(Error::LengthOverflow)?;
    Ok(13.max(bitfield).max(piece).max(extension))
}

impl<'a> Message<'a> {
    /// Decode only the first frame; trailing frames remain untouched. Reject
    /// over-budget prefixes before waiting for a body. Known fixed/minimum
    /// lengths are checked as soon as the ID is present, before body completion.
    /// Incomplete frames borrow nothing and require no allocation.
    pub fn decode(input: &'a [u8], max_frame_length: u32) -> Result<(Self, usize), Error> {
        if input.len() < 4 {
            return Err(Error::Incomplete {
                additional: 4 - input.len(),
            });
        }
        let length = read_u32(&input[..4]);
        check_limit(length, max_frame_length)?;
        let total = total_length(length)?;
        if length == 0 {
            return Ok((Self::KeepAlive, 4));
        }
        if let Some(&id) = input.get(4) {
            check_length(id, length)?;
        }
        if input.len() < total {
            return Err(Error::Incomplete {
                additional: total - input.len(),
            });
        }
        let body = &input[5..total];
        let message = match input[4] {
            0 => Self::Choke,
            1 => Self::Unchoke,
            2 => Self::Interested,
            3 => Self::NotInterested,
            4 => Self::Have(read_u32(body)),
            5 => Self::Bitfield(body),
            6 => Self::Request(read_request(body)),
            7 => Self::Piece {
                index: read_u32(body),
                begin: read_u32(&body[4..]),
                block: &body[8..],
            },
            8 => Self::Cancel(read_request(body)),
            9 => Self::Port(u16::from_be_bytes([body[0], body[1]])),
            13 => Self::SuggestPiece(read_u32(body)),
            14 => Self::HaveAll,
            15 => Self::HaveNone,
            16 => Self::Reject(read_request(body)),
            17 => Self::AllowedFast(read_u32(body)),
            20 => Self::Extended {
                id: body[0],
                payload: &body[1..],
            },
            id => Self::Unknown { id, payload: body },
        };
        Ok((message, total))
    }

    /// Complete frame size, including length prefix, subject to the same budget
    /// as decoding. Does not allocate or validate layout/capability policy.
    pub fn encoded_len(&self, max_frame_length: u32) -> Result<usize, Error> {
        let length = self.body_length()?;
        check_limit(length, max_frame_length)?;
        total_length(length)
    }

    /// Encode into a caller-owned buffer. On any error the buffer is unchanged;
    /// on success bytes beyond the returned frame length are untouched.
    pub fn encode(&self, output: &mut [u8], max_frame_length: u32) -> Result<usize, Error> {
        let total = self.encoded_len(max_frame_length)?;
        if output.len() < total {
            return Err(Error::OutputTooSmall { needed: total });
        }
        let length = self.body_length()?;
        output[..4].copy_from_slice(&length.to_be_bytes());
        if let Some(id) = self.id() {
            output[4] = id;
        }
        match self {
            Self::Have(index) | Self::SuggestPiece(index) | Self::AllowedFast(index) => {
                output[5..9].copy_from_slice(&index.to_be_bytes());
            }
            Self::Bitfield(payload) | Self::Unknown { payload, .. } => {
                output[5..total].copy_from_slice(payload)
            }
            Self::Request(request) | Self::Cancel(request) | Self::Reject(request) => {
                write_pair(&mut output[5..13], request.index, request.begin);
                output[13..17].copy_from_slice(&request.length.to_be_bytes());
            }
            Self::Piece {
                index,
                begin,
                block,
            } => {
                write_pair(&mut output[5..13], *index, *begin);
                output[13..total].copy_from_slice(block);
            }
            Self::Port(port) => output[5..7].copy_from_slice(&port.to_be_bytes()),
            Self::Extended { id, payload } => {
                output[5] = *id;
                output[6..total].copy_from_slice(payload);
            }
            _ => {}
        }
        Ok(total)
    }

    /// Validate only torrent-dependent fields: have index, bitfield length/spare
    /// bits, and request/cancel/piece ranges. Does NOT authorize requests, check
    /// bitfield ordering, negotiate extensions, or reject unknown messages.
    /// Structural decoding intentionally permits empty blocks and arbitrary
    /// indices so the caller can choose how to handle malformed/late traffic.
    pub fn validate_layout(&self, layout: &Layout<'_>, max_block_length: u32) -> Result<(), Error> {
        match self {
            Self::Have(index) | Self::SuggestPiece(index) | Self::AllowedFast(index)
                if *index >= layout.piece_count() =>
            {
                Err(Error::PieceIndex(*index))
            }
            Self::Bitfield(bits) => {
                let expected = usize::try_from(layout.piece_count().div_ceil(8))
                    .map_err(|_| Error::LengthOverflow)?;
                if bits.len() != expected {
                    return Err(Error::BitfieldLength {
                        expected,
                        actual: bits.len(),
                    });
                }
                let spare = (8 - layout.piece_count() % 8) % 8;
                if spare != 0
                    && bits
                        .last()
                        .is_some_and(|last| last & ((1u8 << spare) - 1) != 0)
                {
                    return Err(Error::BitfieldSpareBits);
                }
                Ok(())
            }
            Self::Request(request) | Self::Cancel(request) | Self::Reject(request) => {
                request.validate(layout, max_block_length)
            }
            Self::Piece {
                index,
                begin,
                block,
            } => BlockRequest {
                index: *index,
                begin: *begin,
                length: u32::try_from(block.len()).map_err(|_| Error::LengthOverflow)?,
            }
            .validate(layout, max_block_length),
            _ => Ok(()),
        }
    }

    fn id(&self) -> Option<u8> {
        Some(match self {
            Self::KeepAlive => return None,
            Self::Choke => 0,
            Self::Unchoke => 1,
            Self::Interested => 2,
            Self::NotInterested => 3,
            Self::Have(_) => 4,
            Self::Bitfield(_) => 5,
            Self::Request(_) => 6,
            Self::Piece { .. } => 7,
            Self::Cancel(_) => 8,
            Self::Port(_) => 9,
            Self::SuggestPiece(_) => 13,
            Self::HaveAll => 14,
            Self::HaveNone => 15,
            Self::Reject(_) => 16,
            Self::AllowedFast(_) => 17,
            Self::Extended { .. } => 20,
            Self::Unknown { id, .. } => *id,
        })
    }

    fn body_length(&self) -> Result<u32, Error> {
        let (header, payload) = match self {
            Self::KeepAlive => (0, 0),
            Self::Choke
            | Self::Unchoke
            | Self::Interested
            | Self::NotInterested
            | Self::HaveAll
            | Self::HaveNone => (1, 0),
            Self::Have(_) | Self::SuggestPiece(_) | Self::AllowedFast(_) => (5, 0),
            Self::Request(_) | Self::Cancel(_) | Self::Reject(_) => (13, 0),
            Self::Port(_) => (3, 0),
            Self::Bitfield(bits) => (1, bits.len()),
            Self::Piece { block, .. } => (9, block.len()),
            Self::Extended { payload, .. } => (2, payload.len()),
            Self::Unknown { id, payload } => {
                if *id <= 9 || (13..=17).contains(id) || *id == 20 {
                    return Err(Error::KnownMessageId(*id));
                }
                (1, payload.len())
            }
        };
        u32::try_from(payload)
            .ok()
            .and_then(|length| length.checked_add(header))
            .ok_or(Error::LengthOverflow)
    }
}

fn check_limit(length: u32, limit: u32) -> Result<(), Error> {
    if length > limit {
        return Err(Error::FrameLimit { length, limit });
    }
    Ok(())
}
fn total_length(length: u32) -> Result<usize, Error> {
    usize::try_from(length)
        .ok()
        .and_then(|length| length.checked_add(4))
        .ok_or(Error::LengthOverflow)
}
fn check_length(id: u8, length: u32) -> Result<(), Error> {
    let valid = match id {
        0..=3 => length == 1,
        4 | 13 | 17 => length == 5,
        14 | 15 => length == 1,
        6 | 8 | 16 => length == 13,
        7 => length >= 9,
        9 => length == 3,
        20 => length >= 2,
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidLength { id, length })
    }
}
fn read_u32(input: &[u8]) -> u32 {
    u32::from_be_bytes(input[..4].try_into().expect("checked frame size"))
}
fn read_request(body: &[u8]) -> BlockRequest {
    BlockRequest {
        index: read_u32(body),
        begin: read_u32(&body[4..]),
        length: read_u32(&body[8..]),
    }
}
fn write_pair(output: &mut [u8], index: u32, begin: u32) {
    output[..4].copy_from_slice(&index.to_be_bytes());
    output[4..8].copy_from_slice(&begin.to_be_bytes());
}
