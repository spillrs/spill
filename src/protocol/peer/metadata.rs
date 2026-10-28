//! BEP 9 `ut_metadata` messages: a bencoded header and an opaque binary trailer.

use std::{collections::BTreeMap, fmt, ops::Range};

use crate::protocol::bencode::{self, Limits, Node, Value};

pub const PIECE_LENGTH: u32 = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Request { piece: u32 },
    Data { piece: u32, total_size: u32 },
    Reject { piece: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Bencode(bencode::Error),
    Dictionary,
    Missing(&'static str),
    InvalidField(&'static str),
    MetadataSize { size: u32, limit: u32 },
    PieceIndex(u32),
    PieceLength { expected: usize, actual: usize },
    SizeMismatch { expected: u32, actual: u32 },
    UnexpectedPayload,
    OutputLimit,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metadata message {self:?}")
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bencode(error) => Some(error),
            _ => None,
        }
    }
}

/// Logical range within the original info bytes. The total must be positive and
/// within the caller's metadata-size policy. u64 offset arithmetic cannot wrap
/// even for malicious u32 piece indices; out-of-range indices are rejected.
pub fn piece_range(
    piece: u32,
    total_size: u32,
    max_metadata_size: u32,
) -> Result<Range<u64>, Error> {
    check_size(total_size, max_metadata_size)?;
    let start = u64::from(piece) * u64::from(PIECE_LENGTH);
    let total = u64::from(total_size);
    if start >= total {
        return Err(Error::PieceIndex(piece));
    }
    Ok(start..(start + u64::from(PIECE_LENGTH)).min(total))
}

/// An immutable, parsed or constructed message. Unknown header keys survive
/// encoding; data is borrowed exactly, never interpreted as bencode. Limits
/// apply to the whole payload, including the trailer, not just the header.
#[derive(Clone, Debug)]
pub struct Message<'a> {
    header: Node<'a>,
    payload: &'a [u8],
    kind: Kind,
}

impl<'a> Message<'a> {
    /// Parse one complete extension payload. Bencode input/node/string/depth
    /// limits cover the supplied slice/tree; known data must have exactly the
    /// declared piece's bytes (including a short final piece). No size-based
    /// metadata buffer is allocated.
    pub fn parse(input: &'a [u8], limits: Limits, max_metadata_size: u32) -> Result<Self, Error> {
        let (header, consumed) = bencode::decode_prefix(input, limits).map_err(Error::Bencode)?;
        Self::from_header(header, &input[consumed..], max_metadata_size)
    }

    /// Construct standard fields. The separate payload is empty for requests
    /// and rejects; known data messages enforce the same bounds as decoding.
    pub fn new(kind: Kind, payload: &'a [u8], max_metadata_size: u32) -> Result<Self, Error> {
        let mut fields = BTreeMap::new();
        let message_type = match kind {
            Kind::Request { piece } | Kind::Reject { piece } => {
                fields.insert(
                    b"piece".as_slice(),
                    Node::new(Value::Integer(i64::from(piece))),
                );
                if matches!(kind, Kind::Request { .. }) {
                    0
                } else {
                    2
                }
            }
            Kind::Data { piece, total_size } => {
                fields.insert(
                    b"piece".as_slice(),
                    Node::new(Value::Integer(i64::from(piece))),
                );
                fields.insert(
                    b"total_size".as_slice(),
                    Node::new(Value::Integer(i64::from(total_size))),
                );
                1
            }
        };
        fields.insert(
            b"msg_type".as_slice(),
            Node::new(Value::Integer(message_type)),
        );
        Self::from_header(
            Node::new(Value::Dictionary(fields)),
            payload,
            max_metadata_size,
        )
    }

    fn from_header(
        header: Node<'a>,
        payload: &'a [u8],
        max_metadata_size: u32,
    ) -> Result<Self, Error> {
        if !matches!(header.value(), Value::Dictionary(_)) {
            return Err(Error::Dictionary);
        }
        let message_type = integer(&header, "msg_type")?;
        let kind = match message_type {
            0 | 2 => {
                let piece = unsigned(&header, "piece")?;
                if !payload.is_empty() {
                    return Err(Error::UnexpectedPayload);
                }
                if message_type == 0 {
                    Kind::Request { piece }
                } else {
                    Kind::Reject { piece }
                }
            }
            1 => {
                let piece = unsigned(&header, "piece")?;
                let total_size = unsigned(&header, "total_size")?;
                let range = piece_range(piece, total_size, max_metadata_size)?;
                let expected = (range.end - range.start) as usize; // At most 16 KiB.
                if payload.len() != expected {
                    return Err(Error::PieceLength {
                        expected,
                        actual: payload.len(),
                    });
                }
                Kind::Data { piece, total_size }
            }
            _ => return Err(Error::InvalidField("msg_type")),
        };
        Ok(Self {
            header,
            payload,
            kind,
        })
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }
    pub fn header(&self) -> &Node<'a> {
        &self.header
    }
    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }

    /// Check known messages against the already agreed metadata size. Requests
    /// and rejects have no size of their own; their piece index can only be
    /// range-checked once this size is known.
    /// This does not match outstanding requests or authorize serving content.
    pub fn validate_size(&self, expected_size: u32, max_metadata_size: u32) -> Result<(), Error> {
        let piece = match self.kind {
            Kind::Request { piece } | Kind::Reject { piece } => piece,
            Kind::Data { piece, total_size } => {
                if total_size != expected_size {
                    return Err(Error::SizeMismatch {
                        expected: expected_size,
                        actual: total_size,
                    });
                }
                piece
            }
        };
        piece_range(piece, expected_size, max_metadata_size)?;
        Ok(())
    }

    /// Canonical header plus the EXACT trailer. The output limit includes both;
    /// reserve the trailer's budget before encoding the header. Re-encoding the
    /// transferred info dictionary itself is never a substitute for hashing its
    /// original bytes after complete assembly and size/identity verification.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let max_output_len = limits
            .max_output_len
            .checked_sub(self.payload.len())
            .ok_or(Error::OutputLimit)?;
        let mut output = bencode::encode(
            &self.header,
            Limits {
                max_output_len,
                ..limits
            },
        )
        .map_err(Error::Bencode)?;
        output.extend_from_slice(self.payload);
        Ok(output)
    }
}

fn integer(node: &Node<'_>, key: &'static str) -> Result<i64, Error> {
    match node.get(key.as_bytes()).ok_or(Error::Missing(key))?.value() {
        Value::Integer(value) => Ok(*value),
        _ => Err(Error::InvalidField(key)),
    }
}
fn unsigned(node: &Node<'_>, key: &'static str) -> Result<u32, Error> {
    u32::try_from(integer(node, key)?).map_err(|_| Error::InvalidField(key))
}
fn check_size(size: u32, limit: u32) -> Result<(), Error> {
    if size == 0 || size > limit {
        return Err(Error::MetadataSize { size, limit });
    }
    Ok(())
}
