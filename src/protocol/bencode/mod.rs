//! Bounded bencode with borrowed strings and original encoded spans.
//! Canonical encoding must never replace original info bytes for hashing.

use std::{collections::BTreeMap, fmt};

mod decode;
mod encode;

pub use decode::{decode, decode_prefix};
pub use encode::encode;

/// Hard ceiling on recursive container nesting, including configured limits.
pub const MAX_DEPTH: usize = 128;

/// Resource budgets, independent of the eventual transport/message limits.
///
/// Nodes count values AND dictionary keys, including overwritten duplicates.
/// Borrowed strings do not allocate. Container metadata is bounded by node count
/// and input size; this is not an exact allocator-byte budget. Depth counts
/// containers only: zero permits scalars, one permits a list/dict of scalars.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_input_len: usize,
    pub max_string_len: usize,
    pub max_output_len: usize,
    pub max_nodes: usize,
    pub max_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_input_len: 32 * 1024 * 1024,
            max_string_len: 32 * 1024 * 1024,
            max_output_len: 32 * 1024 * 1024,
            max_nodes: 100_000,
            max_depth: MAX_DEPTH,
        }
    }
}

impl Limits {
    fn validate(self) -> Result<Self, Error> {
        if self.max_depth > MAX_DEPTH {
            return Err(Error::new(0, ErrorKind::InvalidLimits));
        }
        Ok(self)
    }
}

/// Semantic values. Dictionary keys and strings are arbitrary bytes, not UTF-8.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value<'a> {
    Integer(i64),
    Bytes(&'a [u8]),
    List(Vec<Node<'a>>),
    Dictionary(BTreeMap<&'a [u8], Node<'a>>),
}

/// Immutable value and, for decoded nodes, the exact original byte span.
///
/// Children retain their own spans, so an `info` value can be hashed directly.
/// Constructed nodes have no original bytes. Mutation is deliberately not
/// exposed: original bytes must never silently disagree with decoded meaning.
#[derive(Clone, Debug)]
pub struct Node<'a> {
    value: Value<'a>,
    raw: Option<&'a [u8]>,
}

// Equality compares semantic values, not the provenance/encoding of nodes.
impl PartialEq for Node<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for Node<'_> {}

impl<'a> Node<'a> {
    pub fn new(value: Value<'a>) -> Self {
        Self { value, raw: None }
    }

    pub fn value(&self) -> &Value<'a> {
        &self.value
    }

    /// Original encoding, including noncanonical representation if accepted.
    pub fn raw_bytes(&self) -> Option<&'a [u8]> {
        self.raw
    }

    /// Dictionary lookup. Compatible duplicate keys resolve to the last value.
    pub fn get(&self, key: &[u8]) -> Option<&Node<'a>> {
        match &self.value {
            Value::Dictionary(entries) => entries.get(key),
            _ => None,
        }
    }
}

/// Error location is a byte offset in input, or bytes emitted during encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub offset: usize,
    pub kind: ErrorKind,
}

impl Error {
    fn new(offset: usize, kind: ErrorKind) -> Self {
        Self { offset, kind }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    UnexpectedEnd,
    InvalidToken,
    InvalidInteger,
    IntegerOverflow,
    InvalidStringLength,
    StringLengthOverflow,
    DictionaryKey,
    TrailingBytes,
    InputLimit,
    StringLimit,
    OutputLimit,
    NodeLimit,
    DepthLimit,
    InvalidLimits,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bencode {:?} at byte {}", self.kind, self.offset)
    }
}

impl std::error::Error for Error {}
