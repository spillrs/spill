//! Bounded BEP 11 `ut_pex` payloads, not a peer-discovery runtime.

use std::fmt;

use crate::protocol::{
    bencode::{self, Limits, Node, Value},
    compact::{self, PEER_V4_LENGTH, PEER_V6_LENGTH, PeersV4, PeersV6},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Bencode(bencode::Error),
    Compact(compact::Error),
    Dictionary,
    CountOverflow,
    PeerLimit {
        field: &'static str,
        count: usize,
        limit: usize,
    },
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PEX message {self:?}")
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bencode(error) => Some(error),
            Self::Compact(error) => Some(error),
            _ => None,
        }
    }
}

/// A complete payload dictionary with borrowed compact-list views. Unknown
/// fields and malformed optional values survive encoding.
#[derive(Clone, Debug)]
pub struct Message<'a> {
    document: Node<'a>,
    added_v4: PeersV4<'a>,
    added_v6: PeersV6<'a>,
    dropped_v4: PeersV4<'a>,
    dropped_v6: PeersV6<'a>,
}
impl<'a> Message<'a> {
    pub fn parse(
        input: &'a [u8],
        limits: Limits,
        max_added: usize,
        max_dropped: usize,
    ) -> Result<Self, Error> {
        let document = bencode::decode(input, limits).map_err(Error::Bencode)?;
        Self::from_node(document, max_added, max_dropped)
    }

    pub fn from_node(
        document: Node<'a>,
        max_added: usize,
        max_dropped: usize,
    ) -> Result<Self, Error> {
        if !matches!(document.value(), Value::Dictionary(_)) {
            return Err(Error::Dictionary);
        }
        let added = bytes(&document, b"added").unwrap_or_default();
        let added6 = bytes(&document, b"added6").unwrap_or_default();
        let dropped = bytes(&document, b"dropped").unwrap_or_default();
        let dropped6 = bytes(&document, b"dropped6").unwrap_or_default();
        check_count(
            "added",
            added.len() / PEER_V4_LENGTH,
            added6.len() / PEER_V6_LENGTH,
            max_added,
        )?;
        check_count(
            "dropped",
            dropped.len() / PEER_V4_LENGTH,
            dropped6.len() / PEER_V6_LENGTH,
            max_dropped,
        )?;
        let added_v4 = PeersV4::parse(added, max_added).map_err(Error::Compact)?;
        let added_v6 = PeersV6::parse(added6, max_added).map_err(Error::Compact)?;
        let dropped_v4 = PeersV4::parse(dropped, max_dropped).map_err(Error::Compact)?;
        let dropped_v6 = PeersV6::parse(dropped6, max_dropped).map_err(Error::Compact)?;
        Ok(Self {
            document,
            added_v4,
            added_v6,
            dropped_v4,
            dropped_v6,
        })
    }

    pub fn document(&self) -> &Node<'a> {
        &self.document
    }
    pub fn added_v4(&self) -> PeersV4<'a> {
        self.added_v4
    }
    pub fn added_v6(&self) -> PeersV6<'a> {
        self.added_v6
    }
    pub fn dropped_v4(&self) -> PeersV4<'a> {
        self.dropped_v4
    }
    pub fn dropped_v6(&self) -> PeersV6<'a> {
        self.dropped_v6
    }
    pub fn is_empty(&self) -> bool {
        self.added_v4.is_empty()
            && self.added_v6.is_empty()
            && self.dropped_v4.is_empty()
            && self.dropped_v6.is_empty()
    }

    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        bencode::encode(&self.document, limits).map_err(Error::Bencode)
    }
}

fn check_count(field: &'static str, v4: usize, v6: usize, limit: usize) -> Result<(), Error> {
    let count = v4.checked_add(v6).ok_or(Error::CountOverflow)?;
    if count > limit {
        return Err(Error::PeerLimit {
            field,
            count,
            limit,
        });
    }
    Ok(())
}

fn bytes<'a>(document: &Node<'a>, key: &[u8]) -> Option<&'a [u8]> {
    match document.get(key)?.value() {
        Value::Bytes(bytes) => Some(bytes),
        _ => None,
    }
}
