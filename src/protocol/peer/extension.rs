//! BEP 10 handshake dictionaries and bounded, additive extension-ID maps.
//!
//! Only extension-id zero payloads should be passed to `Handshake::parse`.
//! Unknown extension payloads stay opaque in `message::Message::Extended`.
//! These types do not advertise capabilities or implement a connection runtime.

use std::{collections::BTreeMap, fmt};

use crate::protocol::bencode::{self, Limits, Node, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Bencode(bencode::Error),
    Dictionary,
    ConflictingId(u8),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "extension handshake {self:?}")
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

/// A handshake or handshake update. All BEP 10 fields are optional. Unknown
/// fields remain in the document. Malformed optional field values are ignored
/// by typed accessors, not allowed to wrap/truncate integers or trigger work.
#[derive(Clone, Debug)]
pub struct Handshake<'a> {
    document: Node<'a>,
}

impl<'a> Handshake<'a> {
    /// Parse one complete bounded bencoded dictionary, not a peer frame. The
    /// caller must check BEP 10 negotiation and extended message id=0 first.
    pub fn parse(input: &'a [u8], limits: Limits) -> Result<Self, Error> {
        Self::from_node(bencode::decode(input, limits).map_err(Error::Bencode)?)
    }

    /// Construct from fields using the existing raw bencode API. This retains
    /// unknown fields and supports outbound dictionaries without a second AST.
    pub fn from_node(document: Node<'a>) -> Result<Self, Error> {
        if !matches!(document.value(), Value::Dictionary(_)) {
            return Err(Error::Dictionary);
        }
        Ok(Self { document })
    }

    pub fn document(&self) -> &Node<'a> {
        &self.document
    }

    /// Canonical encoding; not necessarily identical to the original bytes.
    /// Optional fields are retained verbatim semantically, even if a typed
    /// accessor ignores their malformed value. No capability is added for you.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        bencode::encode(&self.document, limits).map_err(Error::Bencode)
    }

    /// Entries with valid one-byte integer IDs, including 0 (disable). Missing,
    /// wrong-typed or out-of-range values are ignored. Names are case-sensitive
    /// bytes; recognition is the caller's job, not a global extension registry.
    pub fn extensions(&self) -> impl Iterator<Item = (&'a [u8], u8)> + '_ {
        self.document
            .get(b"m")
            .and_then(|node| match node.value() {
                Value::Dictionary(entries) => Some(entries),
                _ => None,
            })
            .into_iter()
            .flat_map(|entries| entries.iter())
            .filter_map(|(&name, node)| Some((name, u8::try_from(integer(node)?).ok()?)))
    }

    /// A patch value: Some(0) explicitly disables; None leaves an existing ID
    /// unchanged when applying an update. Do not confuse these two cases.
    pub fn extension_id(&self, name: &[u8]) -> Option<u8> {
        u8::try_from(integer(self.document.get(b"m")?.get(name)?)?).ok()
    }

    /// Optional untrusted hints. Zero is retained; no sockets, queues or
    /// metadata buffers are created from these values by this module.
    pub fn port(&self) -> Option<u16> {
        u16::try_from(integer(self.document.get(b"p")?)?).ok()
    }
    pub fn request_queue(&self) -> Option<u32> {
        u32::try_from(integer(self.document.get(b"reqq")?)?).ok()
    }
    /// BEP 9 hint only; metadata-message validation is separate.
    pub fn metadata_size(&self) -> Option<u32> {
        u32::try_from(integer(self.document.get(b"metadata_size")?)?).ok()
    }
    /// Raw client string. Invalid UTF-8 does not invalidate the whole handshake.
    pub fn client(&self) -> Option<&'a [u8]> {
        match self.document.get(b"v")?.value() {
            Value::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }
}

/// A small owned name/ID map whose set of names is chosen by the application,
/// never grown by remote handshakes. Zero means disabled, nonzero IDs are unique.
///
/// Use TWO maps per peer: local advertised IDs interpret INCOMING messages;
/// remote advertised IDs select OUTGOING IDs. Receiving a handshake updates
/// only the remote map. This is map bookkeeping, not capability negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtensionMap {
    entries: BTreeMap<Vec<u8>, u8>,
}

impl ExtensionMap {
    /// Application-selected names and initial IDs. Repeated names are last-wins.
    /// Use zero initial IDs for a remote map awaiting its first handshake.
    pub fn new(entries: &[(&[u8], u8)]) -> Result<Self, Error> {
        let entries: BTreeMap<_, _> = entries
            .iter()
            .map(|(name, id)| (name.to_vec(), *id))
            .collect();
        unique_ids(entries.values().copied())?;
        Ok(Self { entries })
    }

    /// Apply an additive patch only to configured names. Unknown names cannot
    /// consume retained memory or disable recognized extensions via collisions.
    /// Missing/invalid entries leave IDs unchanged; zero disables. Colliding
    /// recognized nonzero IDs reject the WHOLE patch without changing this map.
    /// Changes are checked simultaneously, so swaps and disable/reassign work.
    pub fn apply(&mut self, handshake: &Handshake<'_>) -> Result<(), Error> {
        let next: Vec<_> = self
            .entries
            .iter()
            .map(|(name, id)| handshake.extension_id(name).unwrap_or(*id))
            .collect();
        unique_ids(next.iter().copied())?;
        for (id, next) in self.entries.values_mut().zip(next) {
            *id = next;
        }
        Ok(())
    }

    /// For sending, query the REMOTE map. Disabled/unrecognized names yield None.
    pub fn id_for(&self, name: &[u8]) -> Option<u8> {
        self.entries.get(name).copied().filter(|&id| id != 0)
    }
    /// For receiving, query the LOCAL map. ID zero is always the handshake,
    /// never an extension; disabled/unassigned IDs yield None.
    pub fn name_for(&self, id: u8) -> Option<&[u8]> {
        if id == 0 {
            return None;
        }
        self.entries
            .iter()
            .find_map(|(name, &value)| (value == id).then_some(name.as_slice()))
    }

    /// An outbound `m`-only handshake/update borrowing this map's names.
    /// Includes zero entries so explicit disable updates can be sent. Optional
    /// hints can be constructed with `Handshake::from_node` when needed.
    pub fn handshake(&self) -> Handshake<'_> {
        let entries = self
            .entries
            .iter()
            .map(|(name, &id)| (name.as_slice(), Node::new(Value::Integer(i64::from(id)))))
            .collect();
        Handshake {
            document: Node::new(Value::Dictionary(BTreeMap::from([(
                b"m".as_slice(),
                Node::new(Value::Dictionary(entries)),
            )]))),
        }
    }
}

fn integer(node: &Node<'_>) -> Option<i64> {
    match node.value() {
        Value::Integer(value) => Some(*value),
        _ => None,
    }
}
fn unique_ids(ids: impl Iterator<Item = u8>) -> Result<(), Error> {
    let mut used = [false; 256];
    for id in ids.filter(|&id| id != 0) {
        if used[usize::from(id)] {
            return Err(Error::ConflictingId(id));
        }
        used[usize::from(id)] = true;
    }
    Ok(())
}
