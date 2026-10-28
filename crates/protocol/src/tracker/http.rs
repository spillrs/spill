//! Bounded HTTP announce queries and response bodies (BEP 3/7/23).
//! URL parsing/endpoint derivation, HTTP status, decompression/body limits,
//! redirects, DNS and scheduling are transport/engine responsibilities.

use std::{fmt, net::SocketAddr};

use crate::{
    bencode::{self, Limits, Node, Value},
    compact::{self, PEER_V4_LENGTH, PEER_V6_LENGTH, PeersV4, PeersV6},
    identity::InfoHashV1,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Bencode(bencode::Error),
    Compact(compact::Error),
    Dictionary,
    FailureReasonType,
    PeerLimit,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP tracker response: {self:?}")
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

/// A dictionary peer's untrusted host bytes, port and optional exact-length ID.
/// Hostnames are retained without DNS or hostname/IDNA validation. Invalid
/// optional peer IDs are ignored, with original values in the raw document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DictionaryPeer<'a> {
    pub host: &'a [u8],
    pub port: u16,
    pub peer_id: Option<&'a [u8; 20]>,
}

impl DictionaryPeer<'_> {
    /// Convert a numeric IP literal only. Does not resolve a hostname or imply
    /// permission to dial. Unspecified addresses and port zero are preserved.
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        let ip = std::str::from_utf8(self.host).ok()?.parse().ok()?;
        Some(SocketAddr::new(ip, self.port))
    }
}

/// One complete response dictionary, with borrowed peers/strings. A tracker
/// failure is a valid response, not a codec error, and exposes no candidates.
#[derive(Clone, Debug)]
pub struct AnnounceResponse<'a> {
    document: Node<'a>,
    failure_reason: Option<&'a [u8]>,
    peers_v4: PeersV4<'a>,
    peers_v6: PeersV6<'a>,
}

impl<'a> AnnounceResponse<'a> {
    /// The peer cap combines both families and counts every entry in the
    /// selected dictionary list (even malformed entries), duplicates and
    /// zero-port compact peers. Last-wins duplicate fields use bencode policy;
    /// the bencode budgets still cover overwritten values and ignored fields.
    /// Partial compact tails remain available through each view's remainder().
    ///
    /// Missing/wrong-typed peer fields are empty. Missing/invalid numeric hints
    /// are None, not defaults or scheduling instructions. This permits stopped
    /// replies and inspection of incomplete responses; it does not certify a
    /// conforming successful announce. Failure reason must be a byte
    /// string; it takes precedence over peer semantics, but the entire body is
    /// still decoded under all bencode budgets, including ignored fields.
    pub fn parse(input: &'a [u8], limits: Limits, max_peers: usize) -> Result<Self, Error> {
        let document = bencode::decode(input, limits).map_err(Error::Bencode)?;
        if !matches!(document.value(), Value::Dictionary(_)) {
            return Err(Error::Dictionary);
        }
        let failure_reason = match document.get(b"failure reason") {
            Some(node) => Some(match node.value() {
                Value::Bytes(bytes) => *bytes,
                _ => return Err(Error::FailureReasonType),
            }),
            None => None,
        };
        let mut v4 = &b""[..];
        let mut v6 = &b""[..];
        if failure_reason.is_none() {
            let peers = document.get(b"peers");
            let peers6 = bytes(&document, b"peers6");
            v4 = bytes(&document, b"peers").unwrap_or_default();
            v6 = peers6.unwrap_or_default();
            let count = match peers.map(Node::value) {
                Some(Value::List(entries)) => entries.len(),
                _ => v4.len() / PEER_V4_LENGTH,
            };
            if count
                .checked_add(v6.len() / PEER_V6_LENGTH)
                .is_none_or(|count| count > max_peers)
            {
                return Err(Error::PeerLimit);
            }
        }
        let peers_v4 = PeersV4::parse(v4, max_peers).map_err(Error::Compact)?;
        let peers_v6 = PeersV6::parse(v6, max_peers).map_err(Error::Compact)?;
        Ok(Self {
            document,
            failure_reason,
            peers_v4,
            peers_v6,
        })
    }

    pub fn document(&self) -> &Node<'a> {
        &self.document
    }

    pub fn failure_reason(&self) -> Option<&'a [u8]> {
        self.failure_reason
    }

    pub fn warning_message(&self) -> Option<&'a [u8]> {
        bytes(&self.document, b"warning message")
    }

    pub fn tracker_id(&self) -> Option<&'a [u8]> {
        bytes(&self.document, b"tracker id")
    }

    /// Checked, untrusted seconds, including zero; no runtime defaults/clamps.
    pub fn interval_secs(&self) -> Option<u64> {
        number(&self.document, b"interval")
    }

    pub fn min_interval_secs(&self) -> Option<u64> {
        number(&self.document, b"min interval")
    }

    pub fn complete(&self) -> Option<u64> {
        number(&self.document, b"complete")
    }

    pub fn incomplete(&self) -> Option<u64> {
        number(&self.document, b"incomplete")
    }

    pub fn downloaded(&self) -> Option<u64> {
        number(&self.document, b"downloaded")
    }

    pub fn peers_v4(&self) -> PeersV4<'a> {
        self.peers_v4
    }

    pub fn peers_v6(&self) -> PeersV6<'a> {
        self.peers_v6
    }

    /// Entries with a nonempty byte-string ip and a u16 port are yielded in
    /// original order; malformed entries are skipped, but still count against
    /// max_peers. Port zero and arbitrary host bytes are deliberately retained.
    /// Failure responses yield no peers, even if they contain peer fields.
    pub fn dictionary_peers(&self) -> impl Iterator<Item = DictionaryPeer<'a>> + '_ {
        let entries = if self.failure_reason.is_none() {
            match self.document.get(b"peers").map(Node::value) {
                Some(Value::List(entries)) => entries.as_slice(),
                _ => &[],
            }
        } else {
            &[]
        };
        entries.iter().filter_map(|entry| {
            let host = bytes(entry, b"ip").filter(|host| !host.is_empty())?;
            let port = u16::try_from(number(entry, b"port")?).ok()?;
            let peer_id = bytes(entry, b"peer id").and_then(|id| id.try_into().ok());
            Some(DictionaryPeer {
                host,
                port,
                peer_id,
            })
        })
    }
}

/// Explicit announce event; None on a request means a periodic announce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnounceEvent {
    Started,
    Completed,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryError {
    OutputLimit,
    InvalidExistingQuery,
    ConflictingParameter,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP tracker query: {self:?}")
    }
}

impl std::error::Error for QueryError {}

/// Caller-supplied announce facts, not a torrent lifecycle or counter policy.
/// Port zero, full u64 counters, and explicit numwant/key zero are representable.
/// No IP override, random identity/key generation or event inference is added.
#[derive(Clone, Debug)]
pub struct AnnounceRequest<'a> {
    pub info_hash: InfoHashV1,
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: Option<AnnounceEvent>,
    pub numwant: Option<u32>,
    pub key: Option<u32>,
    pub tracker_id: Option<&'a [u8]>,
}

impl AnnounceRequest<'_> {
    /// Build a raw query WITHOUT a leading `?`, not a URL or HTTP request.
    /// `existing_query` is the URL parser's raw query (without `?` or fragment).
    /// Unknown parameters, duplicates and escape spelling are preserved exactly.
    /// Reserved generated keys are rejected even when absent from this request,
    /// including percent-escaped spellings, to avoid ambiguous duplicate fields.
    /// Keys are case-sensitive. Existing query bytes must obey ASCII URI query
    /// syntax with valid percent escapes; empty segments and flag-only keys are
    /// tolerated. This validates query syntax only, not tracker authentication.
    ///
    /// Binary values are escaped ONCE: only ASCII alphanumerics and `-` remain
    /// literal, matching the deployed baseline; all other bytes use uppercase
    /// %HH (including space, plus, NUL and non-UTF-8). compact=1 is always sent;
    /// responses may still be noncompact. Empty tracker IDs are omitted.
    ///
    /// The cap covers preserved query + appended fields. Writes stop at the
    /// cap; no unbounded intermediate encoding is built. Install the result
    /// with a URL library's RAW query setter, not form/query-pair serialization
    /// that would re-escape `%`. URL validation and total URL limits belong to
    /// that caller. Never log this query indiscriminately: it may hold secrets.
    pub fn encode_query(
        &self,
        existing_query: Option<&str>,
        max_output_len: usize,
    ) -> Result<String, QueryError> {
        let mut query = QueryBuffer::new(
            existing_query,
            max_output_len,
            &[
                "info_hash",
                "peer_id",
                "compact",
                "key",
                "trackerid",
                "numwant",
                "port",
                "uploaded",
                "downloaded",
                "left",
                "event",
            ],
        )?;
        query.pair("info_hash", self.info_hash.as_bytes())?;
        query.pair("peer_id", &self.peer_id)?;
        query.pair("compact", b"1")?;
        if let Some(key) = self.key {
            query.pair("key", format!("{key:08x}").as_bytes())?;
        }
        if let Some(id) = self.tracker_id.filter(|id| !id.is_empty()) {
            query.pair("trackerid", id)?;
        }
        if let Some(numwant) = self.numwant {
            query.pair("numwant", numwant.to_string().as_bytes())?;
        }
        query.pair("port", self.port.to_string().as_bytes())?;
        query.pair("uploaded", self.uploaded.to_string().as_bytes())?;
        query.pair("downloaded", self.downloaded.to_string().as_bytes())?;
        query.pair("left", self.left.to_string().as_bytes())?;
        if let Some(event) = self.event {
            query.pair(
                "event",
                match event {
                    AnnounceEvent::Started => b"started",
                    AnnounceEvent::Completed => b"completed",
                    AnnounceEvent::Stopped => b"stopped",
                },
            )?;
        }
        Ok(query.output)
    }
}

struct QueryBuffer {
    output: String,
    cap: usize,
}

impl QueryBuffer {
    fn new(existing: Option<&str>, cap: usize, reserved: &[&str]) -> Result<Self, QueryError> {
        let existing = existing.unwrap_or_default();
        if existing.len() > cap {
            return Err(QueryError::OutputLimit);
        }
        validate_query(existing, reserved)?;
        let mut query = Self {
            output: String::new(),
            cap,
        };
        query.push(existing)?;
        Ok(query)
    }

    fn push(&mut self, text: &str) -> Result<(), QueryError> {
        if text.len() > self.cap - self.output.len() {
            return Err(QueryError::OutputLimit);
        }
        self.output.push_str(text);
        Ok(())
    }

    fn ascii(&mut self, byte: u8) -> Result<(), QueryError> {
        // Only fixed ASCII bytes or a checked ASCII input byte reach this.
        if self.output.len() == self.cap {
            return Err(QueryError::OutputLimit);
        }
        self.output.push(char::from(byte));
        Ok(())
    }

    fn pair(&mut self, key: &str, bytes: &[u8]) -> Result<(), QueryError> {
        if !self.output.is_empty() && !self.output.ends_with('&') {
            self.push("&")?;
        }
        self.push(key)?;
        self.push("=")?;
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        for &byte in bytes {
            if byte.is_ascii_alphanumeric() || byte == b'-' {
                self.ascii(byte)?;
            } else {
                self.ascii(b'%')?;
                self.ascii(HEX[(byte >> 4) as usize])?;
                self.ascii(HEX[(byte & 15) as usize])?;
            }
        }
        Ok(())
    }
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn validate_query(query: &str, reserved_keys: &[&str]) -> Result<(), QueryError> {
    let mut bytes = query.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            for _ in 0..2 {
                if bytes.next().and_then(hex_digit).is_none() {
                    return Err(QueryError::InvalidExistingQuery);
                }
            }
        } else if !byte.is_ascii_alphanumeric() && !b"-._~!$&'()*+,;=:@/?".contains(&byte) {
            return Err(QueryError::InvalidExistingQuery);
        }
    }
    for pair in query.split('&') {
        let key = pair.split_once('=').map_or(pair, |(key, _)| key);
        for reserved in reserved_keys {
            // Decode keys only for comparison, without allocating or rewriting
            // the preserved query. All escapes were validated above.
            let mut bytes = key.bytes();
            let decoded = std::iter::from_fn(|| {
                let byte = bytes.next()?;
                if byte == b'%' {
                    Some(hex_digit(bytes.next()?)? * 16 + hex_digit(bytes.next()?)?)
                } else {
                    Some(byte)
                }
            });
            if decoded.eq(reserved.bytes()) {
                return Err(QueryError::ConflictingParameter);
            }
        }
    }
    Ok(())
}

fn bytes<'a>(document: &Node<'a>, key: &[u8]) -> Option<&'a [u8]> {
    match document.get(key)?.value() {
        Value::Bytes(bytes) => Some(bytes),
        _ => None,
    }
}

fn number(document: &Node<'_>, key: &[u8]) -> Option<u64> {
    match document.get(key)?.value() {
        Value::Integer(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}
