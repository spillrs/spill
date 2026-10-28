//! Compact peer endpoints (BEP 23 IPv4, BEP 7 IPv6).

use std::{
    fmt,
    iter::FusedIterator,
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
};

pub const PEER_V4_LENGTH: usize = 6;
pub const PEER_V6_LENGTH: usize = 18;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Length {
        expected: usize,
        actual: usize,
    },
    PeerLimit {
        count: usize,
        limit: usize,
    },
    /// Compact IPv6 has no fields for local interface scope or flow info.
    Ipv6Context {
        scope_id: u32,
        flowinfo: u32,
    },
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "compact address {self:?}")
    }
}
impl std::error::Error for Error {}

/// Decode exactly one six-byte endpoint; list/prefix handling is separate.
pub fn decode_v4(bytes: &[u8]) -> Result<SocketAddrV4, Error> {
    let bytes: &[u8; PEER_V4_LENGTH] = bytes.try_into().map_err(|_| Error::Length {
        expected: PEER_V4_LENGTH,
        actual: bytes.len(),
    })?;
    Ok(SocketAddrV4::new(
        Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]),
        u16::from_be_bytes([bytes[4], bytes[5]]),
    ))
}

pub fn encode_v4(address: SocketAddrV4) -> [u8; PEER_V4_LENGTH] {
    let mut bytes = [0; PEER_V4_LENGTH];
    bytes[..4].copy_from_slice(&address.ip().octets());
    bytes[4..].copy_from_slice(&address.port().to_be_bytes());
    bytes
}

/// Decode exactly one eighteen-byte endpoint. Scope and flow info are zero;
/// neither is present on the wire. Mapped addresses remain IPv6, not IPv4.
pub fn decode_v6(bytes: &[u8]) -> Result<SocketAddrV6, Error> {
    let bytes: &[u8; PEER_V6_LENGTH] = bytes.try_into().map_err(|_| Error::Length {
        expected: PEER_V6_LENGTH,
        actual: bytes.len(),
    })?;
    let ip = Ipv6Addr::from(<[u8; 16]>::try_from(&bytes[..16]).expect("sixteen-byte address"));
    Ok(SocketAddrV6::new(
        ip,
        u16::from_be_bytes([bytes[16], bytes[17]]),
        0,
        0,
    ))
}

/// Refuse to silently discard nonzero scope/flow fields. Applications that
/// deliberately export only IP+port must explicitly construct a zero-context
/// endpoint first; this codec does not choose an interface for scoped addresses.
pub fn encode_v6(address: SocketAddrV6) -> Result<[u8; PEER_V6_LENGTH], Error> {
    if address.scope_id() != 0 || address.flowinfo() != 0 {
        return Err(Error::Ipv6Context {
            scope_id: address.scope_id(),
            flowinfo: address.flowinfo(),
        });
    }
    let mut bytes = [0; PEER_V6_LENGTH];
    bytes[..16].copy_from_slice(&address.ip().octets());
    bytes[16..].copy_from_slice(&address.port().to_be_bytes());
    Ok(bytes)
}

/// Borrowed compact IPv4 list. No endpoint vector is allocated. Budgets count
/// complete records, including duplicate/zero-port entries. Partial tails are
/// accepted for deployed compatibility; strict consumers can reject remainder().
#[derive(Clone, Copy, Debug)]
pub struct PeersV4<'a> {
    bytes: &'a [u8],
}
impl<'a> PeersV4<'a> {
    /// Original complete records and any partial tail, unchanged.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn parse(bytes: &'a [u8], max_peers: usize) -> Result<Self, Error> {
        check_count(bytes.len() / PEER_V4_LENGTH, max_peers)?;
        Ok(Self { bytes })
    }
    pub fn len(&self) -> usize {
        self.bytes.len() / PEER_V4_LENGTH
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Original unconsumed tail, always shorter than a complete record.
    pub fn remainder(&self) -> &'a [u8] {
        &self.bytes[self.len() * PEER_V4_LENGTH..]
    }
    pub fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = SocketAddrV4> + DoubleEndedIterator + FusedIterator + Clone + 'a
    {
        self.bytes
            .as_chunks::<PEER_V4_LENGTH>()
            .0
            .iter()
            .map(|bytes| decode_v4(bytes).expect("six-byte chunk"))
    }
}

/// Borrowed compact IPv6 list, with the same count/tail policy as PeersV4.
#[derive(Clone, Copy, Debug)]
pub struct PeersV6<'a> {
    bytes: &'a [u8],
}
impl<'a> PeersV6<'a> {
    /// Original complete records and any partial tail, unchanged.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn parse(bytes: &'a [u8], max_peers: usize) -> Result<Self, Error> {
        check_count(bytes.len() / PEER_V6_LENGTH, max_peers)?;
        Ok(Self { bytes })
    }
    pub fn len(&self) -> usize {
        self.bytes.len() / PEER_V6_LENGTH
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Original unconsumed tail, always shorter than a complete record.
    pub fn remainder(&self) -> &'a [u8] {
        &self.bytes[self.len() * PEER_V6_LENGTH..]
    }
    pub fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = SocketAddrV6> + DoubleEndedIterator + FusedIterator + Clone + 'a
    {
        self.bytes
            .as_chunks::<PEER_V6_LENGTH>()
            .0
            .iter()
            .map(|bytes| decode_v6(bytes).expect("eighteen-byte chunk"))
    }
}

fn check_count(count: usize, limit: usize) -> Result<(), Error> {
    if count > limit {
        return Err(Error::PeerLimit { count, limit });
    }
    Ok(())
}
