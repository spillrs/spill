//! BEP 15 datagrams only. No sockets, random IDs, retries, expiry or matching.
//! Callers must match source endpoint and transaction/action,
//! choose the transport's address family and reject truncated UDP receives.
//! Successful parsing does not authorize dialing or certify tracker success.

use std::{fmt, net::Ipv4Addr};

use crate::{
    compact::{self, PeersV4, PeersV6},
    identity::InfoHashV1,
};

pub const PROTOCOL_ID: u64 = 0x41727101980;
pub const EVENT_NONE: u32 = 0;
pub const EVENT_COMPLETED: u32 = 1;
pub const EVENT_STARTED: u32 = 2;
pub const EVENT_STOPPED: u32 = 3;

/// Explicit resource policy, not an MTU/UDP delivery guarantee. Counts include
/// duplicate and uninteresting complete records; bytes include every tail.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_packet_len: usize,
    pub max_peers: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    PacketLimit,
    Truncated,
    ProtocolId,
    KnownAction,
    LengthOverflow,
    Compact(compact::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UDP tracker: {self:?}")
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Compact(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    V4,
    V6,
}

/// Fixed announce fields, without runtime defaults/adjusted counters. Event is
/// an opaque u32 (known values above); num_want is signed (-1 means default).
/// Malformed negative num_want values are preserved, not endorsed for sending.
/// For IPv6 transport, BEP 15 requires ip=UNSPECIFIED; this structural type does
/// not infer transport policy or rewrite a supplied IPv4 override.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Announce {
    pub info_hash: InfoHashV1,
    pub peer_id: [u8; 20],
    pub downloaded: u64,
    pub left: u64,
    pub uploaded: u64,
    pub event: u32,
    pub ip: Ipv4Addr,
    pub key: u32,
    pub num_want: i32,
    pub port: u16,
}

#[derive(Clone, Copy, Debug)]
pub struct Request<'a> {
    pub connection_id: u64,
    pub transaction_id: u32,
    pub kind: RequestKind<'a>,
}
#[derive(Clone, Copy, Debug)]
pub enum RequestKind<'a> {
    Connect {
        extensions: &'a [u8],
    },
    Announce {
        announce: Announce,
        extensions: &'a [u8],
    },
    Unknown {
        action: u32,
        payload: &'a [u8],
    },
}

impl<'a> Request<'a> {
    pub fn action(&self) -> u32 {
        match self.kind {
            RequestKind::Connect { .. } => 0,
            RequestKind::Announce { .. } => 1,
            RequestKind::Unknown { action, .. } => action,
        }
    }
    /// Decode a complete bounded datagram, borrowing unknown/extension bytes.
    /// Connect validates the magic protocol ID; IDs/counters otherwise retain
    /// all bits, including zero.
    pub fn decode(input: &'a [u8], limits: Limits) -> Result<Self, Error> {
        packet_limit(input.len(), limits)?;
        let mut r = Reader(input);
        let connection_id = r.u64()?;
        let action = r.u32()?;
        let transaction_id = r.u32()?;
        let kind = match action {
            0 => {
                if connection_id != PROTOCOL_ID {
                    return Err(Error::ProtocolId);
                }
                RequestKind::Connect { extensions: r.0 }
            }
            1 => RequestKind::Announce {
                announce: Announce {
                    info_hash: InfoHashV1::from_bytes(r.array()?),
                    peer_id: r.array()?,
                    downloaded: r.u64()?,
                    left: r.u64()?,
                    uploaded: r.u64()?,
                    event: r.u32()?,
                    ip: Ipv4Addr::from(r.array::<4>()?),
                    key: r.u32()?,
                    num_want: i32::from_be_bytes(r.array()?),
                    port: u16::from_be_bytes(r.array()?),
                },
                extensions: r.0,
            },
            action => RequestKind::Unknown {
                action,
                payload: r.0,
            },
        };
        Ok(Self {
            connection_id,
            transaction_id,
            kind,
        })
    }
    /// Validate caps before allocating output; emit the original borrowed tails
    /// unchanged. This is not an outbound extension/num_want/IP sanitizer.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let payload = match self.kind {
            RequestKind::Connect { extensions } => {
                if self.connection_id != PROTOCOL_ID {
                    return Err(Error::ProtocolId);
                }
                extensions.len()
            }
            RequestKind::Announce { extensions, .. } => 82usize
                .checked_add(extensions.len())
                .ok_or(Error::LengthOverflow)?,
            RequestKind::Unknown { action, payload } => {
                if action <= 1 {
                    return Err(Error::KnownAction);
                }
                payload.len()
            }
        };
        let length = 16usize.checked_add(payload).ok_or(Error::LengthOverflow)?;
        packet_limit(length, limits)?;
        let mut out = Vec::with_capacity(length);
        out.extend(self.connection_id.to_be_bytes());
        out.extend(self.action().to_be_bytes());
        out.extend(self.transaction_id.to_be_bytes());
        match self.kind {
            RequestKind::Connect { extensions } => out.extend_from_slice(extensions),
            RequestKind::Announce {
                announce: a,
                extensions,
            } => {
                out.extend(a.info_hash.as_bytes());
                out.extend(a.peer_id);
                out.extend(a.downloaded.to_be_bytes());
                out.extend(a.left.to_be_bytes());
                out.extend(a.uploaded.to_be_bytes());
                out.extend(a.event.to_be_bytes());
                out.extend(a.ip.octets());
                out.extend(a.key.to_be_bytes());
                out.extend(a.num_want.to_be_bytes());
                out.extend(a.port.to_be_bytes());
                out.extend_from_slice(extensions);
            }
            RequestKind::Unknown { payload, .. } => out.extend_from_slice(payload),
        }
        Ok(out)
    }
}

/// Address family is supplied by the actual UDP transport, never inferred from
/// payload divisibility. Views preserve duplicates, endpoints and partial tails.
#[derive(Clone, Copy, Debug)]
pub enum Peers<'a> {
    V4(PeersV4<'a>),
    V6(PeersV6<'a>),
}
impl<'a> Peers<'a> {
    pub fn as_bytes(&self) -> &'a [u8] {
        match self {
            Self::V4(peers) => peers.as_bytes(),
            Self::V6(peers) => peers.as_bytes(),
        }
    }
    fn check(&self, max_peers: usize) -> Result<(), Error> {
        match self {
            Self::V4(peers) => {
                PeersV4::parse(peers.as_bytes(), max_peers).map_err(Error::Compact)?;
            }
            Self::V6(peers) => {
                PeersV6::parse(peers.as_bytes(), max_peers).map_err(Error::Compact)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Response<'a> {
    pub transaction_id: u32,
    pub kind: ResponseKind<'a>,
}
#[derive(Clone, Copy, Debug)]
pub enum ResponseKind<'a> {
    Connect {
        connection_id: u64,
        extensions: &'a [u8],
    },
    Announce {
        interval: u32,
        leechers: u32,
        seeders: u32,
        peers: Peers<'a>,
    },
    Error(&'a [u8]),
    Unknown {
        action: u32,
        payload: &'a [u8],
    },
}
impl<'a> Response<'a> {
    pub fn action(&self) -> u32 {
        match self.kind {
            ResponseKind::Connect { .. } => 0,
            ResponseKind::Announce { .. } => 1,
            ResponseKind::Error(_) => 3,
            ResponseKind::Unknown { action, .. } => action,
        }
    }
    /// Decode one whole datagram. Error messages are arbitrary borrowed bytes,
    /// including empty/non-UTF-8 data. A parsed error is not tracker success.
    pub fn decode(input: &'a [u8], family: Family, limits: Limits) -> Result<Self, Error> {
        packet_limit(input.len(), limits)?;
        let mut r = Reader(input);
        let action = r.u32()?;
        let transaction_id = r.u32()?;
        let kind = match action {
            0 => ResponseKind::Connect {
                connection_id: r.u64()?,
                extensions: r.0,
            },
            1 => ResponseKind::Announce {
                interval: r.u32()?,
                leechers: r.u32()?,
                seeders: r.u32()?,
                peers: match family {
                    Family::V4 => {
                        Peers::V4(PeersV4::parse(r.0, limits.max_peers).map_err(Error::Compact)?)
                    }
                    Family::V6 => {
                        Peers::V6(PeersV6::parse(r.0, limits.max_peers).map_err(Error::Compact)?)
                    }
                },
            },
            3 => ResponseKind::Error(r.0),
            action => ResponseKind::Unknown {
                action,
                payload: r.0,
            },
        };
        Ok(Self {
            transaction_id,
            kind,
        })
    }
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let payload = match self.kind {
            ResponseKind::Connect { extensions, .. } => 8usize
                .checked_add(extensions.len())
                .ok_or(Error::LengthOverflow)?,
            ResponseKind::Announce { peers, .. } => {
                peers.check(limits.max_peers)?;
                12usize
                    .checked_add(peers.as_bytes().len())
                    .ok_or(Error::LengthOverflow)?
            }
            ResponseKind::Error(message) => message.len(),
            ResponseKind::Unknown { action, payload } => {
                if matches!(action, 0 | 1 | 3) {
                    return Err(Error::KnownAction);
                }
                payload.len()
            }
        };
        let length = 8usize.checked_add(payload).ok_or(Error::LengthOverflow)?;
        packet_limit(length, limits)?;
        let mut out = Vec::with_capacity(length);
        out.extend(self.action().to_be_bytes());
        out.extend(self.transaction_id.to_be_bytes());
        match self.kind {
            ResponseKind::Connect {
                connection_id,
                extensions,
            } => {
                out.extend(connection_id.to_be_bytes());
                out.extend_from_slice(extensions);
            }
            ResponseKind::Announce {
                interval,
                leechers,
                seeders,
                peers,
            } => {
                out.extend(interval.to_be_bytes());
                out.extend(leechers.to_be_bytes());
                out.extend(seeders.to_be_bytes());
                out.extend_from_slice(peers.as_bytes());
            }
            ResponseKind::Error(message) => out.extend_from_slice(message),
            ResponseKind::Unknown { payload, .. } => out.extend_from_slice(payload),
        }
        Ok(out)
    }
}

fn packet_limit(length: usize, limits: Limits) -> Result<(), Error> {
    if length > limits.max_packet_len {
        Err(Error::PacketLimit)
    } else if length > isize::MAX as usize {
        // Vec's maximum representable byte capacity, including on 32-bit hosts.
        Err(Error::LengthOverflow)
    } else {
        Ok(())
    }
}
struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let bytes = self.0.get(..N).ok_or(Error::Truncated)?;
        let value = bytes.try_into().expect("checked field length");
        self.0 = &self.0[N..];
        Ok(value)
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(self.array()?))
    }
}
