//! BEP 55 `ut_holepunch` payloads, independent of rendezvous policy.

use std::{fmt, net::SocketAddr};

use crate::protocol::compact;

pub const NO_SUCH_PEER: u32 = 1;
pub const NOT_CONNECTED: u32 = 2;
pub const NO_SUPPORT: u32 = 3;
pub const NO_SELF: u32 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Rendezvous,
    Connect,
    /// Unknown remote error codes remain representable.
    Error(u32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Length,
    MessageType(u8),
    AddressType(u8),
    Compact(compact::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "holepunch message {self:?}")
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
pub struct Message {
    pub kind: Kind,
    pub addr: SocketAddr,
}
impl Message {
    /// Parse one complete payload. Non-error messages also accept libtorrent's
    /// short form without the four-byte error field.
    pub fn parse(input: &[u8]) -> Result<Self, Error> {
        let [message_type, address_type, ..] = *input else {
            return Err(Error::Length);
        };
        let endpoint_end = match address_type {
            0 => 8,
            1 => 20,
            other => return Err(Error::AddressType(other)),
        };
        let kind = match message_type {
            0 => Kind::Rendezvous,
            1 => Kind::Connect,
            2 => Kind::Error(0),
            other => return Err(Error::MessageType(other)),
        };
        let full = input.len() == endpoint_end + 4;
        if !full && (input.len() != endpoint_end || message_type == 2) {
            return Err(Error::Length);
        }
        let addr = match address_type {
            0 => {
                SocketAddr::V4(compact::decode_v4(&input[2..endpoint_end]).map_err(Error::Compact)?)
            }
            1 => {
                SocketAddr::V6(compact::decode_v6(&input[2..endpoint_end]).map_err(Error::Compact)?)
            }
            _ => unreachable!(),
        };
        let kind = if message_type == 2 {
            Kind::Error(u32::from_be_bytes(
                input[endpoint_end..].try_into().expect("four-byte error"),
            ))
        } else {
            kind
        };
        Ok(Self { kind, addr })
    }

    /// Emit the BEP 55 form, including a zero error field for non-error messages.
    /// Scoped IPv6 endpoints cannot be represented on the wire.
    pub fn encode(self) -> Result<Vec<u8>, Error> {
        let (message_type, code) = match self.kind {
            Kind::Rendezvous => (0, 0),
            Kind::Connect => (1, 0),
            Kind::Error(code) => (2, code),
        };
        let mut output = Vec::with_capacity(24);
        output.push(message_type);
        match self.addr {
            SocketAddr::V4(addr) => {
                output.push(0);
                output.extend_from_slice(&compact::encode_v4(addr));
            }
            SocketAddr::V6(addr) => {
                output.push(1);
                output.extend_from_slice(&compact::encode_v6(addr).map_err(Error::Compact)?);
            }
        }
        output.extend_from_slice(&code.to_be_bytes());
        Ok(output)
    }
}
