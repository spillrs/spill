//! uTP v1 datagrams (BEP 29). Unknown extensions are skipped, not echoed.
use std::fmt;

pub const HEADER_LEN: usize = 20;
pub const MAX_EXTENSIONS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Data = 0,
    Fin = 1,
    State = 2,
    Reset = 3,
    Syn = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub kind: Kind,
    pub connection_id: u16,
    pub timestamp_us: u32,
    pub timestamp_diff_us: u32,
    pub wnd_size: u32,
    pub seq_nr: u16,
    pub ack_nr: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Packet<'a> {
    pub header: Header,
    /// Bit zero acknowledges ack_nr + 2; subsequent bits advance in sequence order.
    pub sack: Option<&'a [u8]>,
    pub payload: &'a [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Truncated,
    Version,
    Kind,
    Sack,
    ExtensionLimit,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Truncated => "truncated uTP datagram",
            Self::Version => "unsupported uTP version",
            Self::Kind => "invalid uTP packet type",
            Self::Sack => "invalid uTP selective acknowledgement",
            Self::ExtensionLimit => "too many uTP extensions",
        })
    }
}
impl std::error::Error for Error {}

impl<'a> Packet<'a> {
    pub fn decode(bytes: &'a [u8]) -> Result<Self, Error> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::Truncated);
        }
        if bytes[0] & 0x0f != 1 {
            return Err(Error::Version);
        }
        let kind = match bytes[0] >> 4 {
            0 => Kind::Data,
            1 => Kind::Fin,
            2 => Kind::State,
            3 => Kind::Reset,
            4 => Kind::Syn,
            _ => return Err(Error::Kind),
        };
        let header = Header {
            kind,
            connection_id: u16::from_be_bytes(bytes[2..4].try_into().unwrap()),
            timestamp_us: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            timestamp_diff_us: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
            wnd_size: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
            seq_nr: u16::from_be_bytes(bytes[16..18].try_into().unwrap()),
            ack_nr: u16::from_be_bytes(bytes[18..20].try_into().unwrap()),
        };
        let mut extension = bytes[1];
        let mut offset = HEADER_LEN;
        let mut count = 0;
        let mut sack = None;
        while extension != 0 {
            if count == MAX_EXTENSIONS {
                return Err(Error::ExtensionLimit);
            }
            count += 1;
            let prefix = bytes.get(offset..offset + 2).ok_or(Error::Truncated)?;
            let next = prefix[0];
            let length = usize::from(prefix[1]);
            offset += 2;
            let body = bytes.get(offset..offset + length).ok_or(Error::Truncated)?;
            if extension == 1 {
                if length == 0 || !length.is_multiple_of(4) || sack.is_some() {
                    return Err(Error::Sack);
                }
                sack = Some(body);
            }
            offset += length;
            extension = next;
        }
        Ok(Self {
            header,
            sack,
            payload: &bytes[offset..],
        })
    }

    /// Append a datagram. Locally constructed SACK masks must fit one extension.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let h = self.header;
        out.push((h.kind as u8) << 4 | 1);
        out.push(u8::from(self.sack.is_some()));
        out.extend_from_slice(&h.connection_id.to_be_bytes());
        out.extend_from_slice(&h.timestamp_us.to_be_bytes());
        out.extend_from_slice(&h.timestamp_diff_us.to_be_bytes());
        out.extend_from_slice(&h.wnd_size.to_be_bytes());
        out.extend_from_slice(&h.seq_nr.to_be_bytes());
        out.extend_from_slice(&h.ack_nr.to_be_bytes());
        if let Some(sack) = self.sack {
            assert!(!sack.is_empty() && sack.len() <= 252 && sack.len().is_multiple_of(4));
            out.extend_from_slice(&[0, sack.len() as u8]);
            out.extend_from_slice(sack);
        }
        out.extend_from_slice(self.payload);
    }
}

/// Sequence ordering is defined only within half the sequence space.
pub fn seq_less(a: u16, b: u16) -> bool {
    seq_diff(a, b) < 0
}
pub fn seq_diff(a: u16, b: u16) -> i16 {
    a.wrapping_sub(b) as i16
}
pub fn timestamp_diff(now: u32, then: u32) -> u32 {
    now.wrapping_sub(then)
}
