//! NAT-PMP v0 requests and replies (RFC 6886).
use std::net::Ipv4Addr;

pub fn address_request() -> [u8; 2] {
    [0, 0]
}
pub fn mapping_request(opcode: u8, internal: u16, external: u16, lifetime: u32) -> [u8; 12] {
    let mut bytes = [0; 12];
    bytes[1] = opcode;
    bytes[4..6].copy_from_slice(&internal.to_be_bytes());
    bytes[6..8].copy_from_slice(&external.to_be_bytes());
    bytes[8..12].copy_from_slice(&lifetime.to_be_bytes());
    bytes
}
#[derive(Debug)]
pub enum Reply {
    Address(Ipv4Addr),
    Mapping {
        internal: u16,
        external: u16,
        lifetime: u32,
    },
    Refused(u16),
}
pub fn reply(bytes: &[u8], opcode: u8) -> Option<Reply> {
    if bytes.len() < 8 || bytes[0] != 0 || bytes[1] != opcode + 128 {
        return None;
    }
    let result = u16::from_be_bytes([bytes[2], bytes[3]]);
    if result != 0 {
        return Some(Reply::Refused(result));
    }
    match (opcode, bytes.len()) {
        (0, 12) => Some(Reply::Address(Ipv4Addr::new(
            bytes[8], bytes[9], bytes[10], bytes[11],
        ))),
        (1 | 2, 16) => Some(Reply::Mapping {
            internal: u16::from_be_bytes([bytes[8], bytes[9]]),
            external: u16::from_be_bytes([bytes[10], bytes[11]]),
            lifetime: u32::from_be_bytes(bytes[12..16].try_into().expect("reply length")),
        }),
        _ => None,
    }
}
