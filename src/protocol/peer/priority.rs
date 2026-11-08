//! BEP 40 canonical peer priority. Higher values are preferred within a torrent.
use crate::protocol::crc32c::crc32c;
use std::net::{IpAddr, SocketAddr};

/// Hash a pair of endpoints in network byte order. The local address must be
/// the one visible to the remote peer. Both endpoints must use the same family.
/// Identical IPs use sorted ports instead of masked addresses.
///
/// # Panics
/// Panics if the endpoints use different address families.
pub fn priority(a: SocketAddr, b: SocketAddr) -> u32 {
    if a.ip() == b.ip() {
        let (a, b) = (a.port().min(b.port()), a.port().max(b.port()));
        let mut bytes = [0; 4];
        bytes[..2].copy_from_slice(&a.to_be_bytes());
        bytes[2..].copy_from_slice(&b.to_be_bytes());
        return crc32c(&bytes);
    }
    match (a.ip(), b.ip()) {
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            let mut a = a.octets();
            let mut b = b.octets();
            let unmasked = if a[..2] != b[..2] {
                2
            } else if a[..3] != b[..3] {
                3
            } else {
                4
            };
            for byte in a[unmasked..].iter_mut().chain(&mut b[unmasked..]) {
                *byte &= 0x55;
            }
            let mut bytes = [0; 8];
            bytes[..4].copy_from_slice(&a.min(b));
            bytes[4..].copy_from_slice(&a.max(b));
            crc32c(&bytes)
        }
        (IpAddr::V6(a), IpAddr::V6(b)) => {
            let mut a = a.octets();
            let mut b = b.octets();
            let different = a
                .iter()
                .zip(b)
                .position(|(a, b)| *a != b)
                .expect("different IPs");
            let unmasked = (different + 1).max(6);
            for byte in a[unmasked..].iter_mut().chain(&mut b[unmasked..]) {
                *byte &= 0x55;
            }
            let mut bytes = [0; 32];
            bytes[..16].copy_from_slice(&a.min(b));
            bytes[16..].copy_from_slice(&a.max(b));
            crc32c(&bytes)
        }
        _ => panic!("peer priority requires matching address families"),
    }
}
