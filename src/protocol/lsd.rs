//! BEP 14 local service discovery announcements.
use std::fmt::Write;

pub const IPV4_HOST: &str = "239.192.152.143:6771";
pub const IPV6_HOST: &str = "[ff15::efc0:988f]:6771";
pub const MAX_PACKET: usize = 1400;

#[derive(Debug, PartialEq, Eq)]
pub struct Announce<'a> {
    pub port: u16,
    pub hashes: Vec<[u8; 20]>,
    pub cookie: Option<&'a str>,
}

/// Ignore unknown headers, but reject malformed or incomplete announcements.
pub fn decode(bytes: &[u8]) -> Option<Announce<'_>> {
    if bytes.len() > MAX_PACKET || !bytes.ends_with(b"\r\n\r\n") {
        return None;
    }
    let (headers, tail) = std::str::from_utf8(bytes).ok()?.split_once("\r\n\r\n")?;
    if !tail.is_empty() && tail != "\r\n" {
        return None;
    }
    let mut lines = headers.split("\r\n");
    if lines.next()? != "BT-SEARCH * HTTP/1.1" {
        return None;
    }
    let (mut host, mut port, mut cookie) = (None, None, None);
    let mut hashes = Vec::new();
    for line in lines {
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("host") {
            if host.replace(value).is_some() || ![IPV4_HOST, IPV6_HOST].contains(&value) {
                return None;
            }
        } else if name.eq_ignore_ascii_case("port") {
            if port.is_some() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            port = Some(value.parse::<u16>().ok().filter(|p| *p != 0)?);
        } else if name.eq_ignore_ascii_case("infohash") {
            if value.len() != 40 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
                return None;
            }
            let mut hash = [0; 20];
            for (byte, pair) in hash.iter_mut().zip(value.as_bytes().as_chunks::<2>().0) {
                *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
            }
            hashes.push(hash);
        } else if name.eq_ignore_ascii_case("cookie") && cookie.replace(value).is_some() {
            return None;
        }
    }
    host?;
    if hashes.is_empty() {
        return None;
    }
    Some(Announce {
        port: port?,
        hashes,
        cookie,
    })
}

/// Callers batch hashes so the resulting datagram fits `MAX_PACKET`.
pub fn encode(ipv6: bool, port: u16, hashes: &[[u8; 20]], cookie: &str) -> Vec<u8> {
    let host = if ipv6 { IPV6_HOST } else { IPV4_HOST };
    let mut packet = format!("BT-SEARCH * HTTP/1.1\r\nHost: {host}\r\nPort: {port}\r\n");
    for hash in hashes {
        packet.push_str("Infohash: ");
        for byte in hash {
            write!(packet, "{byte:02x}").expect("string write");
        }
        packet.push_str("\r\n");
    }
    write!(packet, "cookie: {cookie}\r\n\r\n\r\n").expect("string write");
    assert!(packet.len() <= MAX_PACKET);
    packet.into_bytes()
}
