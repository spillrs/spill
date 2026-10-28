//! Runtime peer identity generation; metainfo identity remains in protocol.
use std::io;

/// Generate a fresh 20-byte peer ID from OS entropy and the single client prefix.
/// This is not authentication or a persistent tracker/accounting lifetime.
pub(in crate::engine) fn generate_peer_id() -> io::Result<[u8; 20]> {
    let mut id = [0; 20];
    id[..8].copy_from_slice(b"-sP0000-");
    for (slot, version) in id[3..6].iter_mut().zip([
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR"),
        env!("CARGO_PKG_VERSION_PATCH"),
    ]) {
        let value: u8 = version.parse().expect("numeric package version");
        *slot = match value {
            0..=9 => b'0' + value,
            10..=35 => b'A' + value - 10,
            _ => panic!("peer ID version component exceeds base 36"),
        };
    }
    getrandom::getrandom(&mut id[8..]).map_err(io::Error::other)?;
    Ok(id)
}

pub(in crate::engine) fn entropy<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    getrandom::getrandom(&mut bytes).map_err(io::Error::other)?;
    Ok(bytes)
}
