//! Torrent identities. SHA-1 is used only as mandated by BitTorrent v1,
//! not as a choice for new authentication or security protocols.

use std::fmt;

use sha1::{Digest, Sha1};

/// The SHA-1 identity of a BitTorrent v1 info dictionary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InfoHashV1([u8; 20]);

impl InfoHashV1 {
    /// Hash the exact original bencoded info bytes, never a re-encoded value.
    ///
    /// This computes identity only; it does not validate the info dictionary.
    pub fn from_info_bytes(original_info_bytes: &[u8]) -> Self {
        Self(Sha1::digest(original_info_bytes).into())
    }

    /// Construct from an existing binary identity, e.g. a peer handshake.
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

impl fmt::Display for InfoHashV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
