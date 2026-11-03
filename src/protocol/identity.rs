//! Torrent identities. SHA-1 is used only as mandated by BitTorrent v1,
//! not as a choice for new authentication or security protocols.

use std::fmt;

use sha1::{Digest, Sha1};
use sha2::Sha256;

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

/// The full SHA-256 identity of a BitTorrent v2 info dictionary.
///
/// A truncated wire key is not sufficient to authenticate acquired metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InfoHashV2([u8; 32]);

impl InfoHashV2 {
    /// Hash original info bytes. This does not validate metainfo or piece layers.
    pub fn from_info_bytes(original_info_bytes: &[u8]) -> Self {
        Self(Sha256::digest(original_info_bytes).into())
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The prefix used by peer handshakes, trackers and the Mainline DHT.
    pub fn wire_key(self) -> [u8; 20] {
        self.0[..20].try_into().expect("20-byte prefix")
    }
}

impl fmt::Display for InfoHashV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Known info-dictionary identities, not proof that content has been validated.
/// Display uses v1 for a hybrid and the full hash for v2-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TorrentIdentity {
    V1(InfoHashV1),
    V2(InfoHashV2),
    Hybrid(InfoHashV1, InfoHashV2),
}

impl TorrentIdentity {
    pub const fn v1(self) -> Option<InfoHashV1> {
        match self {
            Self::V1(hash) | Self::Hybrid(hash, _) => Some(hash),
            Self::V2(_) => None,
        }
    }

    pub const fn v2(self) -> Option<InfoHashV2> {
        match self {
            Self::V2(hash) | Self::Hybrid(_, hash) => Some(hash),
            Self::V1(_) => None,
        }
    }

    /// Primary wire key: v1 for a hybrid, SHA-256 prefix for v2-only.
    pub fn wire_key(self) -> [u8; 20] {
        match self {
            Self::V1(hash) | Self::Hybrid(hash, _) => *hash.as_bytes(),
            Self::V2(hash) => hash.wire_key(),
        }
    }

    pub(crate) fn wire_keys(self) -> impl Iterator<Item = [u8; 20]> {
        let primary = self.wire_key();
        [
            Some(primary),
            self.v2()
                .map(InfoHashV2::wire_key)
                .filter(|key| *key != primary),
        ]
        .into_iter()
        .flatten()
    }

    /// Verify every known identity against the exact acquired info bytes.
    /// Does not validate the info dictionary or authenticate outer piece layers.
    pub fn matches_info_bytes(self, original_info_bytes: &[u8]) -> bool {
        self.v1()
            .is_none_or(|hash| hash == InfoHashV1::from_info_bytes(original_info_bytes))
            && self
                .v2()
                .is_none_or(|hash| hash == InfoHashV2::from_info_bytes(original_info_bytes))
    }
}

impl fmt::Display for TorrentIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V1(hash) | Self::Hybrid(hash, _) => hash.fmt(f),
            Self::V2(hash) => hash.fmt(f),
        }
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
