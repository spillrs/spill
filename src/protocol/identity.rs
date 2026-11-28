//! Torrent identities. SHA-1 is used only as mandated by BitTorrent v1,
//! not as a choice for new authentication or security protocols.

use std::{fmt, str::FromStr};

use sha1::{Digest, Sha1};
use sha2::Sha256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidInfoHash;

impl fmt::Display for InvalidInfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid hex info hash")
    }
}

impl std::error::Error for InvalidInfoHash {}

fn decode_hex<const N: usize>(s: &str) -> Result<[u8; N], InvalidInfoHash> {
    if s.len() != N * 2 {
        return Err(InvalidInfoHash);
    }
    let mut out = [0u8; N];
    let (chunks, _) = s.as_bytes().as_chunks::<2>();
    for (slot, chunk) in out.iter_mut().zip(chunks) {
        let hi = hex_nibble(chunk[0]).ok_or(InvalidInfoHash)?;
        let lo = hex_nibble(chunk[1]).ok_or(InvalidInfoHash)?;
        *slot = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// The SHA-1 identity of a BitTorrent v1 info dictionary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl FromStr for InfoHashV1 {
    type Err = InvalidInfoHash;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode_hex(s).map(Self)
    }
}

impl FromStr for InfoHashV2 {
    type Err = InvalidInfoHash;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode_hex(s).map(Self)
    }
}

impl FromStr for TorrentIdentity {
    type Err = InvalidInfoHash;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.len() {
            40 => s.parse().map(Self::V1),
            64 => s.parse().map(Self::V2),
            _ => Err(InvalidInfoHash),
        }
    }
}
