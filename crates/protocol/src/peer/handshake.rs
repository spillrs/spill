use std::fmt;

const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";

/// Length of a complete standard BitTorrent peer handshake.
pub const HANDSHAKE_LEN: usize = 68;

/// A standard peer handshake, without torrent lookup or connection policy.
///
/// Reserved bits are opaque and preserved, including unknown capabilities.
/// `info_hash` is the 20-byte wire identity, not a computed metainfo hash.
/// Matching it and interpreting negotiated capabilities belong to the engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

/// Failure to decode a standard peer handshake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandshakeError {
    /// More bytes are needed; `additional` is relative to the supplied input.
    Incomplete { additional: usize },
    /// The advertised protocol identifier length is not 19.
    ProtocolLength(u8),
    /// The protocol identifier is not `BitTorrent protocol`.
    ProtocolIdentifier,
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete { additional } => write!(f, "handshake needs {additional} more bytes"),
            Self::ProtocolLength(length) => write!(f, "unexpected protocol length {length}"),
            Self::ProtocolIdentifier => f.write_str("unexpected protocol identifier"),
        }
    }
}

impl std::error::Error for HandshakeError {}

impl Handshake {
    /// Encode exactly one handshake. No buffer allocation or I/O is performed.
    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut output = [0; HANDSHAKE_LEN];
        output[0] = 19;
        output[1..20].copy_from_slice(PROTOCOL);
        output[20..28].copy_from_slice(&self.reserved);
        output[28..48].copy_from_slice(&self.info_hash);
        output[48..68].copy_from_slice(&self.peer_id);
        output
    }

    /// Decode the first handshake and return its consumed length.
    ///
    /// Trailing bytes are untouched and may contain peer messages. Incomplete
    /// input is reported before header validation; there is no streaming buffer
    /// in this codec. Established-client conformance checks are still pending.
    pub fn decode(input: &[u8]) -> Result<(Self, usize), HandshakeError> {
        if input.len() < HANDSHAKE_LEN {
            return Err(HandshakeError::Incomplete {
                additional: HANDSHAKE_LEN - input.len(),
            });
        }
        if input[0] != 19 {
            return Err(HandshakeError::ProtocolLength(input[0]));
        }
        if &input[1..20] != PROTOCOL {
            return Err(HandshakeError::ProtocolIdentifier);
        }

        let mut reserved = [0; 8];
        let mut info_hash = [0; 20];
        let mut peer_id = [0; 20];
        reserved.copy_from_slice(&input[20..28]);
        info_hash.copy_from_slice(&input[28..48]);
        peer_id.copy_from_slice(&input[48..68]);
        Ok((
            Self {
                reserved,
                info_hash,
                peer_id,
            },
            HANDSHAKE_LEN,
        ))
    }
}
