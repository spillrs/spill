//! Bounded v1 magnet inputs. Parsing does not authorize tracker/source access.

use std::fmt;

use crate::protocol::identity::InfoHashV1;

/// Limits include ignored and repeated parameters. Decoded bytes cannot exceed
/// the input budget; each key and value additionally has its own byte cap.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_input_bytes: usize,
    pub max_parameters: usize,
    pub max_component_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_input_bytes: 16 * 1024,
            max_parameters: 128,
            max_component_bytes: 4096,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidUri,
    InvalidEscape,
    ResourceLimit,
    MissingHash,
    InvalidHash,
    ConflictingHashes,
    UnsupportedVersion,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "magnet: {self:?}")
    }
}

impl std::error::Error for Error {}

/// Immutable decoded query data. Values may be non-UTF-8, including display
/// names. Unknown parameters are retained, not interpreted or acted upon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parameter {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Parameter {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Magnet {
    info_hash: InfoHashV1,
    parameters: Vec<Parameter>,
}

impl Magnet {
    /// Require `magnet:?` and key=value pairs separated by `&`. Scheme and
    /// URN prefixes are ASCII case-insensitive; parameter names are exact.
    /// Form decoding applies to keys and values: `+` is space, `%2B` is plus.
    /// Raw spaces, controls, non-ASCII and fragments are rejected; escaped
    /// arbitrary bytes are preserved. Empty/trailing pairs are invalid.
    ///
    /// Identical repeated btih topics are allowed; conflicting ones fail.
    /// Any btmh topic rejects the entire URI, including hybrids. Unknown
    /// topics are retained but cannot substitute for a v1 identity.
    pub fn parse(input: &str, limits: Limits) -> Result<Self, Error> {
        if input.len() > limits.max_input_bytes {
            return Err(Error::ResourceLimit);
        }
        let bytes = input.as_bytes();
        if bytes.len() < 8
            || !bytes[..8].eq_ignore_ascii_case(b"magnet:?")
            || bytes
                .iter()
                .any(|b| !matches!(b, 0x21..=0x7e) || *b == b'#')
        {
            return Err(Error::InvalidUri);
        }
        let query = &bytes[8..];
        if query.is_empty() {
            return Err(Error::MissingHash);
        }
        // Validate counts before allocating decoded parameters.
        let count = query.split(|b| *b == b'&').count();
        if count > limits.max_parameters {
            return Err(Error::ResourceLimit);
        }
        let mut parameters = Vec::new();
        for pair in query.split(|b| *b == b'&') {
            let equals = pair
                .iter()
                .position(|b| *b == b'=')
                .ok_or(Error::InvalidUri)?;
            let key = unescape(&pair[..equals], limits.max_component_bytes)?;
            if key.is_empty() {
                return Err(Error::InvalidUri);
            }
            let value = unescape(&pair[equals + 1..], limits.max_component_bytes)?;
            parameters.push(Parameter { key, value });
        }
        // Detect unsupported topics before attempting to interpret v1 hashes.
        if parameters
            .iter()
            .any(|p| p.key == b"xt" && prefix(&p.value, b"urn:btmh:"))
        {
            return Err(Error::UnsupportedVersion);
        }
        let mut info_hash = None;
        for p in &parameters {
            if p.key == b"xt" && prefix(&p.value, b"urn:btih:") {
                let hash = parse_hash(&p.value[9..])?;
                if info_hash.is_some_and(|previous| previous != hash) {
                    return Err(Error::ConflictingHashes);
                }
                info_hash = Some(hash);
            }
        }
        Ok(Self {
            info_hash: info_hash.ok_or(Error::MissingHash)?,
            parameters,
        })
    }

    pub fn info_hash(&self) -> InfoHashV1 {
        self.info_hash
    }

    pub fn parameters(&self) -> &[Parameter] {
        &self.parameters
    }

    /// All tracker hints, preserving duplicates/order. No URL validation.
    pub fn trackers(&self) -> impl Iterator<Item = &[u8]> {
        self.parameters
            .iter()
            .filter(|p| p.key == b"tr")
            .map(|p| p.value())
    }

    /// First display name; all repetitions remain in `parameters`.
    pub fn display_name(&self) -> Option<&[u8]> {
        self.parameters
            .iter()
            .find(|p| p.key == b"dn")
            .map(|p| p.value())
    }
}

fn prefix(value: &[u8], prefix: &[u8]) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|v| v.eq_ignore_ascii_case(prefix))
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn unescape(input: &[u8], cap: usize) -> Result<Vec<u8>, Error> {
    let mut output = Vec::new();
    let mut i = 0;
    while i < input.len() {
        if output.len() == cap {
            return Err(Error::ResourceLimit);
        }
        let byte = if input[i] == b'%' {
            let high = input
                .get(i + 1)
                .and_then(|b| hex(*b))
                .ok_or(Error::InvalidEscape)?;
            let low = input
                .get(i + 2)
                .and_then(|b| hex(*b))
                .ok_or(Error::InvalidEscape)?;
            i += 3;
            high * 16 + low
        } else {
            let byte = if input[i] == b'+' { b' ' } else { input[i] };
            i += 1;
            byte
        };
        output.push(byte);
    }
    Ok(output)
}

fn parse_hash(input: &[u8]) -> Result<InfoHashV1, Error> {
    let mut bytes = [0; 20];
    match input.len() {
        40 => {
            for (out, pair) in bytes.iter_mut().zip(input.as_chunks::<2>().0) {
                *out = hex(pair[0]).ok_or(Error::InvalidHash)? * 16
                    + hex(pair[1]).ok_or(Error::InvalidHash)?;
            }
        }
        32 => {
            // Exactly 160 bits: RFC 4648 base32, without padding.
            let mut bits = 0;
            let mut accumulator = 0u16;
            let mut index = 0;
            for b in input {
                let digit = match b.to_ascii_uppercase() {
                    b'A'..=b'Z' => b.to_ascii_uppercase() - b'A',
                    b'2'..=b'7' => b.to_ascii_uppercase() - b'2' + 26,
                    _ => return Err(Error::InvalidHash),
                };
                accumulator = (accumulator << 5) | u16::from(digit);
                bits += 5;
                if bits >= 8 {
                    bits -= 8;
                    bytes[index] = (accumulator >> bits) as u8;
                    index += 1;
                }
                accumulator &= (1 << bits) - 1;
            }
        }
        _ => return Err(Error::InvalidHash),
    }
    Ok(InfoHashV1::from_bytes(bytes))
}
