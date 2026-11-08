//! Bounded v1/v2 magnet inputs. Parsing does not authorize tracker/source access.

use std::{fmt, ops::RangeInclusive};

use crate::protocol::identity::{InfoHashV1, InfoHashV2, TorrentIdentity};

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
    InvalidSelection,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "magnet: {self:?}")
    }
}

impl std::error::Error for Error {}

/// Decoded query data with lowercase names and numeric suffixes removed.
/// Values may be non-UTF-8, including display names. Unknown parameters are
/// retained, not interpreted or acted upon.
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
    info_hash: TorrentIdentity,
    parameters: Vec<Parameter>,
    select_only: Option<Vec<RangeInclusive<u32>>>,
}

impl Magnet {
    /// Require `magnet:?` and key=value pairs separated by `&`. Scheme and
    /// URN prefixes and parameter names are ASCII case-insensitive. Numeric
    /// parameter suffixes are ignored.
    /// Form decoding applies to keys and values: `+` is space, `%2B` is plus.
    /// Raw spaces, controls, non-ASCII and fragments are rejected; escaped
    /// arbitrary bytes are preserved. Empty/trailing pairs are invalid.
    /// Invalid tracker URLs and malformed escapes in tracker/web-seed hints
    /// are skipped without discarding other discovery sources.
    ///
    /// Identical repeated topics are allowed; conflicting hashes of the same
    /// version fail. Btmh accepts hexadecimal SHA2-256 multihashes (`1220`
    /// followed by 64 hex digits). Other well-formed multihashes are unsupported.
    /// Unknown topics are retained but cannot substitute for a known identity.
    /// BEP 53 `so` accepts decimal u32 indices and inclusive ranges. Empty,
    /// malformed and descending selections fail; repeated parameters are unioned.
    /// Ranges stay compact, independent of their span. After metadata resolution,
    /// the engine ignores out-of-range indices and clips ranges to the manifest.
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
        let mut select_only = None::<Vec<RangeInclusive<u32>>>;
        for pair in query.split(|b| *b == b'&') {
            let equals = pair
                .iter()
                .position(|b| *b == b'=')
                .ok_or(Error::InvalidUri)?;
            let mut key = unescape(&pair[..equals], limits.max_component_bytes)?;
            if let Some(dot) = key.iter().position(|b| *b == b'.')
                && key[dot + 1..].iter().all(u8::is_ascii_digit)
            {
                key.truncate(dot);
            }
            key.make_ascii_lowercase();
            if key.is_empty() {
                return Err(Error::InvalidUri);
            }
            let value = match unescape(&pair[equals + 1..], limits.max_component_bytes) {
                Err(Error::InvalidEscape) if key == b"tr" || key == b"ws" => continue,
                result => result?,
            };
            if key == b"tr"
                && !std::str::from_utf8(&value).ok().is_some_and(|value| {
                    url::Url::parse(value).ok().is_some_and(|url| {
                        matches!(url.scheme(), "http" | "https" | "udp")
                            && url.host().is_some()
                            && url.username().is_empty()
                            && url.password().is_none()
                            && url.fragment().is_none()
                            && url.port_or_known_default().is_some_and(|port| port != 0)
                    })
                })
            {
                continue;
            }
            if key == b"so" {
                let ranges = select_only.get_or_insert_with(Vec::new);
                for token in value.split(|&b| b == b',') {
                    let mut ends = token.split(|&b| b == b'-');
                    let start = file_index(ends.next().expect("one token"))?;
                    let end = ends.next().map(file_index).transpose()?.unwrap_or(start);
                    if ends.next().is_some() || start > end {
                        return Err(Error::InvalidSelection);
                    }
                    ranges.push(start..=end);
                }
            }
            parameters.push(Parameter { key, value });
        }
        if let Some(ranges) = &mut select_only {
            ranges.sort_unstable_by_key(|r| (*r.start(), *r.end()));
            let mut merged: Vec<RangeInclusive<u32>> = Vec::new();
            for range in ranges.drain(..) {
                if let Some(last) = merged.last_mut()
                    && range.start() <= last.end()
                {
                    *last = *last.start()..=(*last.end()).max(*range.end());
                } else {
                    merged.push(range);
                }
            }
            *ranges = merged;
        }
        let mut v1 = None;
        let mut v2 = None;
        for p in &parameters {
            if p.key == b"xt" && prefix(&p.value, b"urn:btih:") {
                let hash = parse_hash(&p.value[9..])?;
                if v1.is_some_and(|previous| previous != hash) {
                    return Err(Error::ConflictingHashes);
                }
                v1 = Some(hash);
            } else if p.key == b"xt" && prefix(&p.value, b"urn:btmh:") {
                let hash = parse_multihash(&p.value[9..])?;
                if v2.is_some_and(|previous| previous != hash) {
                    return Err(Error::ConflictingHashes);
                }
                v2 = Some(hash);
            }
        }
        let info_hash = match (v1, v2) {
            (Some(v1), Some(v2)) => TorrentIdentity::Hybrid(v1, v2),
            (Some(v1), None) => TorrentIdentity::V1(v1),
            (None, Some(v2)) => TorrentIdentity::V2(v2),
            (None, None) => return Err(Error::MissingHash),
        };
        Ok(Self {
            info_hash,
            parameters,
            select_only,
        })
    }

    pub fn info_hash(&self) -> TorrentIdentity {
        self.info_hash
    }

    pub fn parameters(&self) -> &[Parameter] {
        &self.parameters
    }

    /// BEP 53 zero-based file indices, as sorted, non-overlapping inclusive
    /// ranges. `None` means no selection was specified. Padding indices follow
    /// the manifest order but do not select payload.
    pub fn select_only(&self) -> Option<&[RangeInclusive<u32>]> {
        self.select_only.as_deref()
    }

    /// Valid tracker hints, preserving duplicates and link order.
    pub fn trackers(&self) -> impl Iterator<Item = &[u8]> {
        self.parameters
            .iter()
            .filter(|p| p.key == b"tr")
            .map(|p| p.value())
    }

    /// Last display name; all repetitions remain in `parameters`.
    pub fn display_name(&self) -> Option<&[u8]> {
        self.parameters
            .iter()
            .rfind(|p| p.key == b"dn")
            .map(|p| p.value())
    }
}

fn file_index(input: &[u8]) -> Result<u32, Error> {
    if input.is_empty() || !input.iter().all(u8::is_ascii_digit) {
        return Err(Error::InvalidSelection);
    }
    std::str::from_utf8(input)
        .expect("decimal digits")
        .parse()
        .map_err(|_| Error::InvalidSelection)
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

fn parse_multihash(input: &[u8]) -> Result<InfoHashV2, Error> {
    if !input.len().is_multiple_of(2) {
        return Err(Error::InvalidHash);
    }
    let encoded: Vec<u8> = input
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            Ok(hex(pair[0]).ok_or(Error::InvalidHash)? * 16
                + hex(pair[1]).ok_or(Error::InvalidHash)?)
        })
        .collect::<Result<_, Error>>()?;
    let mut remaining = encoded.as_slice();
    let code = varint(&mut remaining)?;
    let length = varint(&mut remaining)?;
    if length != remaining.len() as u64 {
        return Err(Error::InvalidHash);
    }
    if code != 0x12 || length != 32 {
        return Err(Error::UnsupportedVersion);
    }
    Ok(InfoHashV2::from_bytes(
        remaining.try_into().expect("SHA-256 multihash length"),
    ))
}

fn varint(input: &mut &[u8]) -> Result<u64, Error> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = input.split_first().ok_or(Error::InvalidHash)?;
        *input = rest;
        if shift == 63 && byte > 1 {
            return Err(Error::InvalidHash);
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            if shift != 0 && byte == 0 {
                return Err(Error::InvalidHash);
            }
            return Ok(value);
        }
    }
    Err(Error::InvalidHash)
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
