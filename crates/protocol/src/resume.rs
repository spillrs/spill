//! Bounded resume records: original info-hash, exact bitfield and opaque backend
//! fingerprint. A checksum rejects torn/corrupt records; there are no migrations.
use crate::metainfo::OwnedMetainfo;
use sha1::{Digest, Sha1};
use std::io;

const MAGIC: &[u8; 8] = b"SPILLR01";
/// The file backend records 40 bytes per file.
pub const MAX_FILES: usize = 100_000;
const MAX_FINGERPRINT: usize = 40 * MAX_FILES;
/// Last checkpoint's display-only counts; fingerprint validation is still required
/// before trusting any piece for download or upload admission.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResumeProgress {
    pub verified_bytes: u64,
    pub verified_pieces: u32,
}
pub fn resume_progress(meta: &OwnedMetainfo, bytes: &[u8]) -> io::Result<ResumeProgress> {
    let resume = decode(meta, bytes)?;
    let mut progress = ResumeProgress::default();
    for index in 0..meta.layout().piece_count() {
        if resume.verified[index as usize / 8] & (0x80 >> (index % 8)) != 0 {
            let range = meta.layout().piece_range(index).expect("validated piece");
            progress.verified_bytes += range.end - range.start;
            progress.verified_pieces += 1;
        }
    }
    Ok(progress)
}
/// Display-only membership from a checksummed checkpoint. Callers must label
/// this as saved progress: fingerprints have not been checked against storage.
pub fn resume_piece_map(meta: &OwnedMetainfo, bytes: &[u8]) -> io::Result<Vec<bool>> {
    let resume = decode(meta, bytes)?;
    Ok((0..meta.layout().piece_count())
        .map(|index| resume.verified[index as usize / 8] & (0x80 >> (index % 8)) != 0)
        .collect())
}

pub struct Resume {
    pub verified: Vec<u8>,
    pub fingerprint: Vec<u8>,
}
pub fn encode(meta: &OwnedMetainfo, verified: &[u8], fingerprint: &[u8]) -> io::Result<Vec<u8>> {
    if fingerprint.len() > MAX_FINGERPRINT || !valid_bits(meta, verified) {
        return Err(invalid());
    }
    let mut out = Vec::with_capacity(56 + verified.len() + fingerprint.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(meta.info_hash().as_bytes());
    out.extend_from_slice(&meta.layout().piece_count().to_le_bytes());
    out.extend_from_slice(&(fingerprint.len() as u32).to_le_bytes());
    out.extend_from_slice(verified);
    out.extend_from_slice(fingerprint);
    let checksum = Sha1::digest(&out);
    out.extend_from_slice(&checksum);
    Ok(out)
}
pub fn decode(meta: &OwnedMetainfo, bytes: &[u8]) -> io::Result<Resume> {
    let bits = meta.layout().piece_count().div_ceil(8) as usize;
    if bytes.len() < 56 + bits
        || &bytes[..8] != MAGIC
        || &bytes[8..28] != meta.info_hash().as_bytes()
        || u32::from_le_bytes(bytes[28..32].try_into().expect("header"))
            != meta.layout().piece_count()
    {
        return Err(invalid());
    }
    let length = u32::from_le_bytes(bytes[32..36].try_into().expect("header")) as usize;
    if length > MAX_FINGERPRINT
        || bytes.len() != 56 + bits + length
        || Sha1::digest(&bytes[..bytes.len() - 20])[..] != bytes[bytes.len() - 20..]
        || !valid_bits(meta, &bytes[36..36 + bits])
    {
        return Err(invalid());
    }
    Ok(Resume {
        verified: bytes[36..36 + bits].to_vec(),
        fingerprint: bytes[36 + bits..bytes.len() - 20].to_vec(),
    })
}
fn valid_bits(meta: &OwnedMetainfo, bits: &[u8]) -> bool {
    let count = meta.layout().piece_count();
    bits.len() == count.div_ceil(8) as usize
        && (count.is_multiple_of(8)
            || bits
                .last()
                .is_some_and(|b| b & ((1 << (8 - count % 8)) - 1) == 0))
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid resume record")
}
