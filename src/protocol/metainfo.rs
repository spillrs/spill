//! Borrowed/owned metainfo and logical ranges. Hash original info bytes;
//! paths are untrusted byte components, not filesystem authority.

use std::{borrow::Cow, fmt, ops::Range};

pub(crate) mod hash;
mod owned;
mod v2;
mod view;
pub use owned::{InfoMetainfo, OwnedMetainfo};
pub use view::MetainfoView;

use crate::protocol::{
    bencode::{self, Limits, Node, Value},
    identity::{InfoHashV1, InfoHashV2, TorrentIdentity},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Bencode(bencode::Error),
    Missing(&'static str),
    InvalidField(&'static str),
    ConflictingFiles,
    EmptyFiles,
    PathConflict,
    LengthOverflow,
    PieceCount,
    MetadataLimit,
    UnsupportedVersion,
    UnsupportedSymlink,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metainfo {self:?}")
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bencode(error) => Some(error),
            _ => None,
        }
    }
}

/// A validated content layout. Tracker/discovery fields remain accessible in
/// `document`; their validation and runtime policy are deliberately separate.
#[derive(Debug)]
pub struct Metainfo<'a> {
    document: Node<'a>,
    info_bytes: &'a [u8],
    info_hash: TorrentIdentity,
    name: &'a [u8],
    private: bool,
    piece_hashes: &'a [u8],
    v2_hashes: Vec<[u8; 32]>,
    layers_pending: bool,
    layout: Layout<'a>,
}

impl<'a> Metainfo<'a> {
    /// Parse one complete .torrent document within the bencode budgets.
    /// Content structure is validated; tracker URLs and other outer extensions
    /// are retained but not validated. No content or filesystem is accessed.
    /// Expanded v2 data-path bytes and piece commitments share `max_input_len`;
    /// expanded path components are also bounded by `max_nodes`.
    pub fn parse(input: &'a [u8], limits: Limits) -> Result<Self, Error> {
        let document = bencode::decode(input, limits).map_err(Error::Bencode)?;
        dictionary(&document, "metainfo")?;
        let info = required(&document, "info")?;
        dictionary(info, "info")?;
        if let Some(version) = info.get(b"meta version") {
            if !matches!(version.value(), Value::Integer(2)) {
                return Err(Error::UnsupportedVersion);
            }
            return Self::v2(document, limits, false);
        }
        if info.get(b"file tree").is_some() || document.get(b"piece layers").is_some() {
            return Err(Error::UnsupportedVersion);
        }
        Self::v1(document)
    }

    fn v1(document: Node<'a>) -> Result<Self, Error> {
        let info = required(&document, "info")?;
        let name = bytes(required(info, "name")?, "name")?;
        component(name)?;
        let piece_length = u32::try_from(integer(required(info, "piece length")?, "piece length")?)
            .ok()
            .filter(|&n| n != 0)
            .ok_or(Error::InvalidField("piece length"))?;
        let piece_hashes = bytes(required(info, "pieces")?, "pieces")?;
        let private = match info.get(b"private") {
            // Match deployed BEP 27 handling: only integer 1 means private.
            Some(node) => matches!(node.value(), Value::Integer(1)),
            None => false,
        };
        let multi_file = info.get(b"files").is_some();
        let mut files = Vec::new();
        let mut total_length = 0u64;
        match (info.get(b"length"), info.get(b"files")) {
            (Some(_), Some(_)) => return Err(Error::ConflictingFiles),
            (Some(_), None) => add_file(
                &mut files,
                &mut total_length,
                info,
                vec![Cow::Borrowed(name)],
            )?,
            (None, Some(list)) => {
                let Value::List(entries) = list.value() else {
                    return Err(Error::InvalidField("files"));
                };
                if entries.is_empty() {
                    return Err(Error::EmptyFiles);
                }
                for entry in entries {
                    dictionary(entry, "files entry")?;
                    let Value::List(parts) = required(entry, "path")?.value() else {
                        return Err(Error::InvalidField("path"));
                    };
                    let mut path = Vec::with_capacity(parts.len());
                    for part in parts {
                        let part = bytes(part, "path")?;
                        component(part)?;
                        path.push(Cow::Borrowed(part));
                    }
                    if path.is_empty() {
                        return Err(Error::InvalidField("path"));
                    }
                    add_file(&mut files, &mut total_length, entry, path)?;
                }
                let mut paths: Vec<_> = files
                    .iter()
                    .filter(|file| file.kind == FileKind::Data)
                    .map(|file| &file.path)
                    .collect();
                paths.sort_unstable();
                if paths.windows(2).any(|pair| pair[1].starts_with(pair[0])) {
                    return Err(Error::PathConflict);
                }
            }
            (None, None) => return Err(Error::Missing("length or files")),
        }
        let count = total_length.div_ceil(u64::from(piece_length));
        let piece_count = u32::try_from(count).map_err(|_| Error::PieceCount)?;
        if piece_hashes.len() % 20 != 0
            || u64::try_from(piece_hashes.len() / 20).ok() != Some(count)
        {
            return Err(Error::PieceCount);
        }
        // Decoded nodes always retain their original span; no encoder involved.
        let info_bytes = info.raw_bytes().expect("decoded info span");
        Ok(Self {
            info_hash: TorrentIdentity::V1(InfoHashV1::from_info_bytes(info_bytes)),
            info_bytes,
            name,
            private,
            piece_hashes,
            v2_hashes: Vec::new(),
            layers_pending: false,
            layout: Layout {
                files,
                total_length,
                piece_length,
                piece_count,
                multi_file,
            },
            document,
        })
    }

    /// Borrow validated content fields without allocating or retaining a tree.
    pub fn view(&self) -> MetainfoView<'_> {
        MetainfoView {
            info_hash: self.info_hash,
            layout: &self.layout,
            hashes: self.piece_hashes,
            v2_hashes: &self.v2_hashes,
            layers_pending: self.layers_pending,
        }
    }
    pub fn document(&self) -> &Node<'a> {
        &self.document
    }
    pub fn info_bytes(&self) -> &'a [u8] {
        self.info_bytes
    }
    pub fn info_hash(&self) -> TorrentIdentity {
        self.info_hash
    }
    pub fn name(&self) -> &'a [u8] {
        self.name
    }
    pub fn is_private(&self) -> bool {
        self.private
    }
    pub fn layout(&self) -> &Layout<'a> {
        &self.layout
    }
    /// V1 SHA-1 commitment, including a hybrid's v1 commitment. None for v2-only.
    pub fn piece_hash(&self, index: u32) -> Option<&'a [u8; 20]> {
        hash_at(self.piece_hashes, index, self.layout.piece_count)
    }
    /// Hybrid payload can be verified with SHA-1 while required v2 layers are absent.
    pub fn layers_pending(&self) -> bool {
        self.layers_pending
    }
    pub fn verification(&self, index: u32) -> Option<PieceVerification> {
        self.view().verification(index)
    }
}

/// Authenticated commitments for one complete piece. V1 includes virtual padding;
/// v2 hashes only data blocks and pads its tree with absent leaf hashes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceVerification {
    V1([u8; 20]),
    V2(V2Verification),
    Hybrid { sha1: [u8; 20], v2: V2Verification },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct V2Verification {
    pub pieces_root: [u8; 32],
    pub layer_hash: [u8; 32],
    /// Tree width, including absent leaves, for this piece's verification.
    pub blocks: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    Data,
    /// Virtual zero-filled bytes: BEP 47 padding or implicit v2 alignment gaps.
    /// Included in v1 hashes, excluded from v2 file hashes.
    Padding,
}

#[derive(Debug)]
pub struct File<'a> {
    path: Vec<Cow<'a, [u8]>>,
    offset: u64,
    length: u64,
    kind: FileKind,
    pieces_root: Option<[u8; 32]>,
}

impl<'a> File<'a> {
    /// Multi-file paths are relative to `Metainfo::name`. Single-file paths
    /// contain the file name itself (from the tree for v2, from `name` for v1).
    /// Padding paths are virtual, not disk authority. Borrowed paths retain
    /// byte components; OwnedMetainfo owns them.
    pub fn path(&self) -> &[Cow<'a, [u8]>] {
        &self.path
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn length(&self) -> u64 {
        self.length
    }
    pub fn kind(&self) -> FileKind {
        self.kind
    }
    pub fn pieces_root(&self) -> Option<[u8; 32]> {
        self.pieces_root
    }
}

#[derive(Debug)]
pub struct Layout<'a> {
    files: Vec<File<'a>>,
    total_length: u64,
    piece_length: u32,
    piece_count: u32,
    multi_file: bool,
}

impl<'a> Layout<'a> {
    pub fn files(&self) -> &[File<'a>] {
        &self.files
    }
    /// Includes padding bytes, which participate in v1 piece hashes.
    pub fn total_length(&self) -> u64 {
        self.total_length
    }
    pub fn piece_length(&self) -> u32 {
        self.piece_length
    }
    pub fn piece_count(&self) -> u32 {
        self.piece_count
    }
    /// Whether paths are relative to `Metainfo::name`. For v2, only a single
    /// top-level file uses the single-file convention; directories stay intact.
    pub fn is_multi_file(&self) -> bool {
        self.multi_file
    }

    pub fn piece_range(&self, index: u32) -> Option<Range<u64>> {
        if index >= self.piece_count {
            return None;
        }
        let start = u64::from(index) * u64::from(self.piece_length);
        let length = (self.total_length - start).min(u64::from(self.piece_length));
        Some(start..start + length)
    }

    /// End offset of real data in a piece, excluding virtual tail padding.
    /// For v1 this is not a wire limit: requests may include padding.
    pub fn piece_data_end(&self, index: u32) -> Option<u32> {
        let range = self.piece_range(index)?;
        Some(
            self.segments(range.clone())
                .expect("piece range")
                .filter(|segment| segment.kind == FileKind::Data)
                .map(|segment| (segment.torrent_offset + segment.length - range.start) as u32)
                .max()
                .unwrap_or(0),
        )
    }

    /// Map an arbitrary half-open torrent range without allocating a segment
    /// vector. Zero-length files produce no segments. Padding remains explicit.
    pub fn segments(&self, range: Range<u64>) -> Option<Segments<'_, 'a>> {
        if range.start > range.end || range.end > self.total_length {
            return None;
        }
        let index = self
            .files
            .partition_point(|file| file.offset + file.length <= range.start);
        Some(Segments {
            layout: self,
            index,
            position: range.start,
            end: range.end,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Segment {
    pub file_index: usize,
    pub file_offset: u64,
    pub torrent_offset: u64,
    pub length: u64,
    pub kind: FileKind,
}

pub struct Segments<'s, 'a> {
    layout: &'s Layout<'a>,
    index: usize,
    position: u64,
    end: u64,
}

impl Iterator for Segments<'_, '_> {
    type Item = Segment;
    fn next(&mut self) -> Option<Self::Item> {
        while self.position < self.end {
            let index = self.index;
            let file = self.layout.files.get(index)?;
            self.index += 1;
            let length = (file.offset + file.length).min(self.end) - self.position;
            if length == 0 {
                continue;
            }
            let segment = Segment {
                file_index: index,
                file_offset: self.position - file.offset,
                torrent_offset: self.position,
                length,
                kind: file.kind,
            };
            self.position += length;
            return Some(segment);
        }
        None
    }
}

fn hash_at(hashes: &[u8], index: u32, count: u32) -> Option<&[u8; 20]> {
    if index >= count {
        return None;
    }
    let start = usize::try_from(index).ok()?.checked_mul(20)?;
    hashes.get(start..start.checked_add(20)?)?.try_into().ok()
}

fn required<'n, 'a>(node: &'n Node<'a>, key: &'static str) -> Result<&'n Node<'a>, Error> {
    node.get(key.as_bytes()).ok_or(Error::Missing(key))
}
fn dictionary(node: &Node<'_>, field: &'static str) -> Result<(), Error> {
    match node.value() {
        Value::Dictionary(_) => Ok(()),
        _ => Err(Error::InvalidField(field)),
    }
}
fn bytes<'a>(node: &Node<'a>, field: &'static str) -> Result<&'a [u8], Error> {
    match node.value() {
        Value::Bytes(value) => Ok(value),
        _ => Err(Error::InvalidField(field)),
    }
}
fn integer(node: &Node<'_>, field: &'static str) -> Result<i64, Error> {
    match node.value() {
        Value::Integer(value) => Ok(*value),
        _ => Err(Error::InvalidField(field)),
    }
}
fn component(part: &[u8]) -> Result<(), Error> {
    if part.is_empty() || part == b"." || part == b".." || part.contains(&b'/') || part.contains(&0)
    {
        return Err(Error::InvalidField("path component"));
    }
    Ok(())
}
fn add_file<'a>(
    files: &mut Vec<File<'a>>,
    total: &mut u64,
    node: &Node<'a>,
    path: Vec<Cow<'a, [u8]>>,
) -> Result<(), Error> {
    let length = u64::try_from(integer(required(node, "length")?, "length")?)
        .map_err(|_| Error::InvalidField("length"))?;
    // Like the baseline, ignore an optional attr value of the wrong type.
    let attr = match node.get(b"attr").map(Node::value) {
        Some(Value::Bytes(attr)) => *attr,
        _ => &[],
    };
    if attr.contains(&b'l') || node.get(b"symlink path").is_some() {
        return Err(Error::UnsupportedSymlink);
    }
    let kind = if attr.contains(&b'p') {
        FileKind::Padding
    } else {
        FileKind::Data
    };
    let offset = *total;
    *total = total.checked_add(length).ok_or(Error::LengthOverflow)?;
    files.push(File {
        path,
        offset,
        length,
        kind,
        pieces_root: None,
    });
    Ok(())
}
