//! Borrowed/owned v1 metainfo and logical ranges. Hash original info bytes;
//! paths are untrusted byte components, not filesystem authority.

use std::{borrow::Cow, fmt, ops::Range};

mod owned;
mod view;
pub use owned::OwnedMetainfo;
pub use view::MetainfoView;

use crate::protocol::{
    bencode::{self, Limits, Node, Value},
    identity::InfoHashV1,
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

/// A validated v1 content layout. Tracker/discovery fields remain accessible in
/// `document`; their validation and runtime policy are deliberately separate.
#[derive(Debug)]
pub struct Metainfo<'a> {
    document: Node<'a>,
    info_bytes: &'a [u8],
    info_hash: InfoHashV1,
    name: &'a [u8],
    private: bool,
    piece_hashes: &'a [u8],
    layout: Layout<'a>,
}

impl<'a> Metainfo<'a> {
    /// Parse one complete .torrent document within the bencode budgets.
    /// Content structure is validated; tracker URLs and other outer extensions
    /// are retained but not validated. No content or filesystem is accessed.
    pub fn parse(input: &'a [u8], limits: Limits) -> Result<Self, Error> {
        let document = bencode::decode(input, limits).map_err(Error::Bencode)?;
        dictionary(&document, "metainfo")?;
        let info = required(&document, "info")?;
        dictionary(info, "info")?;
        if info.get(b"meta version").is_some()
            || info.get(b"file tree").is_some()
            || document.get(b"piece layers").is_some()
        {
            return Err(Error::UnsupportedVersion);
        }
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
                let mut paths: Vec<_> = files.iter().map(|file| &file.path).collect();
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
            info_hash: InfoHashV1::from_info_bytes(info_bytes),
            info_bytes,
            name,
            private,
            piece_hashes,
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
        }
    }
    pub fn document(&self) -> &Node<'a> {
        &self.document
    }
    pub fn info_bytes(&self) -> &'a [u8] {
        self.info_bytes
    }
    pub fn info_hash(&self) -> InfoHashV1 {
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
    pub fn piece_hash(&self, index: u32) -> Option<&'a [u8; 20]> {
        hash_at(self.piece_hashes, index, self.layout.piece_count)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    Data,
    /// BEP 47 bytes: part of the v1 hash stream, logically zero-filled.
    Padding,
}

#[derive(Debug)]
pub struct File<'a> {
    path: Vec<Cow<'a, [u8]>>,
    offset: u64,
    length: u64,
    kind: FileKind,
}

impl<'a> File<'a> {
    /// Multi-file paths are relative to `Metainfo::name`; single-file paths
    /// contain the name itself. No decoding, joining or normalization is done.
    /// Borrowed parsing retains borrowed components; OwnedMetainfo owns them.
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
    });
    Ok(())
}
