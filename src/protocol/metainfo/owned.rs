//! Shared ownership without a self-referential parse tree. Byte fields are
//! offsets into immutable source bytes; layout paths own their components.

use super::*;
use std::sync::Arc;

/// Cheaply cloned, Send + Sync v1 metainfo with no caller-buffer lifetime.
/// Original outer/info bytes are retained verbatim, including noncanonical
/// encodings, duplicate/unknown fields and binary values. Identity never uses
/// re-encoding. No storage, networking or daemon admission policy lives here.
///
/// The same bencode input/string/node/depth budgets as Metainfo::parse bound
/// validation, retained source bytes, file/path metadata and copied path bytes.
/// The owned layout adds at most one copy of each retained path component; no
/// second piece-hash array or persistent decoded tree is retained. Allocation
/// overhead is additional and proportional to admitted bytes/nodes. Cloning
/// shares both the source and validated layout; it does not parse/copy either.
#[derive(Clone, Debug)]
pub struct OwnedMetainfo {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    source: Arc<[u8]>,
    index: Index,
    limits: Limits,
}
#[derive(Debug)]
struct Index {
    info: Range<usize>,
    name: Range<usize>,
    hashes: Range<usize>,
    info_hash: InfoHashV1,
    private: bool,
    layout: Layout<'static>,
}

impl OwnedMetainfo {
    /// Validate before copying input. Only input.len() bytes are retained, not
    /// any spare capacity in a caller's Vec. Temporary decoder metadata is freed
    /// after validation/indexing; owned-allocation work is input/node bounded.
    pub fn parse(input: &[u8], limits: Limits) -> Result<Self, Error> {
        let parsed = Metainfo::parse(input, limits)?;
        let index = Index::new(input, parsed);
        Ok(Self {
            data: Arc::new(Data {
                source: Arc::from(input),
                index,
                limits,
            }),
        })
    }

    /// Borrow validated content fields without decoding/copying the catalog.
    pub fn view(&self) -> MetainfoView<'_> {
        MetainfoView {
            info_hash: self.info_hash(),
            layout: self.layout(),
            hashes: &self.data.source[self.data.index.hashes.clone()],
        }
    }
    pub fn source_bytes(&self) -> &[u8] {
        &self.data.source
    }
    pub fn info_bytes(&self) -> &[u8] {
        &self.data.source[self.data.index.info.clone()]
    }
    pub fn info_hash(&self) -> InfoHashV1 {
        self.data.index.info_hash
    }
    pub fn name(&self) -> &[u8] {
        &self.data.source[self.data.index.name.clone()]
    }
    pub fn is_private(&self) -> bool {
        self.data.index.private
    }
    pub fn layout(&self) -> &Layout<'static> {
        &self.data.index.layout
    }
    pub fn piece_hash(&self, index: u32) -> Option<&[u8; 20]> {
        hash_at(
            &self.data.source[self.data.index.hashes.clone()],
            index,
            self.layout().piece_count(),
        )
    }

    /// Explicitly decode outer discovery/unknown fields on demand. Allocates a
    /// temporary borrowed tree using the original admission budgets; not called
    /// by content/layout/hash accessors. The immutable validated source cannot
    /// become invalid between calls. Returned strings/spans borrow this handle.
    pub fn decode_document(&self) -> Node<'_> {
        bencode::decode(&self.data.source, self.data.limits)
            .expect("immutable validated metainfo document")
    }
}

impl Index {
    fn new(source: &[u8], parsed: Metainfo<'_>) -> Self {
        let Metainfo {
            info_bytes,
            info_hash,
            name,
            private,
            piece_hashes,
            layout,
            ..
        } = parsed;
        Self {
            info: span(source, info_bytes),
            name: span(source, name),
            hashes: span(source, piece_hashes),
            info_hash,
            private,
            layout: Layout {
                files: layout
                    .files
                    .into_iter()
                    .map(|file| File {
                        path: file
                            .path
                            .into_iter()
                            .map(|part| Cow::Owned(part.into_owned()))
                            .collect(),
                        offset: file.offset,
                        length: file.length,
                        kind: file.kind,
                    })
                    .collect(),
                total_length: layout.total_length,
                piece_length: layout.piece_length,
                piece_count: layout.piece_count,
                multi_file: layout.multi_file,
            },
        }
    }
}

// All inputs come from this module's validated borrowed parser. Preserve exact
// positions, not a byte-content search (identical fields may have different spans).
fn span(source: &[u8], part: &[u8]) -> Range<usize> {
    let start = (part.as_ptr() as usize)
        .checked_sub(source.as_ptr() as usize)
        .expect("decoded metainfo subslice");
    let end = start
        .checked_add(part.len())
        .filter(|&end| end <= source.len())
        .expect("decoded metainfo span within source");
    start..end
}
