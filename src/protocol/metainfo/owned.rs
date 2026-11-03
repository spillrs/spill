//! Shared ownership without a self-referential parse tree. Byte fields are
//! offsets into immutable source bytes; layout paths own their components.

use super::{v2::piece_layers_size, *};
use std::sync::{Arc, OnceLock};

/// Cheaply cloned, Send + Sync metainfo with no caller-buffer lifetime.
/// Original outer/info bytes are retained verbatim, including noncanonical
/// encodings, duplicate/unknown fields and binary values. Identity never uses
/// re-encoding. No storage, networking or daemon admission policy lives here.
///
/// The same bencode input/string/node/depth budgets as Metainfo::parse bound
/// validation, retained source bytes, file/path metadata and copied path bytes.
/// The owned layout adds at most one copy of each retained path component; no
/// persistent decoded tree is retained. V2 commitments add one 32-byte hash
/// per piece. Expanded data-path bytes, commitments and lazily cached upper
/// proof-tree nodes per root share the input budget.
/// Allocation overhead is additional and proportional to admitted bytes/nodes. Cloning
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
    info_hash: TorrentIdentity,
    v2_hashes: Vec<[u8; 32]>,
    layers_pending: bool,
    #[allow(clippy::type_complexity)]
    trees: std::collections::BTreeMap<[u8; 32], OnceLock<Vec<Vec<[u8; 32]>>>>,
    private: bool,
    layout: Layout<'static>,
}

/// Validated BEP 9 info dictionary, without authenticated outer piece layers.
/// This state cannot be used for payload verification or storage admission.
#[derive(Clone, Debug)]
pub struct InfoMetainfo {
    inner: OwnedMetainfo,
}

impl InfoMetainfo {
    pub fn parse(info: &[u8], limits: Limits) -> Result<Self, Error> {
        // Reject trailing values before wrapping, so they cannot become outer keys.
        bencode::decode(info, limits).map_err(Error::Bencode)?;
        let mut source = b"d4:info".to_vec();
        source.extend_from_slice(info);
        source.push(b'e');
        let document = bencode::decode(&source, limits).map_err(Error::Bencode)?;
        let node = required(&document, "info")?;
        dictionary(node, "info")?;
        let parsed = if let Some(version) = node.get(b"meta version") {
            if !matches!(version.value(), Value::Integer(2)) {
                return Err(Error::UnsupportedVersion);
            }
            Metainfo::v2(document, limits, true)?
        } else {
            if node.get(b"file tree").is_some() {
                return Err(Error::UnsupportedVersion);
            }
            Metainfo::v1(document)?
        };
        let index = Index::new(&source, parsed);
        let info = Self {
            inner: OwnedMetainfo {
                data: Arc::new(Data {
                    source: Arc::from(source),
                    index,
                    limits,
                }),
            },
        };
        if info.completed_size() > limits.max_input_len {
            return Err(Error::MetadataLimit);
        }
        Ok(info)
    }
    /// Exact encoded size of the completed document, excluding discovery fields.
    pub fn completed_size(&self) -> usize {
        let mut layers = std::collections::BTreeMap::new();
        if self.info_hash().v2().is_some() {
            for file in self
                .layout()
                .files()
                .iter()
                .filter(|file| file.length() > u64::from(self.layout().piece_length()))
            {
                layers.insert(
                    file.pieces_root().expect("v2 file root"),
                    (file
                        .length()
                        .div_ceil(u64::from(self.layout().piece_length()))
                        * 32) as usize,
                );
            }
        }
        self.encoded_size(layers.into_values())
    }
    fn encoded_size(&self, layers: impl Iterator<Item = usize>) -> usize {
        8 + self.info_bytes().len()
            + if self.info_hash().v2().is_some() {
                piece_layers_size(layers)
            } else {
                0
            }
    }
    pub fn info_hash(&self) -> TorrentIdentity {
        self.inner.info_hash()
    }
    pub fn layout(&self) -> &Layout<'static> {
        self.inner.layout()
    }
    pub fn is_private(&self) -> bool {
        self.inner.is_private()
    }
    pub fn info_bytes(&self) -> &[u8] {
        self.inner.info_bytes()
    }

    /// Complete with acquired layers, authenticating each against its file root.
    /// Hybrids may omit required layers and remain SHA-1-verifiable with
    /// [`OwnedMetainfo::layers_pending`] until their payload rebuilds the layers.
    pub fn complete(
        &self,
        layers: &std::collections::BTreeMap<[u8; 32], Vec<u8>>,
    ) -> Result<OwnedMetainfo, Error> {
        self.inner.with_piece_layers(layers)
    }
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
            v2_hashes: &self.data.index.v2_hashes,
            layers_pending: self.layers_pending(),
        }
    }
    pub fn source_bytes(&self) -> &[u8] {
        &self.data.source
    }
    pub fn info_bytes(&self) -> &[u8] {
        &self.data.source[self.data.index.info.clone()]
    }
    pub fn info_hash(&self) -> TorrentIdentity {
        self.data.index.info_hash
    }
    /// Required v2 layers are absent; this hybrid verifies payload with SHA-1.
    pub fn layers_pending(&self) -> bool {
        self.data.index.layers_pending
    }

    /// Replace outer layers without re-encoding authenticated info bytes.
    pub(crate) fn with_piece_layers(
        &self,
        layers: &std::collections::BTreeMap<[u8; 32], Vec<u8>>,
    ) -> Result<Self, Error> {
        if self.info_hash().v2().is_none() && !layers.is_empty() {
            return Err(Error::InvalidField("unused piece layer"));
        }
        let document = self.decode_document();
        let Value::Dictionary(entries) = document.value() else {
            unreachable!("validated document")
        };
        let fields = entries.iter().filter(|(key, _)| **key != b"piece layers");
        let size = 2
            + fields
                .clone()
                .map(|(key, node)| {
                    key.len().to_string().len()
                        + 1
                        + key.len()
                        + node.raw_bytes().expect("decoded field").len()
                })
                .sum::<usize>()
            + if self.info_hash().v2().is_some() {
                piece_layers_size(layers.values().map(Vec::len))
            } else {
                0
            };
        if size > self.data.limits.max_input_len {
            return Err(Error::MetadataLimit);
        }
        let mut source = Vec::with_capacity(size);
        source.push(b'd');
        let mut pending = self.info_hash().v2().is_some().then_some(layers);
        for (key, node) in fields {
            if *key > b"piece layers".as_slice()
                && let Some(layers) = pending.take()
            {
                encode_layers(&mut source, layers);
            }
            source.extend_from_slice(format!("{}:", key.len()).as_bytes());
            source.extend_from_slice(key);
            source.extend_from_slice(node.raw_bytes().expect("decoded field"));
        }
        if let Some(layers) = pending {
            encode_layers(&mut source, layers);
        }
        source.push(b'e');
        Self::parse(&source, self.data.limits)
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

    pub(crate) fn piece_tree(&self, file: usize) -> &[Vec<[u8; 32]>] {
        let file = &self.layout().files()[file];
        let piece_length = u64::from(self.layout().piece_length());
        let first = (file.offset() / piece_length) as usize;
        let count = file.length().div_ceil(piece_length) as usize;
        self.data.index.trees[&file.pieces_root().expect("nonempty v2 file")].get_or_init(|| {
            let mut levels: Vec<Vec<[u8; 32]>> = Vec::new();
            let mut layer = &self.data.index.v2_hashes[first..first + count];
            let mut height = file
                .length()
                .min(piece_length)
                .div_ceil(crate::protocol::merkle::BLOCK_LENGTH as u64)
                .next_power_of_two()
                .ilog2();
            while layer.len() > 1 {
                let padding = crate::protocol::merkle::padding_hash(height);
                let next = layer
                    .chunks(2)
                    .map(|pair| {
                        crate::protocol::merkle::parent(&pair[0], pair.get(1).unwrap_or(&padding))
                    })
                    .collect();
                levels.push(next);
                layer = levels.last().expect("tree level");
                height += 1;
            }
            levels
        })
    }

    pub fn verification(&self, index: u32) -> Option<PieceVerification> {
        self.view().verification(index)
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

fn encode_layers(source: &mut Vec<u8>, layers: &std::collections::BTreeMap<[u8; 32], Vec<u8>>) {
    source.extend_from_slice(b"12:piece layersd");
    for (root, hashes) in layers {
        source.extend_from_slice(b"32:");
        source.extend_from_slice(root);
        source.extend_from_slice(format!("{}:", hashes.len()).as_bytes());
        source.extend_from_slice(hashes);
    }
    source.push(b'e');
}

impl Index {
    fn new(source: &[u8], parsed: Metainfo<'_>) -> Self {
        let Metainfo {
            info_bytes,
            info_hash,
            name,
            private,
            piece_hashes,
            v2_hashes,
            layers_pending,
            layout,
            ..
        } = parsed;
        Self {
            info: span(source, info_bytes),
            name: span(source, name),
            hashes: span(source, piece_hashes),
            info_hash,
            v2_hashes,
            layers_pending,
            trees: layout
                .files
                .iter()
                .filter_map(|file| file.pieces_root.map(|root| (root, OnceLock::new())))
                .collect(),
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
                        pieces_root: file.pieces_root,
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
