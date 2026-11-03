use super::*;
use crate::protocol::merkle;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn piece_layers_size(layers: impl Iterator<Item = usize>) -> usize {
    17 + layers
        .map(|bytes| 36 + bytes.to_string().len() + bytes)
        .sum::<usize>()
}

impl<'a> Metainfo<'a> {
    pub(super) fn v2(document: Node<'a>, limits: Limits, info_only: bool) -> Result<Self, Error> {
        let info = required(&document, "info")?;
        let name = bytes(required(info, "name")?, "name")?;
        let piece_length = u32::try_from(integer(required(info, "piece length")?, "piece length")?)
            .ok()
            .filter(|length| *length >= merkle::BLOCK_LENGTH as u32 && length.is_power_of_two())
            .ok_or(Error::InvalidField("piece length"))?;
        let tree = required(info, "file tree")?;
        dictionary(tree, "file tree")?;
        if tree.get(b"").is_some() {
            return Err(Error::InvalidField("file tree root"));
        }
        let mut data = Vec::new();
        let mut path_bytes = 0;
        let mut path_nodes = 0;
        tree_files(
            tree,
            &mut Vec::new(),
            &mut data,
            limits,
            &mut path_bytes,
            &mut path_nodes,
        )?;
        if data.is_empty() {
            return Err(Error::EmptyFiles);
        }
        let multi_file = data.len() != 1 || data[0].path.len() != 1;
        if multi_file {
            component(name)?;
        }
        let mut root_lengths = BTreeMap::new();
        for file in &data {
            if let Some(root) = file.pieces_root
                && let Some(length) = root_lengths.insert(root, file.length)
                && length != file.length
            {
                return Err(Error::InvalidField("pieces root length"));
            }
        }
        let last_nonempty = data.iter().rposition(|file| file.length != 0);
        let mut files = Vec::new();
        let mut total_length = 0u64;
        for (index, mut file) in data.into_iter().enumerate() {
            file.offset = total_length;
            total_length = total_length
                .checked_add(file.length)
                .ok_or(Error::LengthOverflow)?;
            files.push(file);
            if last_nonempty.is_some_and(|last| index < last) {
                let padding = (u64::from(piece_length) - total_length % u64::from(piece_length))
                    % u64::from(piece_length);
                if padding != 0 {
                    files.push(File {
                        path: vec![
                            Cow::Borrowed(b".pad"),
                            Cow::Owned(padding.to_string().into_bytes()),
                        ],
                        offset: total_length,
                        length: padding,
                        kind: FileKind::Padding,
                        pieces_root: None,
                    });
                    total_length = total_length
                        .checked_add(padding)
                        .ok_or(Error::LengthOverflow)?;
                }
            }
        }
        let piece_count = u32::try_from(total_length.div_ceil(u64::from(piece_length)))
            .map_err(|_| Error::PieceCount)?;
        // Repeated roots share outer layers but still expand to per-piece
        // commitments. Bound that expansion before allocating it.
        let mut roots = BTreeMap::new();
        for file in files
            .iter()
            .filter(|file| file.length != 0 && file.kind == FileKind::Data)
        {
            roots.insert(
                file.pieces_root.expect("v2 file root"),
                file.length.div_ceil(u64::from(piece_length)),
            );
        }
        let mut tree_nodes = 0;
        for mut count in roots.values().copied() {
            while count > 1 {
                count = count.div_ceil(2);
                tree_nodes += count;
            }
        }
        if (u64::from(piece_count) + tree_nodes) * 32 + path_bytes as u64
            > limits.max_input_len as u64
        {
            return Err(Error::MetadataLimit);
        }
        let mut layout = Layout {
            files,
            total_length,
            piece_length,
            piece_count,
            multi_file,
        };
        let hybrid = info.get(b"pieces").is_some()
            || info.get(b"length").is_some()
            || info.get(b"files").is_some();
        let empty = BTreeMap::new();
        let entries = if info_only {
            &empty
        } else if let Some(layers) = document.get(b"piece layers") {
            let Value::Dictionary(entries) = layers.value() else {
                return Err(Error::InvalidField("piece layers"));
            };
            entries
        } else if hybrid {
            &empty
        } else {
            return Err(Error::Missing("piece layers"));
        };
        let mut layers_pending = false;
        let mut used = BTreeSet::new();
        let mut v2_hashes = Vec::new();
        for file in layout
            .files
            .iter()
            .filter(|file| file.kind == FileKind::Data && file.length != 0)
        {
            let root = file.pieces_root.expect("nonempty file root");
            if file.length <= u64::from(piece_length) {
                v2_hashes.push(root);
                continue;
            }
            let Some(layer) = entries.get(root.as_slice()) else {
                if hybrid {
                    layers_pending = true;
                } else if !info_only {
                    return Err(Error::Missing("piece layer"));
                }
                continue;
            };
            let layer = bytes(layer, "piece layer")?;
            let count = file.length.div_ceil(u64::from(piece_length));
            if layer.len() as u64 != count * 32 {
                return Err(Error::InvalidField("piece layer length"));
            }
            let hashes = layer.as_chunks::<32>().0;
            if merkle::root_from_layer(hashes, (piece_length / merkle::BLOCK_LENGTH as u32).ilog2())
                != root
            {
                return Err(Error::InvalidField("piece layer root"));
            }
            used.insert(root);
            v2_hashes.extend_from_slice(hashes);
        }
        if used.len() != entries.len() {
            return Err(Error::InvalidField("unused piece layer"));
        }
        if layers_pending {
            // Rebuilding must fit the document budget, even with large outer fields.
            if !info_only {
                let layers_size = piece_layers_size(
                    roots
                        .values()
                        .filter(|&&count| count > 1)
                        .map(|count| (count * 32) as usize),
                );
                let existing = document.get(b"piece layers").map_or(0, |node| {
                    15 + node.raw_bytes().expect("decoded layer dictionary").len()
                });
                if document.raw_bytes().expect("decoded document").len() - existing + layers_size
                    > limits.max_input_len
                {
                    return Err(Error::MetadataLimit);
                }
            }
            v2_hashes.clear();
        } else if !info_only {
            assert_eq!(v2_hashes.len(), piece_count as usize);
        }
        let info_bytes = info.raw_bytes().expect("decoded info span");
        let v2 = InfoHashV2::from_info_bytes(info_bytes);
        let (info_hash, piece_hashes) = if hybrid {
            let v1 = Self::v1(document.clone())?;
            validate_hybrid(&v1.layout, &mut layout)?;
            (
                TorrentIdentity::Hybrid(v1.info_hash.v1().expect("v1 parser"), v2),
                v1.piece_hashes,
            )
        } else {
            (TorrentIdentity::V2(v2), &info_bytes[..0])
        };
        let private = info
            .get(b"private")
            .is_some_and(|node| matches!(node.value(), Value::Integer(1)));
        Ok(Self {
            document,
            info_bytes,
            info_hash,
            name,
            private,
            piece_hashes,
            v2_hashes,
            layers_pending,
            layout,
        })
    }
}

fn tree_files<'a>(
    node: &Node<'a>,
    path: &mut Vec<Cow<'a, [u8]>>,
    files: &mut Vec<File<'a>>,
    limits: Limits,
    path_bytes: &mut usize,
    path_nodes: &mut usize,
) -> Result<(), Error> {
    let Value::Dictionary(entries) = node.value() else {
        return Err(Error::InvalidField("file tree entry"));
    };
    if let Some(properties) = entries.get(b"".as_slice()) {
        if entries.len() != 1 {
            return Err(Error::PathConflict);
        }
        dictionary(properties, "file properties")?;
        let length = u64::try_from(integer(required(properties, "length")?, "length")?)
            .map_err(|_| Error::InvalidField("length"))?;
        if properties.get(b"symlink path").is_some()
            || properties.get(b"attr").is_some_and(
                |node| matches!(node.value(), Value::Bytes(attr) if attr.contains(&b'l')),
            )
        {
            return Err(Error::UnsupportedSymlink);
        }
        let pieces_root = match (length, properties.get(b"pieces root")) {
            (0, None) => None,
            (0, Some(_)) => return Err(Error::InvalidField("empty file root")),
            (_, Some(root)) => Some(
                bytes(root, "pieces root")?
                    .try_into()
                    .map_err(|_| Error::InvalidField("pieces root"))?,
            ),
            (_, None) => return Err(Error::Missing("pieces root")),
        };
        *path_bytes += path.iter().map(|part| part.len()).sum::<usize>();
        *path_nodes += path.len();
        if *path_bytes > limits.max_input_len || *path_nodes > limits.max_nodes {
            return Err(Error::MetadataLimit);
        }
        files.push(File {
            path: path.clone(),
            offset: 0,
            length,
            kind: FileKind::Data,
            pieces_root,
        });
        return Ok(());
    }
    if entries.is_empty() {
        return Err(Error::EmptyFiles);
    }
    for (&name, child) in entries {
        component(name)?;
        path.push(Cow::Borrowed(name));
        tree_files(child, path, files, limits, path_bytes, path_nodes)?;
        path.pop();
    }
    Ok(())
}

fn validate_hybrid(v1: &Layout<'_>, v2: &mut Layout<'_>) -> Result<(), Error> {
    if v1.piece_count != v2.piece_count || v1.piece_length != v2.piece_length {
        return Err(Error::InvalidField("hybrid layout"));
    }
    let data1: Vec<_> = v1
        .files
        .iter()
        .filter(|file| file.kind == FileKind::Data)
        .collect();
    let data2: Vec<_> = v2
        .files
        .iter()
        .filter(|file| file.kind == FileKind::Data)
        .collect();
    if data1.len() != data2.len()
        || data1.iter().zip(data2).any(|(a, b)| {
            a.path != b.path || a.length != b.length || (a.length != 0 && a.offset != b.offset)
        })
    {
        return Err(Error::InvalidField("hybrid files"));
    }
    // Real-file offsets fix internal padding extents; only final tail padding
    // may differ, without adding a piece or changing any payload bytes.
    if v1.total_length < v2.total_length {
        return Err(Error::InvalidField("hybrid padding"));
    }
    if v1.total_length > v2.total_length {
        let length = v1.total_length - v2.total_length;
        v2.files.push(File {
            path: vec![
                Cow::Borrowed(b".pad"),
                Cow::Owned(length.to_string().into_bytes()),
            ],
            offset: v2.total_length,
            length,
            kind: FileKind::Padding,
            pieces_root: None,
        });
        v2.total_length = v1.total_length;
    }
    v2.multi_file = v1.multi_file;
    Ok(())
}
