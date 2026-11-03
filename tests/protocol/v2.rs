use crate::protocol::merkle::{self, BLOCK_LENGTH};
use spill::protocol::{
    bencode::{self, Limits, Node, Value},
    identity::{InfoHashV1, InfoHashV2, TorrentIdentity},
    metainfo::{self, FileKind, Metainfo, OwnedMetainfo, PieceVerification},
    peer::{
        hash::{Error as HashError, HashRequest},
        message::{Error as MessageError, Message},
    },
};

#[path = "../../src/protocol/peer/hash.rs"]
mod hash_response;

#[test]
fn hash_response_range_can_end_past_u32_max() {
    let request = hash_response::HashRequest {
        pieces_root: merkle::padding_hash(32),
        base_layer: 0,
        index: u32::MAX - 1,
        length: 2,
        proof_layers: 31,
    };
    let file_length = (1u64 << 32) * BLOCK_LENGTH as u64;
    request.validate(file_length, 2).unwrap();
    let hashes = request.response(&[], 0, &vec![Vec::new(); 32], &[]);
    assert_eq!(hashes.len(), 33 * 32);
    request.verify(file_length, 2, &hashes).unwrap();
}

const V2: &[u8] = include_bytes!("fixtures/v2.torrent");
const HYBRID: &[u8] = include_bytes!("fixtures/hybrid.torrent");
const LIBTORRENT_HYBRID: &[u8] = include_bytes!("fixtures/libtorrent-hybrid.torrent");
const PIECE_LENGTH: u32 = 32768;

fn bytes<'a>(node: &Node<'a>) -> &'a [u8] {
    match node.value() {
        Value::Bytes(bytes) => bytes,
        _ => panic!("expected bytes"),
    }
}

fn hex(hash: &[u8; 32]) -> String {
    InfoHashV2::from_bytes(*hash).to_string()
}

fn fixture_layers() -> ([u8; 32], Vec<[u8; 32]>) {
    let document = bencode::decode(V2, Limits::default()).unwrap();
    let info = document.get(b"info").unwrap();
    let file = info
        .get(b"file tree")
        .unwrap()
        .get(b"odd")
        .unwrap()
        .get(b"")
        .unwrap();
    let root: [u8; 32] = bytes(file.get(b"pieces root").unwrap()).try_into().unwrap();
    let layer = bytes(document.get(b"piece layers").unwrap().get(&root).unwrap());
    (root, layer.as_chunks::<32>().0.to_vec())
}

fn rewrite<'a>(wire: &'a [u8], path: &[&'a [u8]], value: Option<Value<'a>>) -> Vec<u8> {
    fn replace<'a>(node: Node<'a>, path: &[&'a [u8]], value: Option<Value<'a>>) -> Node<'a> {
        let Value::Dictionary(mut fields) = node.value().clone() else {
            panic!("dictionary path");
        };
        if path.len() == 1 {
            match value {
                Some(value) => {
                    fields.insert(path[0], Node::new(value));
                }
                None => {
                    fields.remove(path[0]);
                }
            }
        } else {
            let key = *fields.get_key_value(path[0]).unwrap().0;
            let child = fields.remove(key).unwrap();
            fields.insert(key, replace(child, &path[1..], value));
        }
        Node::new(Value::Dictionary(fields))
    }
    let document = bencode::decode(wire, Limits::default()).unwrap();
    bencode::encode(&replace(document, path, value), Limits::default()).unwrap()
}

fn payload(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}

#[test]
fn info_only_metadata_requires_authenticated_layers_before_becoming_metainfo() {
    use metainfo::InfoMetainfo;
    use std::collections::BTreeMap;
    assert!(InfoMetainfo::parse(b"de3:fooi1e", Limits::default()).is_err());
    for fixture in [
        V2,
        HYBRID,
        LIBTORRENT_HYBRID,
        include_bytes!("fixtures/v2-single.torrent").as_slice(),
    ] {
        let complete = OwnedMetainfo::parse(fixture, Limits::default()).unwrap();
        let injected = [complete.info_bytes(), b"3:fooi1e"].concat();
        assert!(InfoMetainfo::parse(&injected, Limits::default()).is_err());
        let info = InfoMetainfo::parse(complete.info_bytes(), Limits::default()).unwrap();
        assert_eq!(info.info_hash(), complete.info_hash());
        assert_eq!(info.info_bytes(), complete.info_bytes());
        let doc = complete.decode_document();
        let Value::Dictionary(entries) = doc.get(b"piece layers").unwrap().value() else {
            unreachable!()
        };
        let layers: BTreeMap<[u8; 32], Vec<u8>> = entries
            .iter()
            .map(|(root, value)| ((*root).try_into().unwrap(), bytes(value).to_vec()))
            .collect();
        let resolved = info.complete(&layers).unwrap();
        for piece in 0..complete.layout().piece_count() {
            assert_eq!(resolved.verification(piece), complete.verification(piece));
        }
        if !layers.is_empty() {
            if complete.info_hash().v1().is_some() {
                let pending = info.complete(&BTreeMap::new()).unwrap();
                assert!(pending.layers_pending());
                assert_eq!(pending.info_hash(), complete.info_hash());
                for piece in 0..pending.layout().piece_count() {
                    assert!(matches!(
                        pending.verification(piece),
                        Some(metainfo::PieceVerification::V1(_))
                    ));
                }
                let missing = rewrite(fixture, &[b"piece layers"], None);
                assert!(
                    OwnedMetainfo::parse(&missing, Limits::default())
                        .unwrap()
                        .layers_pending()
                );
                let bits = vec![0; pending.layout().piece_count().div_ceil(8) as usize];
                let record = spill::protocol::resume::encode(&pending, &bits, &[]).unwrap();
                assert!(spill::protocol::resume::decode(&complete, &record).is_err());
            } else {
                assert!(info.complete(&BTreeMap::new()).is_err());
            }
            let mut corrupt = layers.clone();
            corrupt.values_mut().next().unwrap()[0] ^= 1;
            assert!(info.complete(&corrupt).is_err());
        }
    }
    let invalid = rewrite(
        HYBRID,
        &[b"info", b"piece length"],
        Some(Value::Integer(17000)),
    );
    let doc = bencode::decode(&invalid, Limits::default()).unwrap();
    assert!(
        InfoMetainfo::parse(
            doc.get(b"info").unwrap().raw_bytes().unwrap(),
            Limits::default()
        )
        .is_err()
    );
    let meta = OwnedMetainfo::parse(V2, Limits::default()).unwrap();
    let limits = Limits {
        max_input_len: meta.info_bytes().len() + 32,
        ..Limits::default()
    };
    assert!(InfoMetainfo::parse(meta.info_bytes(), limits).is_err());
}

#[test]
fn independent_creators_identities_and_owned_commitments_match() {
    for (wire, sha1, sha256) in [
        (
            V2,
            "d91d2fb0d4fd111df8a2f62113e17f590c9535f6",
            "52216953860eaa057e6634eb87b2ce45007b107105b67d1ff8b3b53e11134d89",
        ),
        (
            HYBRID,
            "70a45de6b5f32bba9074db3c74e3b832b1abb576",
            "3fc36df669f075b1a2d5f6f96a4e64e5584a2172829c6873aab7e0c0b3cfd402",
        ),
        (
            LIBTORRENT_HYBRID,
            "4cd259c8d8aa0566c220c16359d2fbf407d30c76",
            "c0940c57d9cf2d1103ed5cb40550b2eafa7811e8fc6aefec85610db15007190a",
        ),
    ] {
        let document = bencode::decode(wire, Limits::default()).unwrap();
        let info = document.get(b"info").unwrap().raw_bytes().unwrap();
        let v1 = InfoHashV1::from_info_bytes(info);
        let v2 = InfoHashV2::from_info_bytes(info);
        assert_eq!(v1.to_string(), sha1);
        assert_eq!(v2.to_string(), sha256);
        assert!(TorrentIdentity::Hybrid(v1, v2).matches_info_bytes(info));
        let parsed = Metainfo::parse(wire, Limits::default()).unwrap();
        let owned = OwnedMetainfo::parse(wire, Limits::default()).unwrap();
        let expected = if wire == V2 {
            TorrentIdentity::V2(v2)
        } else {
            TorrentIdentity::Hybrid(v1, v2)
        };
        assert_eq!(parsed.info_hash(), expected);
        assert_eq!(owned.info_hash(), expected);
        assert_eq!(owned.source_bytes(), wire);
        assert_eq!(owned.info_bytes(), info);
        for index in 0..parsed.layout().piece_count() {
            assert_eq!(parsed.verification(index), owned.verification(index));
            assert_eq!(
                parsed.view().verification(index),
                owned.view().verification(index)
            );
        }
        assert!(parsed.verification(parsed.layout().piece_count()).is_none());
    }
}

#[test]
fn v2_layout_synthesizes_padding_and_keeps_file_tail_wire_limits() {
    use spill::protocol::peer::message::BlockRequest;
    for wire in [V2, HYBRID] {
        let meta = OwnedMetainfo::parse(wire, Limits::default()).unwrap();
        let layout = meta.layout();
        assert_eq!(layout.piece_count(), 6);
        assert_eq!(layout.files().len(), 8);
        assert!(layout.is_multi_file());
        assert_eq!(
            layout
                .files()
                .iter()
                .filter(|file| file.kind() == FileKind::Data)
                .map(|file| file.length())
                .sum::<u64>(),
            114812
        );
        for (index, end, blocks) in [
            (0, 16384, 1),
            (1, 32768, 2),
            (2, 32768, 2),
            (3, 123, 2),
            (4, 1, 1),
            (5, 32768, 2),
        ] {
            assert_eq!(layout.piece_data_end(index), Some(end));
            let v2 = match meta.verification(index).unwrap() {
                PieceVerification::V2(v2) => {
                    assert_eq!(wire, V2);
                    v2
                }
                PieceVerification::Hybrid { v2, .. } => {
                    assert_eq!(wire, HYBRID);
                    v2
                }
                _ => panic!("v2 commitment"),
            };
            assert_eq!(v2.blocks, blocks);
            let request = BlockRequest {
                index,
                begin: end - 1,
                length: 1,
            };
            assert!(request.validate(layout, 16384, true).is_ok());
            if end < PIECE_LENGTH {
                let request = BlockRequest {
                    begin: end,
                    ..request
                };
                assert!(request.validate(layout, 16384, true).is_err());
                assert!(request.validate(layout, 16384, false).is_ok());
            }
        }
        assert_eq!(layout.piece_data_end(6), None);
    }
}

#[test]
fn v2_rejects_missing_corrupt_and_uncommitted_layers() {
    let (root, _) = fixture_layers();
    let bad_layers = [
        rewrite(V2, &[b"piece layers"], None),
        rewrite(V2, &[b"piece layers", &root], None),
        rewrite(V2, &[b"piece layers", &root], Some(Value::Bytes(&[0; 96]))),
        rewrite(V2, &[b"piece layers", &root], Some(Value::Bytes(&[0; 64]))),
        rewrite(
            V2,
            &[b"piece layers", &[1; 32]],
            Some(Value::Bytes(&[0; 96])),
        ),
    ];
    for wire in bad_layers {
        assert!(Metainfo::parse(&wire, Limits::default()).is_err());
        assert!(OwnedMetainfo::parse(&wire, Limits::default()).is_err());
    }
    for end in 0..V2.len() {
        assert!(Metainfo::parse(&V2[..end], Limits::default()).is_err());
    }
}

#[test]
fn v2_tree_boundaries_and_version_precedence() {
    use std::collections::BTreeMap;
    let future = rewrite(V2, &[b"info", b"meta version"], Some(Value::Integer(3)));
    let future = rewrite(&future, &[b"info", b"name"], Some(Value::Integer(0)));
    assert!(matches!(
        Metainfo::parse(&future, Limits::default()),
        Err(metainfo::Error::UnsupportedVersion)
    ));
    for wire in [
        rewrite(V2, &[b"info", b"piece length"], Some(Value::Integer(16385))),
        rewrite(V2, &[b"info", b"piece length"], Some(Value::Integer(8192))),
        rewrite(
            V2,
            &[b"info", b"file tree", b"one", b"", b"pieces root"],
            None,
        ),
        rewrite(
            V2,
            &[b"info", b"file tree", b"one", b"", b"pieces root"],
            Some(Value::Bytes(&[0; 31])),
        ),
        rewrite(
            V2,
            &[b"info", b"file tree", b"empty", b"", b"pieces root"],
            Some(Value::Bytes(&[0; 32])),
        ),
        rewrite(
            V2,
            &[b"info", b"file tree", b"one", b"", b"length"],
            Some(Value::Integer(-1)),
        ),
        rewrite(
            V2,
            &[b"info", b"file tree", b"one", b"sibling"],
            Some(Value::Dictionary(BTreeMap::new())),
        ),
        rewrite(
            V2,
            &[b"info", b"file tree", b""],
            Some(Value::Dictionary(BTreeMap::new())),
        ),
        rewrite(
            V2,
            &[b"info", b"file tree", b"one", b"", b"attr"],
            Some(Value::Bytes(b"l")),
        ),
    ] {
        assert!(Metainfo::parse(&wire, Limits::default()).is_err());
    }
    let advisory = rewrite(
        V2,
        &[b"info", b"name"],
        Some(Value::Bytes(b"../display only")),
    );
    assert!(Metainfo::parse(&advisory, Limits::default()).is_err());
    let document = bencode::decode(V2, Limits::default()).unwrap();
    let tree = document.get(b"info").unwrap().get(b"file tree").unwrap();
    let empty = Value::Dictionary(BTreeMap::from([(
        b"empty".as_slice(),
        tree.get(b"empty").unwrap().clone(),
    )]));
    let wire = rewrite(V2, &[b"info", b"file tree"], Some(empty));
    let wire = rewrite(
        &wire,
        &[b"piece layers"],
        Some(Value::Dictionary(BTreeMap::new())),
    );
    let parsed = Metainfo::parse(&wire, Limits::default()).unwrap();
    assert_eq!(parsed.layout().piece_count(), 0);
    assert_eq!(parsed.layout().total_length(), 0);
    assert_eq!(parsed.layout().files().len(), 1);
    assert!(parsed.verification(0).is_none());
}

#[test]
fn v2_destination_paths_root_only_multi_file_trees() {
    use std::collections::BTreeMap;
    let document = bencode::decode(V2, Limits::default()).unwrap();
    let tree = document.get(b"info").unwrap().get(b"file tree").unwrap();
    let file = tree.get(b"one").unwrap().clone();
    let single = Value::Dictionary(BTreeMap::from([(b"file".as_slice(), file.clone())]));
    let nested = Value::Dictionary(BTreeMap::from([(
        b"dir".as_slice(),
        Node::new(single.clone()),
    )]));
    let several = Value::Dictionary(BTreeMap::from([
        (b"a".as_slice(), file.clone()),
        (b"b".as_slice(), file),
    ]));
    for (tree, multi, paths) in [
        (single, false, vec![vec![b"file".as_slice()]]),
        (
            nested,
            true,
            vec![vec![b"dir".as_slice(), b"file".as_slice()]],
        ),
        (
            several,
            true,
            vec![vec![b"a".as_slice()], vec![b"b".as_slice()]],
        ),
    ] {
        let wire = rewrite(V2, &[b"info", b"file tree"], Some(tree));
        let wire = rewrite(
            &wire,
            &[b"piece layers"],
            Some(Value::Dictionary(BTreeMap::new())),
        );
        for name in [b"destination".as_slice(), b"../unsafe", b"", b".", b"a/b"] {
            let wire = rewrite(&wire, &[b"info", b"name"], Some(Value::Bytes(name)));
            if multi && name != b"destination" {
                assert!(Metainfo::parse(&wire, Limits::default()).is_err());
                assert!(OwnedMetainfo::parse(&wire, Limits::default()).is_err());
                continue;
            }
            let borrowed = Metainfo::parse(&wire, Limits::default()).unwrap();
            let owned = OwnedMetainfo::parse(&wire, Limits::default()).unwrap();
            for layout in [borrowed.layout(), owned.layout()] {
                assert_eq!(layout.is_multi_file(), multi);
                let actual: Vec<Vec<&[u8]>> = layout
                    .files()
                    .iter()
                    .filter(|file| file.kind() == FileKind::Data)
                    .map(|file| file.path().iter().map(|part| part.as_ref()).collect())
                    .collect();
                assert_eq!(actual, paths);
            }
        }
    }
}

#[test]
fn shared_pieces_roots_require_equal_file_lengths() {
    let (root, _) = fixture_layers();
    for wire in [
        rewrite(
            V2,
            &[b"info", b"file tree", b"one", b"", b"pieces root"],
            Some(Value::Bytes(&root)),
        ),
        rewrite(
            V2,
            &[b"info", b"file tree", b"block", b"", b"pieces root"],
            Some(Value::Bytes(&merkle::block_hash(&[0]))),
        ),
    ] {
        assert!(matches!(
            Metainfo::parse(&wire, Limits::default()),
            Err(metainfo::Error::InvalidField("pieces root length"))
        ));
        assert!(OwnedMetainfo::parse(&wire, Limits::default()).is_err());
    }
    // Equal roots with different lengths can also share a valid outer layer.
    let document = bencode::decode(V2, Limits::default()).unwrap();
    let odd = document
        .get(b"info")
        .unwrap()
        .get(b"file tree")
        .unwrap()
        .get(b"odd")
        .unwrap();
    let wire = rewrite(
        V2,
        &[b"info", b"file tree", b"copy"],
        Some(odd.value().clone()),
    );
    assert!(Metainfo::parse(&wire, Limits::default()).is_ok());
    let wire = rewrite(
        &wire,
        &[b"info", b"file tree", b"copy", b"", b"length"],
        Some(Value::Integer(65660)),
    );
    assert!(matches!(
        Metainfo::parse(&wire, Limits::default()),
        Err(metainfo::Error::InvalidField("pieces root length"))
    ));
    assert!(OwnedMetainfo::parse(&wire, Limits::default()).is_err());
}

#[test]
fn hybrid_checks_real_paths_and_offsets_but_not_padding_names() {
    let document = bencode::decode(HYBRID, Limits::default()).unwrap();
    let Value::List(original) = document
        .get(b"info")
        .unwrap()
        .get(b"files")
        .unwrap()
        .value()
    else {
        panic!("files");
    };
    let mut changed = original.clone();
    let Value::Dictionary(mut padding) = changed[1].value().clone() else {
        panic!("padding");
    };
    padding.insert(
        b"path",
        Node::new(Value::List(vec![Node::new(Value::Bytes(
            b"arbitrary padding name",
        ))])),
    );
    changed[1] = Node::new(Value::Dictionary(padding));
    let wire = rewrite(HYBRID, &[b"info", b"files"], Some(Value::List(changed)));
    let parsed = Metainfo::parse(&wire, Limits::default()).unwrap();
    let original_meta = Metainfo::parse(HYBRID, Limits::default()).unwrap();
    for (a, b) in parsed
        .layout()
        .files()
        .iter()
        .zip(original_meta.layout().files())
    {
        assert_eq!(a.path(), b.path());
        assert_eq!(a.offset(), b.offset());
    }
    let mut reordered = original.clone();
    reordered.swap(0, 2);
    let wire = rewrite(HYBRID, &[b"info", b"files"], Some(Value::List(reordered)));
    assert!(Metainfo::parse(&wire, Limits::default()).is_err());
}

#[test]
fn libtorrent_hybrid_retains_final_padding_without_extending_v2_data() {
    let meta = OwnedMetainfo::parse(LIBTORRENT_HYBRID, Limits::default()).unwrap();
    assert_eq!(meta.layout().piece_count(), 7);
    let last = meta.layout().files().last().unwrap();
    assert_eq!(last.kind(), FileKind::Padding);
    assert_eq!(last.length(), 32645);
    assert_eq!(meta.layout().piece_data_end(6), Some(123));
    let PieceVerification::Hybrid { v2, .. } = meta.verification(6).unwrap() else {
        panic!("hybrid tail");
    };
    assert_eq!(v2.blocks, 1);
    assert_eq!(v2.layer_hash, merkle::block_hash(&payload(123)));
    let document = meta.decode_document();
    let Value::List(files) = document
        .get(b"info")
        .unwrap()
        .get(b"files")
        .unwrap()
        .value()
    else {
        panic!("files");
    };
    let mut files = files.clone();
    let Value::Dictionary(mut last) = files.pop().unwrap().value().clone() else {
        panic!("tail padding");
    };
    last.insert(b"length", Node::new(Value::Integer(32646)));
    files.push(Node::new(Value::Dictionary(last)));
    let bad = rewrite(
        LIBTORRENT_HYBRID,
        &[b"info", b"files"],
        Some(Value::List(files)),
    );
    assert!(Metainfo::parse(&bad, Limits::default()).is_err());
}

#[test]
fn repeated_roots_cannot_expand_commitments_beyond_the_metadata_budget() {
    use std::collections::BTreeMap;
    let document = bencode::decode(V2, Limits::default()).unwrap();
    let odd = document
        .get(b"info")
        .unwrap()
        .get(b"file tree")
        .unwrap()
        .get(b"odd")
        .unwrap();
    let names: Vec<_> = (0..100)
        .map(|index| format!("{index:03}").into_bytes())
        .collect();
    let files: BTreeMap<_, _> = names
        .iter()
        .map(|name| (name.as_slice(), odd.clone()))
        .collect();
    let wire = rewrite(V2, &[b"info", b"file tree"], Some(Value::Dictionary(files)));
    let meta = Metainfo::parse(&wire, Limits::default()).unwrap();
    assert_eq!(meta.layout().piece_count(), 300);
    assert!(matches!(
        Metainfo::parse(
            &wire,
            Limits {
                max_input_len: wire.len(),
                ..Limits::default()
            }
        ),
        Err(metainfo::Error::MetadataLimit)
    ));
}

#[test]
fn btmh_and_dual_topic_magnets_validate_every_identity() {
    use spill::protocol::magnet::{Error, Magnet};
    let v1 = InfoHashV1::from_info_bytes(b"info");
    let v2 = InfoHashV2::from_info_bytes(b"info");
    let topic = format!("urn:btmh:1220{v2}");
    for (uri, expected) in [
        (format!("magnet:?xt={topic}"), TorrentIdentity::V2(v2)),
        (
            format!("magnet:?xt=urn:btih:{v1}&xt={topic}"),
            TorrentIdentity::Hybrid(v1, v2),
        ),
        (
            format!("magnet:?xt={topic}&xt=urn:btih:{v1}&xt={topic}"),
            TorrentIdentity::Hybrid(v1, v2),
        ),
    ] {
        let magnet = Magnet::parse(&uri, Default::default()).unwrap();
        assert_eq!(magnet.info_hash(), expected);
        assert!(magnet.info_hash().matches_info_bytes(b"info"));
    }
    let other = InfoHashV2::from_info_bytes(b"other");
    assert_eq!(
        Magnet::parse(
            &format!("magnet:?xt={topic}&xt=urn:btmh:1220{other}"),
            Default::default()
        )
        .unwrap_err(),
        Error::ConflictingHashes
    );
    for (hash, expected) in [
        (
            "11200000000000000000000000000000000000000000000000000000000000000000",
            Error::UnsupportedVersion,
        ),
        ("120100", Error::UnsupportedVersion),
        ("1220ff", Error::InvalidHash),
        ("122g", Error::InvalidHash),
        ("122", Error::InvalidHash),
        ("920020", Error::InvalidHash),
        ("", Error::InvalidHash),
    ] {
        assert_eq!(
            Magnet::parse(&format!("magnet:?xt=urn:btmh:{hash}"), Default::default()).unwrap_err(),
            expected
        );
    }
}

#[test]
fn merkle_roots_match_reference_creator_for_file_and_piece_boundaries() {
    let document = bencode::decode(V2, Limits::default()).unwrap();
    let tree = document.get(b"info").unwrap().get(b"file tree").unwrap();
    for (name, length) in [
        ("empty", 0),
        ("one", 1),
        ("block", BLOCK_LENGTH),
        ("piece", PIECE_LENGTH as usize),
        ("odd", 65536 + 123),
    ] {
        let file = tree.get(name.as_bytes()).unwrap().get(b"").unwrap();
        let data = payload(length);
        let leaves: Vec<_> = data.chunks(BLOCK_LENGTH).map(merkle::block_hash).collect();
        if length == 0 {
            assert!(file.get(b"pieces root").is_none());
            continue;
        }
        let expected = bytes(file.get(b"pieces root").unwrap());
        assert_eq!(merkle::root_from_layer(&leaves, 0), expected);
        if length <= PIECE_LENGTH as usize {
            continue;
        }
        let (root, layer) = fixture_layers();
        let pieces: Vec<_> = leaves
            .chunks(PIECE_LENGTH as usize / BLOCK_LENGTH)
            .map(|blocks| merkle::piece_hash(blocks, PIECE_LENGTH))
            .collect();
        assert_eq!(pieces, layer);
        assert_eq!(merkle::root_from_layer(&pieces, 1), root);
        assert_ne!(merkle::root_from_layer(&pieces, 0), root);
    }
    assert_eq!(
        hex(&merkle::padding_hash(1)),
        "f5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b"
    );
    assert_eq!(merkle::padding_hash(0), [0; 32]);
    let short_block = merkle::block_hash(&[0]);
    assert_ne!(
        merkle::piece_hash(&[short_block], PIECE_LENGTH),
        short_block
    );
    assert_ne!(short_block, merkle::block_hash(&vec![0; BLOCK_LENGTH]));
}

fn request() -> HashRequest {
    HashRequest {
        pieces_root: fixture_layers().0,
        base_layer: 1,
        index: 0,
        length: 2,
        proof_layers: 1,
    }
}

#[test]
fn hash_proofs_authenticate_left_right_and_whole_ranges() {
    let (root, layer) = fixture_layers();
    let pad = merkle::padding_hash(1);
    let left = merkle::parent(&layer[0], &layer[1]);
    let right = merkle::parent(&layer[2], &pad);
    for (index, nodes) in [
        (0, vec![layer[0], layer[1], right]),
        (2, vec![layer[2], pad, left]),
    ] {
        let request = HashRequest { index, ..request() };
        let hashes = nodes.concat();
        assert_eq!(request.response_hash_count(), Ok(3));
        assert_eq!(request.verify(65659, 512, &hashes), Ok(()));
        for offset in [0, 32, 64] {
            let mut bad = hashes.clone();
            bad[offset] ^= 1;
            assert_eq!(
                request.verify(65659, 512, &bad),
                Err(HashError::RootMismatch)
            );
        }
        assert_eq!(
            request.verify(65659, 512, &hashes[..hashes.len() - 1]),
            Err(HashError::HashCount)
        );
    }
    for proof_layers in [0, 1] {
        let request = HashRequest {
            length: 4,
            proof_layers,
            ..request()
        };
        assert_eq!(request.response_hash_count(), Ok(4));
        assert_eq!(
            request.verify(65659, 512, &[layer[0], layer[1], layer[2], pad].concat()),
            Ok(())
        );
    }
    let partial = HashRequest {
        proof_layers: 0,
        ..request()
    };
    assert_eq!(
        partial.verify(65659, 512, &[layer[0], layer[1]].concat()),
        Err(HashError::IncompleteProof)
    );

    let data = payload(65659);
    let leaves: Vec<_> = data[..PIECE_LENGTH as usize]
        .chunks(BLOCK_LENGTH)
        .map(merkle::block_hash)
        .collect();
    let request = HashRequest {
        pieces_root: root,
        base_layer: 0,
        proof_layers: 2,
        ..request()
    };
    let expected = [leaves[0], leaves[1], layer[1], right].concat();
    assert_eq!(request.verify(65659, 512, &expected), Ok(()));
}

#[test]
fn singleton_hash_frames_match_libtorrent_counts_and_authenticate_full_proofs() {
    use sha2::{Digest, Sha256};
    let mut leaves: Vec<[u8; 32]> = (0u32..1025)
        .map(|i| Sha256::digest(i.to_be_bytes()).into())
        .collect();
    leaves.resize(2048, [0; 32]);
    let mut tree = vec![leaves];
    while tree.last().unwrap().len() > 1 {
        let next = tree
            .last()
            .unwrap()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| Sha256::digest([pair[0], pair[1]].concat()).into())
            .collect();
        tree.push(next);
    }
    for index in [0, 17, 1024, 2047] {
        for proof_layers in [0, 9, 10] {
            let request = HashRequest {
                pieces_root: tree[11][0],
                base_layer: 0,
                index,
                length: 1,
                proof_layers,
            };
            let mut hashes = tree[0][index as usize].to_vec();
            let mut position = index as usize;
            for layer in tree.iter().take(proof_layers as usize + 1) {
                hashes.extend_from_slice(&layer[position ^ 1]);
                position /= 2;
            }
            assert_eq!(request.response_hash_count(), Ok(proof_layers + 2));
            assert_eq!(hashes.len(), (proof_layers as usize + 2) * 32);
            let message = Message::Hashes {
                request,
                hashes: &hashes,
            };
            let mut wire = vec![0; message.encoded_len(2048).unwrap()];
            let length = message.encode(&mut wire, 2048).unwrap();
            assert_eq!(Message::decode(&wire, 2048), Ok((message, length)));
            if proof_layers == 10 {
                assert_eq!(request.verify(1025 * 16384, 512, &hashes), Ok(()));
                hashes[0] ^= 1;
                assert_eq!(
                    request.verify(1025 * 16384, 512, &hashes),
                    Err(HashError::RootMismatch)
                );
            } else {
                assert_eq!(
                    request.verify(1025 * 16384, 512, &hashes),
                    Err(HashError::IncompleteProof)
                );
                if proof_layers == 9 {
                    // Libtorrent anchors this range in the preceding 512-hash proof.
                    let mut anchor: [u8; 32] = hashes[..32].try_into().unwrap();
                    let mut position = index as usize;
                    for sibling in hashes[32..].as_chunks::<32>().0 {
                        anchor = if position & 1 == 0 {
                            merkle::parent(&anchor, sibling)
                        } else {
                            merkle::parent(sibling, &anchor)
                        };
                        position /= 2;
                    }
                    assert_eq!(anchor, tree[10][position]);
                    let root = if position == 0 {
                        merkle::parent(&anchor, &tree[10][1])
                    } else {
                        merkle::parent(&tree[10][0], &anchor)
                    };
                    assert_eq!(root, request.pieces_root);
                }
            }
        }
    }
}

#[test]
fn hash_request_geometry_is_validated_before_work() {
    let good = request();
    assert_eq!(good.validate(65659, 512), Ok(()));
    for bad in [
        HashRequest { length: 0, ..good },
        HashRequest { length: 3, ..good },
        HashRequest { index: 1, ..good },
        HashRequest { index: 4, ..good },
        HashRequest {
            index: u32::MAX,
            ..good
        },
        HashRequest {
            base_layer: u32::MAX,
            ..good
        },
        HashRequest {
            proof_layers: u32::MAX,
            ..good
        },
        HashRequest {
            base_layer: 3,
            ..good
        },
        HashRequest {
            proof_layers: 2,
            ..good
        },
        HashRequest { length: 8, ..good },
    ] {
        assert!(bad.validate(65659, 512).is_err(), "{bad:?}");
    }
    assert_eq!(good.validate(65659, 1), Err(HashError::Length));
    for index in 0..4 {
        assert_eq!(
            HashRequest {
                index,
                length: 1,
                ..good
            }
            .validate(65659, 1),
            Ok(())
        );
    }
    assert_eq!(good.validate(0, 512), Err(HashError::Range));
    assert_eq!(good.validate(1, 512), Err(HashError::Layer));
    assert_eq!(
        HashRequest {
            base_layer: 0,
            proof_layers: 0,
            length: 2,
            index: 0,
            ..good
        }
        .validate(u64::MAX, 512),
        Ok(())
    );
}

fn wire_header(id: u8, length: u32) -> Vec<u8> {
    let mut wire = length.to_be_bytes().to_vec();
    wire.push(id);
    wire.extend_from_slice(&request().pieces_root);
    // base layer, index, length and proof layers, independent of the codec.
    wire.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1]);
    wire
}

#[test]
fn hash_frames_match_binary_vectors_and_preserve_trailing_frames() {
    let (_, layer) = fixture_layers();
    let sibling = merkle::parent(&layer[2], &merkle::padding_hash(1));
    let hashes = [layer[0], layer[1], sibling].concat();
    for (message, mut wire) in [
        (Message::HashRequest(request()), wire_header(21, 49)),
        (
            Message::Hashes {
                request: request(),
                hashes: &hashes,
            },
            wire_header(22, 145),
        ),
        (Message::HashReject(request()), wire_header(23, 49)),
    ] {
        if matches!(message, Message::Hashes { .. }) {
            wire.extend_from_slice(&hashes);
        }
        let length = wire.len();
        let mut output = vec![0x55; length + 4];
        assert_eq!(message.encode(&mut output, 1024), Ok(length));
        assert_eq!(&output[..length], wire);
        assert_eq!(&output[length..], &[0x55; 4]);
        wire.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(Message::decode(&wire, 1024), Ok((message, length)));
        for end in 0..length {
            assert!(Message::decode(&wire[..end], 1024).is_err());
        }
        let mut short = vec![0x55; length - 1];
        assert!(message.encode(&mut short, 1024).is_err());
        assert!(short.iter().all(|byte| *byte == 0x55));
        assert!(Message::decode(&wire, length as u32 - 5).is_err());
    }
}

#[test]
fn hash_frames_reject_malformed_headers_and_hash_counts() {
    for (id, length) in [
        (21, 48u32),
        (21, 50),
        (23, 48),
        (23, 50),
        (22, 49),
        (22, 112),
        (22, 114),
    ] {
        let mut wire = length.to_be_bytes().to_vec();
        wire.push(id);
        assert!(matches!(
            Message::decode(&wire, 1024),
            Err(MessageError::InvalidLength { .. })
        ));
    }
    for offset in [40, 44, 48] {
        let mut wire = wire_header(21, 49);
        wire[offset] = 0xff;
        assert!(Message::decode(&wire, 1024).is_err());
    }
    let mut wrong_count = wire_header(22, 113);
    wrong_count.extend_from_slice(&[0; 64]);
    assert_eq!(
        Message::decode(&wrong_count, 1024),
        Err(MessageError::HashRequest(HashError::HashCount))
    );
    let invalid = Message::Hashes {
        request: request(),
        hashes: &[0; 64],
    };
    let mut output = vec![0x55; 1024];
    assert!(invalid.encode(&mut output, 1024).is_err());
    assert!(output.iter().all(|byte| *byte == 0x55));
    for id in 21..=23 {
        assert_eq!(
            Message::Unknown { id, payload: &[] }.encoded_len(1024),
            Err(MessageError::KnownMessageId(id))
        );
    }
}
