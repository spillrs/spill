use crate::{payload::Temp, v2_peer as raw};
use spill::{
    engine::{
        profile::Profile,
        selection::FilePriority,
        session::{Limits, NetworkOptions, PeerEncryption, Session},
        torrent::{Destination, RunPolicy, TorrentHandle, TorrentOptions, TorrentSource},
    },
    protocol::{metainfo::OwnedMetainfo, peer::hash::HashRequest},
};
use std::time::Duration;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

const V2: &[u8] = include_bytes!("../protocol/fixtures/v2.torrent");
const LENGTHS: &[usize] = &[16384, 0, 65659, 1, 32768];

async fn session() -> Session {
    Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            utp: false,
            peer_encryption: PeerEncryption::Allowed,
            allow_local_network: true,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap()
}
async fn ready(handle: &mut TorrentHandle) {
    while !handle.snapshot().progress.ready {
        assert!(handle.changed().await, "{:?}", handle.snapshot());
    }
}
fn options(meta: &OwnedMetainfo, destination: Destination) -> TorrentOptions {
    let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta.clone()), destination);
    options.diagnostics = true;
    options.settings.trackers = false;
    options.settings.pex = false;
    options
}
async fn peer(session: &Session, meta: &OwnedMetainfo, id: u8) -> TcpStream {
    let mut peer = TcpStream::connect(session.local_addr()).await.unwrap();
    raw::initiate(&mut peer, meta.info_hash().wire_key(), id, false).await;
    peer
}
fn install(root: &Temp) {
    std::fs::create_dir(root.0.join("vectors")).unwrap();
    for (name, length) in ["block", "empty", "odd", "one", "piece"]
        .into_iter()
        .zip(LENGTHS)
    {
        std::fs::write(root.0.join("vectors").join(name), raw::payload(0, *length)).unwrap();
    }
}
fn hash_header(request: HashRequest) -> Vec<u8> {
    let mut bytes = request.pieces_root.to_vec();
    for value in [
        request.base_layer,
        request.index,
        request.length,
        request.proof_layers,
    ] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    bytes
}
fn odd_request(meta: &OwnedMetainfo) -> HashRequest {
    let root = meta
        .layout()
        .files()
        .iter()
        .find(|file| file.path().last().unwrap().as_ref() == b"odd")
        .unwrap()
        .pieces_root()
        .unwrap();
    HashRequest {
        pieces_root: root,
        base_layer: 1,
        index: 0,
        length: 2,
        proof_layers: 1,
    }
}

#[tokio::test]
async fn raw_downloads_cover_file_tails_destinations_and_large_piece_streaming() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let single = include_bytes!("../protocol/fixtures/v2-single.torrent");
        let name = b"4:name6:single";
        let offset = single
            .windows(name.len())
            .position(|bytes| bytes == name)
            .unwrap();
        let advisory = [
            &single[..offset],
            b"4:name15:../display only",
            &single[offset + name.len()..],
        ]
        .concat();
        for (wire, lengths, piece_length, files) in [
            (advisory.as_slice(), &[1][..], 32768, vec![("single", 1)]),
            (
                V2,
                LENGTHS,
                32768,
                vec![
                    ("vectors/block", 16384),
                    ("vectors/empty", 0),
                    ("vectors/odd", 65659),
                    ("vectors/one", 1),
                    ("vectors/piece", 32768),
                ],
            ),
            (
                include_bytes!("../protocol/fixtures/v2-leaves.torrent").as_slice(),
                LENGTHS,
                16384,
                vec![("vectors/odd", 65659)],
            ),
            (
                include_bytes!("../protocol/fixtures/v2-single.torrent").as_slice(),
                &[1][..],
                32768,
                vec![("single", 1)],
            ),
            (
                include_bytes!("../protocol/fixtures/v2-nested.torrent").as_slice(),
                &[1][..],
                32768,
                vec![("nested/dir/file", 1)],
            ),
            (
                include_bytes!("../protocol/fixtures/v2-large.torrent").as_slice(),
                &[16 * 1024 * 1024 + 123][..],
                16 * 1024 * 1024,
                vec![("large", 16 * 1024 * 1024 + 123)],
            ),
        ] {
            let meta = Profile::default().parse_owned(wire).unwrap();
            let root = Temp::new();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let key = meta.info_hash().wire_key();
            let pieces = raw::pieces(lengths, piece_length);
            let source = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                raw::serve(socket, key, 41, pieces, false).await
            });
            let session = session().await;
            let mut opts = options(&meta, Destination::Open(root.0.clone()));
            opts.peers.push(address);
            let handle = session.admit(opts).unwrap();
            assert!(session.torrent(meta.info_hash()).is_some());
            assert_eq!(handle.snapshot().identity, meta.info_hash());
            assert!(handle.snapshot().progress.metadata_ready);
            let mut wrong = *meta.info_hash().v2().unwrap().as_bytes();
            wrong[31] ^= 1;
            assert!(
                session
                    .torrent(spill::protocol::identity::TorrentIdentity::V2(
                        spill::protocol::identity::InfoHashV2::from_bytes(wrong)
                    ))
                    .is_none()
            );
            assert_eq!(
                session
                    .admit(options(&meta, Destination::Open(root.0.clone())))
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
            handle.wait_complete().await.unwrap();
            assert_eq!(
                handle.snapshot().progress.verified_pieces,
                meta.layout().piece_count()
            );
            for (path, length) in files {
                assert_eq!(
                    std::fs::read(root.0.join(path)).unwrap(),
                    raw::payload(0, length)
                );
            }
            assert!(!root.0.join(".pad").exists());
            assert!(!root.0.join("vectors/.pad").exists());
            assert!(
                handle
                    .snapshot()
                    .diagnostics
                    .as_ref()
                    .unwrap()
                    .trackers
                    .is_empty()
            );
            if wire == V2 {
                let target = Temp::new();
                let second = self::session().await;
                let mut opts = options(&meta, Destination::Open(target.0.clone()));
                opts.peers.push(session.local_addr());
                let leecher = second.admit(opts).unwrap();
                leecher.wait_complete().await.unwrap();
                assert_eq!(
                    std::fs::read(target.0.join("vectors/odd")).unwrap(),
                    raw::payload(0, 65659)
                );
                second.shutdown().await.unwrap();
            }
            if piece_length == 16384 {
                let mut peer = peer(&session, &meta, 51).await;
                let request = HashRequest {
                    base_layer: 0,
                    proof_layers: 2,
                    ..odd_request(&meta)
                };
                raw::send(&mut peer, 21, &hash_header(request)).await;
                let hashes = raw::next(&mut peer, 22).await;
                request.verify(65659, 512, &hashes[49..]).unwrap();
            }
            handle.stop().await.unwrap();
            let offline = Session::offline(Limits::default());
            let mut opts = options(
                &meta,
                Destination::Recover {
                    root: root.0.clone(),
                    state: handle.recovery().unwrap(),
                },
            );
            opts.run = RunPolicy::Recheck;
            let recheck = offline.admit(opts).unwrap();
            let outcome = recheck.outcome().await;
            assert!(outcome.recheck_finished && outcome.full_complete && outcome.error.is_none());
            offline.shutdown().await.unwrap();
            if handle.snapshot().progress.processed_payload_bytes == 0 {
                source.abort();
                assert!(source.await.unwrap_err().is_cancelled());
            } else {
                assert!(!source.await.unwrap().is_empty());
            }
            session.shutdown().await.unwrap();
        }
        let meta = Profile::default()
            .parse_owned(include_bytes!("../protocol/fixtures/v2-empty.torrent"))
            .unwrap();
        let root = Temp::new();
        let session = session().await;
        let handle = session
            .admit(options(&meta, Destination::Open(root.0.clone())))
            .unwrap();
        handle.wait_complete().await.unwrap();
        assert_eq!(handle.snapshot().progress.verified_pieces, 0);
        assert_eq!(std::fs::read(root.0.join("empty/a")).unwrap(), b"");
        assert_eq!(std::fs::read(root.0.join("empty/b")).unwrap(), b"");
        session.shutdown().await.unwrap();
    })
    .await
    .expect("v2 fixture transfers");
}

#[tokio::test]
async fn selective_v2_download_skips_independent_files_across_padding() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let key = meta.info_hash().wire_key();
        let source = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            raw::serve(socket, key, 42, raw::pieces(LENGTHS, 32768), false).await
        });
        let session = session().await;
        let mut opts = options(&meta, Destination::Open(root.0.clone()));
        opts.peers.push(address);
        opts.settings.file_priorities = meta
            .layout()
            .files()
            .iter()
            .map(|file| {
                if matches!(file.path().last().unwrap().as_ref(), b"odd" | b"piece") {
                    FilePriority::Normal
                } else {
                    FilePriority::Skip
                }
            })
            .collect();
        let handle = session.admit(opts).unwrap();
        handle.wait_complete().await.unwrap();
        assert_eq!(handle.snapshot().progress.wanted_bytes, 65659 + 32768);
        assert_eq!(handle.snapshot().progress.verified_pieces, 5);
        assert_eq!(
            std::fs::read(root.0.join("vectors/odd")).unwrap(),
            raw::payload(0, 65659)
        );
        handle.stop().await.unwrap();
        let requests = source.await.unwrap();
        assert!(requests.iter().all(|index| [1, 2, 3, 5].contains(index)));
        session.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn leaf_proofs_use_verified_copies_when_the_first_shared_root_is_skipped() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default()
            .parse_owned(include_bytes!("../protocol/fixtures/v2-copies.torrent"))
            .unwrap();
        let root = Temp::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let key = meta.info_hash().wire_key();
        let source = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            raw::serve(
                socket,
                key,
                53,
                raw::pieces(&[65659, 65659, 65659], 32768),
                false,
            )
            .await
        });
        let session = session().await;
        let mut opts = options(&meta, Destination::Open(root.0.clone()));
        opts.peers.push(address);
        opts.settings.file_priorities = meta
            .layout()
            .files()
            .iter()
            .map(|file| {
                if file.path().last().unwrap().as_ref() == b"b" {
                    FilePriority::Normal
                } else {
                    FilePriority::Skip
                }
            })
            .collect();
        let handle = session.admit(opts).unwrap();
        handle.wait_complete().await.unwrap();
        assert_eq!(
            handle.snapshot().progress.verified_piece_map.as_ref(),
            &[false, false, false, true, true, true, false, false, false]
        );
        assert!(!root.0.join("copies/.pad").exists());
        let mut peer = peer(&session, &meta, 54).await;
        raw::send(&mut peer, 2, &[]).await;
        raw::next(&mut peer, 1).await;
        raw::send(&mut peer, 6, &raw::request(3, 0, 16384)).await;
        assert_eq!(&raw::next(&mut peer, 7).await[9..], raw::payload(0, 16384));
        let request = HashRequest {
            pieces_root: meta.layout().files()[0].pieces_root().unwrap(),
            base_layer: 0,
            index: 0,
            length: 2,
            proof_layers: 2,
        };
        raw::send(&mut peer, 21, &hash_header(request)).await;
        let hashes = raw::next(&mut peer, 22).await;
        request.verify(65659, 512, &hashes[49..]).unwrap();
        handle.stop().await.unwrap();
        assert!(
            source
                .await
                .unwrap()
                .iter()
                .all(|piece| (3..6).contains(piece))
        );
        session.shutdown().await.unwrap();
    })
    .await
    .expect("shared-root selective proof serving");
}

#[tokio::test]
async fn v2_upload_duties_hash_proofs_and_unsolicited_piece_policy() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        install(&root);
        let session = session().await;
        let mut opts = options(&meta, Destination::Adopt(root.0.clone()));
        opts.settings.upload_limit = 32768;
        let mut handle = session.admit(opts).unwrap();
        ready(&mut handle).await;
        let mut peer = peer(&session, &meta, 61).await;
        assert_eq!(raw::next(&mut peer, 5).await, [5, 0xfc]);
        let block = raw::request(1, 0, 16384);
        raw::send(&mut peer, 6, &block).await;
        assert_eq!(&raw::next(&mut peer, 16).await[1..], &block);
        raw::send(&mut peer, 2, &[]).await;
        raw::next(&mut peer, 1).await;
        raw::send(&mut peer, 6, &block).await;
        let body = raw::next(&mut peer, 7).await;
        assert_eq!(&body[9..], raw::payload(0, 16384));
        let tail = raw::request(3, 122, 2);
        raw::send(&mut peer, 6, &tail).await;
        assert_eq!(&raw::next(&mut peer, 16).await[1..], &tail);
        let piece = odd_request(&meta);
        let leaf = HashRequest {
            base_layer: 0,
            proof_layers: 2,
            ..piece
        };
        // The leaf proof queued after a served chunk remains owed across a choke.
        raw::send(&mut peer, 21, &hash_header(leaf)).await;
        raw::send(&mut peer, 3, &[]).await;
        let mut choked = false;
        let hashes = loop {
            let body = raw::frame(&mut peer).await.unwrap();
            if body.first() == Some(&0) {
                choked = true;
            }
            if body.first() == Some(&22) {
                break body;
            }
        };
        assert!(choked, "queued leaf proof must survive our choke");
        assert_eq!(&hashes[1..49], &hash_header(leaf));
        leaf.verify(65659, 512, &hashes[49..]).unwrap();
        raw::send(&mut peer, 21, &hash_header(leaf)).await;
        assert_eq!(&raw::next(&mut peer, 23).await[1..], &hash_header(leaf));
        for request in [piece, HashRequest { index: 2, ..piece }] {
            raw::send(&mut peer, 21, &hash_header(request)).await;
            let hashes = raw::next(&mut peer, 22).await;
            assert_eq!(&hashes[1..49], &hash_header(request));
            request.verify(65659, 512, &hashes[49..]).unwrap();
        }
        raw::send(&mut peer, 2, &[]).await;
        raw::next(&mut peer, 1).await;
        let tail_leaf = HashRequest { index: 4, ..leaf };
        raw::send(&mut peer, 21, &hash_header(tail_leaf)).await;
        tail_leaf
            .verify(65659, 512, &raw::next(&mut peer, 22).await[49..])
            .unwrap();
        let small_root = meta
            .layout()
            .files()
            .iter()
            .find(|file| file.path().last().unwrap().as_ref() == b"piece")
            .unwrap()
            .pieces_root()
            .unwrap();
        let small = HashRequest {
            pieces_root: small_root,
            base_layer: 0,
            index: 0,
            length: 2,
            proof_layers: 0,
        };
        raw::send(&mut peer, 21, &hash_header(small)).await;
        let hashes = raw::next(&mut peer, 22).await;
        small.verify(32768, 512, &hashes[49..]).unwrap();
        for request in [
            HashRequest {
                pieces_root: [0; 32],
                ..piece
            },
            HashRequest {
                base_layer: 2,
                proof_layers: 0,
                ..piece
            },
            HashRequest { index: 8, ..leaf },
            HashRequest { length: 4, ..leaf },
            HashRequest {
                length: 1024,
                ..leaf
            },
        ] {
            raw::send(&mut peer, 21, &hash_header(request)).await;
            assert_eq!(&raw::next(&mut peer, 23).await[1..], &hash_header(request));
        }
        // Cancel and loss of interest cannot silently drop a v2 request.
        let requests: Vec<_> = (0..8)
            .map(|begin| raw::request(1, begin * 128, 128))
            .collect();
        let mut batch = Vec::new();
        for request in &requests {
            for id in [6, 8] {
                batch.extend_from_slice(&13u32.to_be_bytes());
                batch.push(id);
                batch.extend_from_slice(request);
            }
        }
        batch.extend_from_slice(&[0, 0, 0, 1, 3]);
        peer.write_all(&batch).await.unwrap();
        let mut answered = vec![false; requests.len()];
        while answered.iter().any(|answer| !answer) {
            let body = raw::frame(&mut peer).await.unwrap();
            match body.first() {
                Some(7) | Some(16) => {
                    let begin = u32::from_be_bytes(body[5..9].try_into().unwrap()) / 128;
                    answered[begin as usize] = true;
                }
                _ => {}
            }
        }
        drop(peer);
        let mut peer = self::peer(&session, &meta, 62).await;
        let mut unsolicited = raw::request(0, 0, 1)[..8].to_vec();
        unsolicited.push(0);
        raw::send(&mut peer, 7, &unsolicited).await;
        while raw::frame(&mut peer).await.is_ok() {}
        assert!(session.torrent(meta.info_hash()).is_some());
        session.shutdown().await.unwrap();
    })
    .await
    .expect("v2 wire duties");
}

#[tokio::test]
async fn raw_singleton_hash_requests_serve_any_index_with_libtorrent_sized_proofs() {
    use sha2::{Digest, Sha256};
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        install(&root);
        let session = session().await;
        let mut handle = session
            .admit(options(&meta, Destination::Adopt(root.0.clone())))
            .unwrap();
        ready(&mut handle).await;
        let mut peer = peer(&session, &meta, 63).await;
        let data = raw::payload(0, 65659);
        let mut leaves: Vec<[u8; 32]> = data
            .chunks(16384)
            .map(|block| Sha256::digest(block).into())
            .collect();
        leaves.resize(8, [0; 32]);
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
        assert_eq!(tree[3][0], odd_request(&meta).pieces_root);
        for base_layer in [1, 0] {
            if base_layer == 0 {
                let leaf = HashRequest {
                    base_layer: 0,
                    length: 1,
                    ..odd_request(&meta)
                };
                raw::send(&mut peer, 21, &hash_header(leaf)).await;
                assert_eq!(&raw::next(&mut peer, 23).await[1..], &hash_header(leaf));
                raw::send(&mut peer, 2, &[]).await;
                raw::next(&mut peer, 1).await;
            }
            for index in 0..(8 >> base_layer) {
                for proof_layers in [0, 2 - base_layer] {
                    let request = HashRequest {
                        base_layer,
                        index,
                        length: 1,
                        proof_layers,
                        ..odd_request(&meta)
                    };
                    raw::send(&mut peer, 21, &hash_header(request)).await;
                    let body = raw::next(&mut peer, 22).await;
                    assert_eq!(&body[1..49], &hash_header(request));
                    assert_eq!(body.len(), 49 + (proof_layers as usize + 2) * 32);
                    let mut expected = tree[base_layer as usize][index as usize].to_vec();
                    let mut position = index as usize;
                    for layer in tree
                        .iter()
                        .skip(base_layer as usize)
                        .take(proof_layers as usize + 1)
                    {
                        expected.extend_from_slice(&layer[position ^ 1]);
                        position /= 2;
                    }
                    assert_eq!(&body[49..], expected);
                    if proof_layers == 2 - base_layer {
                        request.verify(65659, 512, &body[49..]).unwrap();
                    } else {
                        assert_eq!(
                            request.verify(65659, 512, &body[49..]),
                            Err(spill::protocol::peer::hash::Error::IncompleteProof)
                        );
                    }
                }
            }
        }
        drop(peer);
        session.shutdown().await.unwrap();
    })
    .await
    .expect("singleton piece-layer and verified-leaf proof serving");
}

#[tokio::test]
async fn choked_leaf_requests_are_rejected_without_spending_payload_bandwidth() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default()
            .parse_owned(include_bytes!("../protocol/fixtures/v2-large.torrent"))
            .unwrap();
        let root = Temp::new();
        std::fs::write(
            root.0.join("large"),
            raw::payload(0, 16 * 1024 * 1024 + 123),
        )
        .unwrap();
        for upload_limit in [0, 16384] {
            let session = session().await;
            let mut opts = options(&meta, Destination::Adopt(root.0.clone()));
            opts.settings.upload_limit = upload_limit;
            let mut handle = session.admit(opts).unwrap();
            ready(&mut handle).await;
            let mut choked = peer(&session, &meta, 94).await;
            let request = HashRequest {
                pieces_root: meta.layout().files()[0].pieces_root().unwrap(),
                base_layer: 0,
                index: 0,
                length: 2,
                proof_layers: 10,
            };
            for _ in 0..3 {
                raw::send(&mut choked, 21, &hash_header(request)).await;
                assert_eq!(
                    &raw::next(&mut choked, 23).await[1..],
                    &hash_header(request)
                );
            }
            let mut granted = peer(&session, &meta, 95).await;
            raw::send(&mut granted, 2, &[]).await;
            raw::next(&mut granted, 1).await;
            raw::send(&mut granted, 6, &raw::request(0, 0, 16384)).await;
            let block = tokio::time::timeout(Duration::from_secs(2), raw::next(&mut granted, 7))
                .await
                .expect("rejected leaf requests must not consume the granted peer's upload credit");
            assert_eq!(&block[9..], raw::payload(0, 16384));
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("choked proof rejection and upload budget isolation");
}

#[tokio::test]
async fn unchoked_large_leaf_reads_spend_the_upload_budget() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default()
            .parse_owned(include_bytes!("../protocol/fixtures/v2-large.torrent"))
            .unwrap();
        let root = Temp::new();
        std::fs::write(
            root.0.join("large"),
            raw::payload(0, 16 * 1024 * 1024 + 123),
        )
        .unwrap();
        let session = session().await;
        let mut opts = options(&meta, Destination::Adopt(root.0.clone()));
        opts.settings.upload_limit = 16 * 1024;
        let mut handle = session.admit(opts).unwrap();
        ready(&mut handle).await;
        let mut peer = peer(&session, &meta, 91).await;
        raw::send(&mut peer, 2, &[]).await;
        raw::next(&mut peer, 1).await;
        let request = HashRequest {
            pieces_root: meta.layout().files()[0].pieces_root().unwrap(),
            base_layer: 0,
            index: 0,
            length: 2,
            proof_layers: 10,
        };
        raw::send(&mut peer, 21, &hash_header(request)).await;
        request
            .verify(
                16 * 1024 * 1024 + 123,
                512,
                &raw::next(&mut peer, 22).await[49..],
            )
            .unwrap();
        raw::send(&mut peer, 21, &hash_header(request)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), raw::frame(&mut peer))
                .await
                .is_err(),
            "a tiny hash reply must not authorize another unmetered 16 MiB read"
        );
        session.shutdown().await.unwrap();
    })
    .await
    .expect("leaf read bandwidth accounting");
}

#[tokio::test]
async fn duplicate_v2_requests_disconnect_instead_of_rejecting_a_live_upload() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        install(&root);
        let session = session().await;
        let mut opts = options(&meta, Destination::Adopt(root.0.clone()));
        opts.settings.upload_limit = 1;
        let mut handle = session.admit(opts).unwrap();
        ready(&mut handle).await;
        let mut peer = peer(&session, &meta, 92).await;
        raw::send(&mut peer, 2, &[]).await;
        raw::next(&mut peer, 1).await;
        raw::send(&mut peer, 6, &raw::request(1, 0, 16384)).await;
        raw::next(&mut peer, 7).await;
        let mut batch = Vec::new();
        // Backpressure keeps the final request queued until its duplicate arrives.
        for begin in (0..256).chain(std::iter::once(255)) {
            batch.extend_from_slice(&13u32.to_be_bytes());
            batch.push(6);
            batch.extend_from_slice(&raw::request(1, begin, 1));
        }
        peer.write_all(&batch).await.unwrap();
        while raw::frame(&mut peer).await.is_ok() {}
        assert!(session.torrent(meta.info_hash()).is_some());
        session.shutdown().await.unwrap();
    })
    .await
    .expect("duplicate v2 request retirement");
}

#[tokio::test]
async fn fast_and_v2_uploads_choke_before_rejecting_pending_requests() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for fast in [true, false] {
            let root = Temp::new();
            let meta = if fast {
                let payload = raw::payload(0, 32768);
                std::fs::write(root.0.join("x"), &payload).unwrap();
                Profile::default()
                    .parse_owned(&crate::payload::seed_metainfo("x", &payload, None))
                    .unwrap()
            } else {
                install(&root);
                Profile::default().parse_owned(V2).unwrap()
            };
            let session = session().await;
            let mut opts = options(&meta, Destination::Adopt(root.0.clone()));
            opts.settings.pex = false;
            let mut handle = session.admit(opts).unwrap();
            ready(&mut handle).await;
            let mut peer = TcpStream::connect(session.local_addr()).await.unwrap();
            raw::initiate(&mut peer, meta.info_hash().wire_key(), 93, fast).await;
            raw::send(&mut peer, 2, &[]).await;
            raw::next(&mut peer, 1).await;
            let mut batch = Vec::new();
            for begin in 0..256 {
                batch.extend_from_slice(&13u32.to_be_bytes());
                batch.push(6);
                batch.extend_from_slice(&raw::request(u32::from(!fast), begin, 16384));
            }
            batch.extend_from_slice(&[0, 0, 0, 1, 3]);
            peer.write_all(&batch).await.unwrap();
            let mut choked = false;
            let mut rejected = false;
            for _ in 0..256 {
                loop {
                    let frame = raw::frame(&mut peer).await.unwrap();
                    match frame.first() {
                        Some(0) => choked = true,
                        Some(16) => {
                            assert!(choked, "reject preceded choke (fast={fast})");
                            rejected = true;
                            break;
                        }
                        Some(7) => break,
                        _ => {}
                    }
                }
            }
            assert!(rejected, "queued requests must be rejected after choking");
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("choke before reject in both wire modes");
}

#[tokio::test]
async fn no_fast_seed_can_omit_choke_rejects_without_blocking_honest_peers_or_late_pieces() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        let slow = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let honest = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let slow_addr = slow.local_addr().unwrap();
        let honest_addr = honest.local_addr().unwrap();
        let key = meta.info_hash().wire_key();
        let (choked, choking) = tokio::sync::oneshot::channel();
        let (late, send_late) = tokio::sync::oneshot::channel();
        let (canceled, canceling) = tokio::sync::oneshot::channel();
        let slow = tokio::spawn(async move {
            let (mut socket, _) = slow.accept().await.unwrap();
            raw::accept(&mut socket, key, 96).await;
            raw::send(&mut socket, 5, &[0x40]).await;
            raw::send(&mut socket, 1, &[]).await;
            let mut pending = Vec::new();
            for _ in 0..2 {
                pending.push(raw::next(&mut socket, 6).await);
            }
            pending.sort_by_key(|body| u32::from_be_bytes(body[5..9].try_into().unwrap()));
            assert_eq!(&pending[0][1..], &raw::request(1, 0, 16384));
            assert_eq!(&pending[1][1..], &raw::request(1, 16384, 16384));
            raw::send(&mut socket, 0, &[]).await;
            choked.send(()).unwrap();
            send_late.await.unwrap();
            raw::send(&mut socket, 1, &[]).await;
            let mut body = pending[0][1..9].to_vec();
            body.extend_from_slice(&raw::payload(0, 16384));
            raw::send(&mut socket, 7, &body).await;
            let mut canceled = Some(canceled);
            // Leave the other request unanswered even after its cancel.
            while let Ok(body) = raw::frame(&mut socket).await {
                if body.first() == Some(&8)
                    && body[1..] == pending[1][1..]
                    && let Some(canceled) = canceled.take()
                {
                    canceled.send(()).unwrap();
                }
            }
        });
        let session = session().await;
        let mut opts = options(&meta, Destination::Open(root.0.clone()));
        opts.peers = vec![slow_addr, honest_addr];
        let mut handle = session.admit(opts).unwrap();
        choking.await.unwrap();
        let (offer, offering) = tokio::sync::oneshot::channel();
        let honest = tokio::spawn(async move {
            let (socket, _) = honest.accept().await.unwrap();
            raw::serve_with_delayed_have(socket, key, raw::pieces(LENGTHS, 32768), offering).await;
        });
        while handle.snapshot().progress.verified_pieces < 5 {
            assert!(handle.changed().await);
        }
        late.send(()).unwrap();
        loop {
            let status = handle.snapshot();
            if let Some(peer) = status
                .diagnostics
                .as_ref()
                .unwrap()
                .peers
                .iter()
                .find(|peer| peer.endpoint == Some(slow_addr))
                && peer.detail.downloaded_bytes == 16384
            {
                assert!(!peer.detail.fast_extension && !peer.detail.retiring);
                assert_eq!(peer.detail.authorized_requests, 1);
                break;
            }
            assert!(handle.changed().await);
        }
        offer.send(()).unwrap();
        handle.wait_complete().await.unwrap();
        canceling.await.unwrap();
        assert_eq!(
            std::fs::read(root.0.join("vectors/odd")).unwrap(),
            raw::payload(0, 65659)
        );
        handle.stop().await.unwrap();
        slow.await.unwrap();
        honest.await.unwrap();
        session.shutdown().await.unwrap();
    })
    .await
    .expect("no-Fast choke recovery with a retained late-piece authorization");
}

#[tokio::test]
async fn unverified_payload_is_not_uploaded_and_corruption_is_redownloaded() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        let session = session().await;
        // Corrupt-source IP bans must not also cover the honest peer.
        let bad = TcpListener::bind("[::1]:0").await.unwrap();
        let good = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut opts = options(&meta, Destination::Open(root.0.clone()));
        opts.peers = vec![bad.local_addr().unwrap(), good.local_addr().unwrap()];
        let mut handle = session.admit(opts).unwrap();
        ready(&mut handle).await;
        let mut peer = peer(&session, &meta, 63).await;
        assert_eq!(raw::next(&mut peer, 5).await, [5, 8]);
        raw::send(&mut peer, 2, &[]).await;
        raw::send(&mut peer, 6, &raw::request(1, 0, 16384)).await;
        raw::next(&mut peer, 16).await;
        let leaf = HashRequest {
            base_layer: 0,
            proof_layers: 2,
            ..odd_request(&meta)
        };
        raw::send(&mut peer, 21, &hash_header(leaf)).await;
        raw::next(&mut peer, 23).await;
        drop(peer);
        let key = meta.info_hash().wire_key();
        let bad = tokio::spawn(async move {
            let (socket, _) = bad.accept().await.unwrap();
            raw::serve(socket, key, 64, raw::pieces(LENGTHS, 32768), true).await
        });
        while handle.snapshot().progress.hash_failed_pieces == 0 {
            assert!(handle.changed().await);
        }
        let good = tokio::spawn(async move {
            let (socket, _) = good.accept().await.unwrap();
            raw::serve(socket, key, 65, raw::pieces(LENGTHS, 32768), false).await
        });
        handle.wait_complete().await.unwrap();
        assert!(handle.snapshot().progress.hash_failed_pieces > 0);
        assert_eq!(
            std::fs::read(root.0.join("vectors/odd")).unwrap(),
            raw::payload(0, 65659)
        );
        handle.stop().await.unwrap();
        bad.await.unwrap();
        assert!(!good.await.unwrap().is_empty());
        session.shutdown().await.unwrap();
    })
    .await
    .expect("v2 corruption recovery");
}

#[tokio::test]
async fn hash_request_flood_is_bounded_even_while_choked() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        install(&root);
        let session = session().await;
        let mut opts = options(&meta, Destination::Adopt(root.0.clone()));
        opts.settings.upload_limit = 1;
        let mut handle = session.admit(opts).unwrap();
        ready(&mut handle).await;
        let mut peer = peer(&session, &meta, 71).await;
        let request = hash_header(odd_request(&meta));
        let mut batch = Vec::new();
        for _ in 0..4500 {
            batch.extend_from_slice(&49u32.to_be_bytes());
            batch.push(21);
            batch.extend_from_slice(&request);
        }
        peer.write_all(&batch).await.unwrap();
        let mut responses = 0;
        while let Ok(body) = raw::frame(&mut peer).await {
            if body.first() == Some(&22) {
                responses += 1;
            }
        }
        assert!(responses > 0 && responses < 4500);
        assert!(session.torrent(meta.info_hash()).is_some());
        session.shutdown().await.unwrap();
    })
    .await
    .expect("bounded v2 hash queue");
}

#[tokio::test]
async fn unsupported_resume_format_rechecks_bound_payload_instead_of_trusting_saved_bits() {
    tokio::time::timeout(Duration::from_secs(10), async {
        use sha1::{Digest, Sha1};
        use spill::protocol::resume;
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        install(&root);
        let seed = session().await;
        let handle = seed
            .admit(options(&meta, Destination::Adopt(root.0.clone())))
            .unwrap();
        handle.wait_complete().await.unwrap();
        handle.stop().await.unwrap();
        let saved = handle.recovery().unwrap();
        seed.shutdown().await.unwrap();
        let fingerprint = resume::decode(&meta, saved.resume.as_ref().unwrap())
            .unwrap()
            .fingerprint;
        let data_files = meta
            .layout()
            .files()
            .iter()
            .filter(|file| file.kind() == spill::protocol::metainfo::FileKind::Data)
            .count();
        assert_eq!(fingerprint.len(), data_files * 40);
        assert!(data_files < meta.layout().files().len());
        for (magic, expected) in [(b"SPILLR01", 0), (b"SPILLR00", 6)] {
            let mut state = saved.clone();
            let mut record = resume::encode(&meta, &[0], &fingerprint).unwrap();
            record[..8].copy_from_slice(magic);
            let end = record.len() - 20;
            let checksum = Sha1::digest(&record[..end]);
            record[end..].copy_from_slice(&checksum);
            state.resume = Some(record);
            let session = session().await;
            let mut handle = session
                .admit(options(
                    &meta,
                    Destination::Recover {
                        root: root.0.clone(),
                        state,
                    },
                ))
                .unwrap();
            ready(&mut handle).await;
            assert_eq!(handle.snapshot().progress.verified_pieces, expected);
            handle.stop().await.unwrap();
            assert_eq!(
                &handle.recovery().unwrap().resume.unwrap()[..8],
                b"SPILLR01"
            );
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("resume version rejection triggers a full recheck");
}

#[tokio::test]
async fn resume_recovery_and_offline_recheck_verify_v2_disk_content() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let root = Temp::new();
        let source = Temp::new();
        install(&source);
        let seed = session().await;
        let mut seeder = seed
            .admit(options(&meta, Destination::Adopt(source.0.clone())))
            .unwrap();
        ready(&mut seeder).await;
        let session = session().await;
        let mut opts = options(&meta, Destination::Open(root.0.clone()));
        opts.peers.push(seed.local_addr());
        let handle = session.admit(opts).unwrap();
        handle.wait_complete().await.unwrap();
        handle.stop().await.unwrap();
        let saved = handle.recovery().unwrap();
        assert_eq!(&saved.resume.as_ref().unwrap()[..8], b"SPILLR01");
        session.shutdown().await.unwrap();
        seed.shutdown().await.unwrap();
        let session = self::session().await;
        let mut recovered = session
            .admit(options(
                &meta,
                Destination::Recover {
                    root: root.0.clone(),
                    state: saved.clone(),
                },
            ))
            .unwrap();
        ready(&mut recovered).await;
        assert_eq!(recovered.snapshot().progress.verified_pieces, 6);
        assert_eq!(recovered.snapshot().progress.processed_payload_bytes, 0);
        let mut peer = peer(&session, &meta, 81).await;
        raw::send(&mut peer, 2, &[]).await;
        raw::next(&mut peer, 1).await;
        raw::send(
            &mut peer,
            21,
            &hash_header(HashRequest {
                base_layer: 0,
                proof_layers: 2,
                ..odd_request(&meta)
            }),
        )
        .await;
        let hashes = raw::next(&mut peer, 22).await;
        HashRequest {
            base_layer: 0,
            proof_layers: 2,
            ..odd_request(&meta)
        }
        .verify(65659, 512, &hashes[49..])
        .unwrap();
        recovered.stop().await.unwrap();
        session.shutdown().await.unwrap();
        let saved = recovered.recovery().unwrap();
        let mut bytes = std::fs::read(root.0.join("vectors/odd")).unwrap();
        bytes[0] ^= 1;
        std::fs::write(root.0.join("vectors/odd"), bytes).unwrap();
        for run in [RunPolicy::Persistent, RunPolicy::Recheck] {
            let session = if run == RunPolicy::Recheck {
                Session::offline(Limits::default())
            } else {
                self::session().await
            };
            let mut opts = options(
                &meta,
                Destination::Recover {
                    root: root.0.clone(),
                    state: saved.clone(),
                },
            );
            opts.run = run;
            let mut handle = session.admit(opts).unwrap();
            if run == RunPolicy::Recheck {
                let outcome = handle.outcome().await;
                assert!(
                    outcome.recheck_finished && !outcome.full_complete && outcome.error.is_none()
                );
            } else {
                ready(&mut handle).await;
                handle.stop().await.unwrap();
            }
            assert_eq!(handle.snapshot().progress.verified_pieces, 5);
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("v2 resume and recheck");
}
