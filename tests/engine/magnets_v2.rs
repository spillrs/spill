use crate::{metadata_peer, payload::Temp};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, PeerEncryption, Session},
        settings::TorrentSettings,
        torrent::{Destination, TorrentOptions, TorrentSource},
    },
    protocol::{identity::TorrentIdentity, magnet::Magnet, metainfo::OwnedMetainfo},
};
use std::time::Duration;
use tokio::net::TcpListener;

const V2: &[u8] = include_bytes!("../protocol/fixtures/v2.torrent");
const HYBRID: &[u8] = include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent");

async fn session() -> Session {
    Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            allow_local_network: true,
            utp: false,
            peer_encryption: PeerEncryption::Allowed,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap()
}
fn settings() -> TorrentSettings {
    TorrentSettings {
        dht: false,
        pex: false,
        web_seeds: false,
        ..Default::default()
    }
}
fn magnet(meta: &OwnedMetainfo, topic: &str, tracker: &str) -> Magnet {
    let mut topics = Vec::new();
    if topic != "v2" {
        topics.push(format!("xt=urn:btih:{}", meta.info_hash().v1().unwrap()));
    }
    if topic != "v1" {
        topics.push(format!(
            "xt=urn:btmh:1220{}",
            meta.info_hash().v2().unwrap()
        ));
    }
    Magnet::parse(
        &format!("magnet:?{}&tr={tracker}", topics.join("&")),
        Default::default(),
    )
    .unwrap()
}
async fn source(
    meta: &OwnedMetainfo,
    fault: u8,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    (
        address,
        tokio::spawn(metadata_peer::serve_full(listener, meta.clone(), fault)),
    )
}

#[tokio::test]
async fn shared_root_ranges_are_disjoint_and_reassigned_after_reject_or_timeout() {
    use crate::v2_peer as raw;
    use sha2::{Digest, Sha256};
    use spill::protocol::bencode::{self, Node, Value};
    use std::collections::BTreeMap;
    tokio::time::timeout(Duration::from_secs(60), async {
        let layer: Vec<[u8; 32]> = (0u32..1027)
            .map(|index| Sha256::digest(index.to_be_bytes()).into())
            .collect();
        let padding: [u8; 32] = Sha256::digest([0; 64]).into();
        let mut nodes = layer.clone();
        nodes.resize(nodes.len().next_power_of_two(), padding);
        while nodes.len() > 1 {
            nodes = nodes
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| Sha256::digest([pair[0], pair[1]].concat()).into())
                .collect();
        }
        let root = nodes[0];
        let leaf = Node::new(Value::Dictionary(BTreeMap::from([(
            b"".as_slice(),
            Node::new(Value::Dictionary(BTreeMap::from([
                (
                    b"length".as_slice(),
                    Node::new(Value::Integer(1027 * 32768)),
                ),
                (b"pieces root".as_slice(), Node::new(Value::Bytes(&root))),
            ]))),
        )])));
        let doc = Node::new(Value::Dictionary(BTreeMap::from([
            (
                b"info".as_slice(),
                Node::new(Value::Dictionary(BTreeMap::from([
                    (
                        b"file tree".as_slice(),
                        Node::new(Value::Dictionary(BTreeMap::from([
                            (b"a".as_slice(), leaf.clone()),
                            (b"b".as_slice(), leaf),
                        ]))),
                    ),
                    (b"meta version".as_slice(), Node::new(Value::Integer(2))),
                    (b"name".as_slice(), Node::new(Value::Bytes(b"copies"))),
                    (b"piece length".as_slice(), Node::new(Value::Integer(32768))),
                ]))),
            ),
            (
                b"piece layers".as_slice(),
                Node::new(Value::Dictionary(BTreeMap::from([(
                    root.as_slice(),
                    Node::new(Value::Bytes(layer.as_flattened())),
                )]))),
            ),
        ])));
        let meta = Profile::default()
            .parse_owned(&bencode::encode(&doc, Default::default()).unwrap())
            .unwrap();
        for reject in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            // Separate ports keep discovery from collapsing the three independent peers.
            let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let third = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (url, tracker) = metadata_peer::tracker(&[
                address,
                second.local_addr().unwrap(),
                third.local_addr().unwrap(),
            ])
            .await;
            let (requests, mut received) = tokio::sync::mpsc::unbounded_channel();
            let ready = std::sync::Arc::new(tokio::sync::Barrier::new(3));
            let mut sources = tokio::task::JoinSet::new();
            for (id, listener) in [listener, second, third].into_iter().enumerate() {
                let meta = meta.clone();
                let requests = requests.clone();
                let ready = ready.clone();
                sources.spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    raw::accept(&mut socket, meta.info_hash().wire_key(), 90 + id as u8).await;
                    ready.wait().await;
                    let hello = format!(
                        "d1:md11:ut_metadatai3ee13:metadata_sizei{}ee",
                        meta.info_bytes().len()
                    );
                    raw::send(&mut socket, 20, &[&[0], hello.as_bytes()].concat()).await;
                    let mut metadata_id = None;
                    while let Ok(frame) = raw::frame(&mut socket).await {
                        match frame.as_slice() {
                            [20, 0, payload @ ..] => {
                                let doc = bencode::decode(payload, Default::default()).unwrap();
                                let Value::Integer(id) =
                                    doc.get(b"m").unwrap().get(b"ut_metadata").unwrap().value()
                                else {
                                    unreachable!()
                                };
                                metadata_id = Some(*id as u8);
                            }
                            [20, 3, ..] => {
                                let mut reply = vec![metadata_id.unwrap()];
                                reply.extend_from_slice(
                                    format!(
                                        "d8:msg_typei1e5:piecei0e10:total_sizei{}ee",
                                        meta.info_bytes().len()
                                    )
                                    .as_bytes(),
                                );
                                reply.extend_from_slice(meta.info_bytes());
                                raw::send(&mut socket, 20, &reply).await;
                            }
                            [21, header @ ..] => {
                                let (response, wait) = tokio::sync::oneshot::channel();
                                requests.send((id, header.to_vec(), response)).unwrap();
                                match wait.await {
                                    Ok(true) => {
                                        raw::send(
                                            &mut socket,
                                            22,
                                            &metadata_peer::hash_reply(&meta, header),
                                        )
                                        .await
                                    }
                                    Ok(false) => raw::send(&mut socket, 23, header).await,
                                    Err(_) => return,
                                }
                            }
                            _ => {}
                        }
                    }
                });
            }
            let session = session().await;
            let handle = session
                .inspect(magnet(&meta, "v2", &url), Profile::default(), settings())
                .unwrap();
            let mut assigned = BTreeMap::new();
            for _ in 0..3 {
                let (peer, header, response) = received.recv().await.unwrap();
                assert_eq!(&header[..32], &root);
                let index = u32::from_be_bytes(header[36..40].try_into().unwrap());
                assert!(
                    assigned.insert(index, (peer, response)).is_none(),
                    "no duplicate in-flight ranges"
                );
            }
            assert_eq!(assigned.keys().copied().collect::<Vec<_>>(), [0, 512, 1024]);
            let (tail_peer, response) = assigned.remove(&1024).unwrap();
            response.send(true).unwrap();
            while handle.snapshot().progress.hash_bytes != 3 * 32 {
                tokio::task::yield_now().await;
            }
            let (mut duplicate_peer, header, mut duplicate_reply) = received.recv().await.unwrap();
            assert_eq!(duplicate_peer, tail_peer);
            let duplicate_index = u32::from_be_bytes(header[36..40].try_into().unwrap());
            assert!(matches!(duplicate_index, 0 | 512));
            assigned.remove(&0).unwrap().1.send(true).unwrap();
            while handle.snapshot().progress.hash_bytes != 515 * 32 {
                tokio::task::yield_now().await;
            }
            if duplicate_index == 0 {
                // The late reply releases its claim even though range 0 is stored.
                duplicate_reply.send(true).unwrap();
                let (peer, header, response) = received.recv().await.unwrap();
                assert_ne!(peer, assigned.get(&512).unwrap().0);
                assert_eq!(u32::from_be_bytes(header[36..40].try_into().unwrap()), 512);
                duplicate_peer = peer;
                duplicate_reply = response;
            }
            assert!(received.try_recv().is_err(), "at most two claims per range");
            let (failed_peer, response) = assigned.remove(&512).unwrap();
            let pending_duplicate = if reject {
                response.send(false).unwrap();
                let (peer, header, response) = received.recv().await.unwrap();
                assert_ne!(peer, failed_peer);
                assert_ne!(peer, duplicate_peer);
                assert_eq!(u32::from_be_bytes(header[36..40].try_into().unwrap()), 512);
                duplicate_reply.send(true).unwrap();
                Some(response)
            } else {
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(31)).await;
                tokio::time::resume();
                drop(response);
                drop(duplicate_reply);
                let (peer, header, response) = received.recv().await.unwrap();
                assert_ne!(peer, failed_peer);
                assert_ne!(peer, duplicate_peer);
                assert_eq!(u32::from_be_bytes(header[36..40].try_into().unwrap()), 512);
                response.send(true).unwrap();
                None
            };
            let resolved = handle.wait_metadata().await.unwrap();
            drop(pending_duplicate);
            assert_eq!(resolved.info_hash(), meta.info_hash());
            assert_eq!(handle.snapshot().progress.hash_bytes, 1027 * 32);
            for index in 0..meta.layout().piece_count() {
                assert_eq!(resolved.verification(index), meta.verification(index));
            }
            assert!(handle.outcome().await.cleanup_complete);
            session.shutdown().await.unwrap();
            sources.abort_all();
            tracker.abort();
        }
    })
    .await
    .expect("disjoint authenticated ranges survive peer rejection and timeout");
}

#[tokio::test]
async fn metadata_resolution_preserves_pending_dials_and_inflight_handshakes() {
    use crate::v2_peer as raw;
    use spill::protocol::utp::{Kind, Packet};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UdpSocket;
    async fn reset(socket: &UdpSocket, bytes: &[u8], address: std::net::SocketAddr) {
        let mut header = Packet::decode(bytes).unwrap().header;
        assert_eq!(header.kind, Kind::Syn);
        header.kind = Kind::Reset;
        let mut reply = Vec::new();
        Packet {
            header,
            sack: None,
            payload: &[],
        }
        .encode(&mut reply);
        socket.send_to(&reply, address).await.unwrap();
    }
    async fn layers(
        listener: TcpListener,
        meta: OwnedMetainfo,
        ready: std::sync::Arc<tokio::sync::Barrier>,
        id: u8,
    ) {
        let (mut socket, _) = listener.accept().await.unwrap();
        raw::accept(&mut socket, meta.info_hash().v2().unwrap().wire_key(), id).await;
        ready.wait().await;
        while let Ok(frame) = raw::frame(&mut socket).await {
            if let [21, header @ ..] = frame.as_slice() {
                raw::send(&mut socket, 22, &metadata_peer::hash_reply(&meta, header)).await;
            }
        }
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pending = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let inflight = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap_udp = UdpSocket::bind(bootstrap.local_addr().unwrap())
            .await
            .unwrap();
        let pending_udp = UdpSocket::bind(pending.local_addr().unwrap())
            .await
            .unwrap();
        let inflight_udp = UdpSocket::bind(inflight.local_addr().unwrap())
            .await
            .unwrap();
        let (url, tracker) = metadata_peer::tracker(&[
            bootstrap.local_addr().unwrap(),
            pending.local_addr().unwrap(),
            inflight.local_addr().unwrap(),
        ])
        .await;
        let (syn_seen, syn_wait) = tokio::sync::oneshot::channel();
        let (handshake_seen, handshake_wait) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let pending_reset = tokio::spawn(async move {
            let mut bytes = [0; 1500];
            let (n, address) = pending_udp.recv_from(&mut bytes).await.unwrap();
            syn_seen.send(()).unwrap();
            released.await.unwrap();
            reset(&pending_udp, &bytes[..n], address).await;
        });
        let bootstrap_reset = tokio::spawn(async move {
            syn_wait.await.unwrap();
            handshake_wait.await.unwrap();
            let mut bytes = [0; 1500];
            loop {
                let (n, address) = bootstrap_udp.recv_from(&mut bytes).await.unwrap();
                reset(&bootstrap_udp, &bytes[..n], address).await;
            }
        });
        let inflight_reset = tokio::spawn(async move {
            let mut bytes = [0; 1500];
            loop {
                let (n, address) = inflight_udp.recv_from(&mut bytes).await.unwrap();
                reset(&inflight_udp, &bytes[..n], address).await;
            }
        });
        let ready = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let pending_source = tokio::spawn(layers(pending, meta.clone(), ready.clone(), 92));
        let inflight_meta = meta.clone();
        let inflight_source = tokio::spawn(async move {
            let (mut socket, _) = inflight.accept().await.unwrap();
            let mut hello = [0; 68];
            socket.read_exact(&mut hello).await.unwrap();
            assert_eq!(
                &hello[28..48],
                &inflight_meta.info_hash().v2().unwrap().wire_key()
            );
            handshake_seen.send(()).unwrap();
            hello[20..28].copy_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
            hello[48..].fill(93);
            socket.write_all(&hello).await.unwrap();
            ready.wait().await;
            while let Ok(frame) = raw::frame(&mut socket).await {
                if let [21, header @ ..] = frame.as_slice() {
                    raw::send(
                        &mut socket,
                        22,
                        &metadata_peer::hash_reply(&inflight_meta, header),
                    )
                    .await;
                }
            }
        });
        let bootstrap_source = tokio::spawn(metadata_peer::serve_full(bootstrap, meta.clone(), 4));
        let session = Session::bind(
            NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(),
                allow_local_network: true,
                utp: true,
                peer_encryption: PeerEncryption::Allowed,
                ..Default::default()
            },
            Limits::default(),
        )
        .await
        .unwrap();
        let handle = session
            .inspect(magnet(&meta, "v2", &url), Profile::default(), settings())
            .unwrap();
        while !handle.snapshot().progress.info_validated {
            tokio::task::yield_now().await;
        }
        bootstrap_source.abort();
        release.send(()).unwrap();
        let resolved = handle.wait_metadata().await.unwrap();
        assert_eq!(resolved.info_hash(), meta.info_hash());
        assert!(handle.outcome().await.cleanup_complete);
        session.shutdown().await.unwrap();
        pending_reset.await.unwrap();
        pending_source.await.unwrap();
        inflight_source.await.unwrap();
        bootstrap_reset.abort();
        inflight_reset.abort();
        tracker.abort();
    })
    .await
    .expect("pending sockets and inflight handshakes acquire v2 layers");
}

#[tokio::test]
async fn raw_magnets_authenticate_layers_download_and_own_learned_aliases() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for (fixture, topic) in [
            (V2, "v2"),
            (HYBRID, "v1"),
            (HYBRID, "v2"),
            (HYBRID, "dual"),
            (
                include_bytes!("../protocol/fixtures/v2-single.torrent").as_slice(),
                "v2",
            ),
            (
                include_bytes!("../protocol/fixtures/v2-leaves.torrent").as_slice(),
                "v2",
            ),
            (
                include_bytes!("../protocol/fixtures/v2-copies.torrent").as_slice(),
                "v2",
            ),
        ] {
            let meta = Profile::default().parse_owned(fixture).unwrap();
            let fault = if meta.info_hash().v1().is_some() && topic == "v1" {
                7
            } else {
                0
            };
            let (address, source) = source(&meta, fault).await;
            let (url, tracker) = metadata_peer::tracker(&[address]).await;
            let session = session().await;
            let root = Temp::new();
            let mut options = TorrentOptions::new(
                TorrentSource::Magnet(magnet(&meta, topic, &url)),
                Destination::Create(root.0.clone()),
            );
            options.settings = settings();
            let mut handle = session.admit(options).unwrap();
            let resolved = handle.wait_metadata().await.unwrap();
            assert_eq!(resolved.info_bytes(), meta.info_bytes());
            for index in 0..meta.layout().piece_count() {
                if resolved.layers_pending() {
                    assert_eq!(
                        resolved.verification(index),
                        Some(spill::protocol::metainfo::PieceVerification::V1(
                            *meta.piece_hash(index).unwrap()
                        ))
                    );
                } else {
                    assert_eq!(resolved.verification(index), meta.verification(index));
                }
            }
            handle.wait_complete().await.unwrap();
            while handle.snapshot().progress.layers_pending {
                assert!(handle.changed().await, "{:?}", handle.snapshot());
            }
            let progress = handle.snapshot().progress;
            assert!(progress.metadata_ready && progress.info_validated);
            assert!(!progress.layers_pending);
            assert_eq!(progress.hash_bytes, progress.hash_total_bytes);
            let rebuilt = handle.metainfo().unwrap();
            assert!(!rebuilt.layers_pending());
            for piece in 0..meta.layout().piece_count() {
                assert_eq!(rebuilt.verification(piece), meta.verification(piece));
            }
            assert!(
                session
                    .torrent(TorrentIdentity::V2(meta.info_hash().v2().unwrap()))
                    .is_some()
            );
            assert!(
                session
                    .inspect(magnet(&meta, "v2", &url), Profile::default(), settings())
                    .is_err()
            );
            if meta.info_hash().v1().is_some() {
                assert!(
                    session
                        .torrent(TorrentIdentity::V1(meta.info_hash().v1().unwrap()))
                        .is_some()
                );
                assert!(
                    session
                        .inspect(magnet(&meta, "v1", &url), Profile::default(), settings())
                        .is_err()
                );
            }
            handle.stop().await.unwrap();
            let recovery = handle.recovery().unwrap();
            session.shutdown().await.unwrap();
            assert!(session.torrents().is_empty());
            if topic == "v1" {
                use crate::v2_peer as raw;
                use spill::protocol::peer::hash::HashRequest;
                let restarted = self::session().await;
                let mut opts = TorrentOptions::new(
                    TorrentSource::Metainfo(rebuilt.clone()),
                    Destination::Recover {
                        root: root.0.clone(),
                        state: recovery,
                    },
                );
                opts.settings = settings();
                let recovered = restarted.admit(opts).unwrap();
                recovered.wait_complete().await.unwrap();
                let mut peer = tokio::net::TcpStream::connect(restarted.local_addr())
                    .await
                    .unwrap();
                raw::initiate(
                    &mut peer,
                    rebuilt.info_hash().v2().unwrap().wire_key(),
                    90,
                    false,
                )
                .await;
                let file = rebuilt
                    .layout()
                    .files()
                    .iter()
                    .find(|file| file.length() > u64::from(rebuilt.layout().piece_length()))
                    .unwrap();
                let base_layer = (rebuilt.layout().piece_length() / 16384).ilog2();
                let request = HashRequest {
                    pieces_root: file.pieces_root().unwrap(),
                    base_layer,
                    index: 0,
                    length: (file
                        .length()
                        .div_ceil(u64::from(rebuilt.layout().piece_length()))
                        as u32)
                        .next_power_of_two(),
                    proof_layers: file.length().div_ceil(16384).next_power_of_two().ilog2()
                        - base_layer
                        - 1,
                };
                let mut header = request.pieces_root.to_vec();
                for value in [
                    request.base_layer,
                    request.index,
                    request.length,
                    request.proof_layers,
                ] {
                    header.extend_from_slice(&value.to_be_bytes());
                }
                raw::send(&mut peer, 21, &header).await;
                let response = raw::next(&mut peer, 22).await;
                request.verify(file.length(), 512, &response[49..]).unwrap();
                restarted.shutdown().await.unwrap();
            }
            source.abort();
            tracker.abort();
        }
    })
    .await
    .expect("raw v2 and hybrid magnet downloads");
}

#[tokio::test]
async fn magnet_tracker_tiers_skip_failed_tracker_on_later_announce() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let (address, source) = source(&meta, 0).await;
        let (url, tracker) = metadata_peer::tracker(&[address]).await;
        // Reserve a non-listening port so refusal is immediate and cannot race reuse.
        let dead = tokio::net::TcpSocket::new_v4().unwrap();
        dead.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let dead_url = format!("http://{}/announce", dead.local_addr().unwrap());
        let link = magnet(&meta, "v2", &format!("{dead_url}&tr={url}&tr={url}"));
        let session = session().await;
        let root = Temp::new();
        let mut options = TorrentOptions::new(
            TorrentSource::Magnet(link),
            Destination::Create(root.0.clone()),
        );
        options.settings = settings();
        options.diagnostics = true;
        let mut handle = session.admit(options).unwrap();
        let resolved = handle.wait_metadata().await.unwrap();
        assert_eq!(resolved.info_bytes(), meta.info_bytes());
        let document = resolved.decode_document();
        let spill::protocol::bencode::Value::List(tiers) =
            document.get(b"announce-list").unwrap().value()
        else {
            unreachable!()
        };
        assert_eq!(tiers.len(), 2);
        for (tier, expected) in tiers.iter().zip([&dead_url, &url]) {
            let spill::protocol::bencode::Value::List(urls) = tier.value() else {
                unreachable!()
            };
            assert_eq!(urls.len(), 1);
            assert_eq!(
                urls[0].value(),
                &spill::protocol::bencode::Value::Bytes(expected.as_bytes())
            );
        }
        handle.wait_complete().await.unwrap();
        loop {
            let snapshot = handle.snapshot();
            let trackers = &snapshot.diagnostics.as_ref().unwrap().trackers;
            if trackers.iter().any(|t| t.url == url && t.exchanges >= 2) {
                assert_eq!(trackers.len(), 2);
                assert_eq!(
                    trackers
                        .iter()
                        .find(|t| t.url == dead_url)
                        .unwrap()
                        .exchanges,
                    1
                );
                break;
            }
            assert!(handle.changed().await);
        }
        let before = handle
            .snapshot()
            .diagnostics
            .as_ref()
            .unwrap()
            .trackers
            .iter()
            .find(|t| t.url == url)
            .unwrap()
            .exchanges;
        handle.reannounce();
        loop {
            let snapshot = handle.snapshot();
            let trackers = &snapshot.diagnostics.as_ref().unwrap().trackers;
            if trackers
                .iter()
                .any(|t| t.url == url && t.exchanges > before)
            {
                assert_eq!(
                    trackers
                        .iter()
                        .find(|t| t.url == dead_url)
                        .unwrap()
                        .exchanges,
                    1
                );
                break;
            }
            assert!(handle.changed().await);
        }
        let dead_address = dead.local_addr().unwrap();
        drop(dead);
        let stalled = TcpListener::bind(dead_address).await.unwrap();
        let before = handle
            .snapshot()
            .diagnostics
            .as_ref()
            .unwrap()
            .trackers
            .iter()
            .find(|t| t.url == url)
            .unwrap()
            .exchanges;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(18)).await;
        tokio::time::resume();
        handle.reannounce();
        let (_silent, _) = tokio::time::timeout(Duration::from_secs(3), stalled.accept())
            .await
            .expect("expired failure backoff starts a retry")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = handle.snapshot();
                let trackers = &snapshot.diagnostics.as_ref().unwrap().trackers;
                if trackers
                    .iter()
                    .any(|t| t.url == url && t.exchanges > before)
                {
                    assert_eq!(
                        trackers
                            .iter()
                            .find(|t| t.url == dead_url)
                            .unwrap()
                            .exchanges,
                        1,
                        "working tracker answers while the failed tracker retry is still pending"
                    );
                    break;
                }
                assert!(handle.changed().await);
            }
        })
        .await
        .expect("a failed tracker retry must not delay the working tracker");
        handle.reannounce();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stalled.accept())
                .await
                .is_err(),
            "reannounce must not duplicate a tracker's pending retry"
        );
        handle.stop().await.unwrap();
        session.shutdown().await.unwrap();
        source.abort();
        tracker.abort();
    })
    .await
    .expect("magnet fallback, backoff and non-blocking failure retry");
}

#[tokio::test]
async fn corrupt_metadata_and_bad_hash_peers_do_not_poison_honest_resolution() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        for fault in [1, 2, 3, 4, 5, 6] {
            let (bad, bad_task) = source(&meta, fault).await;
            let (good, good_task) = source(&meta, 0).await;
            let (url, tracker) = metadata_peer::tracker(&[bad, good]).await;
            let session = session().await;
            let handle = session
                .inspect(magnet(&meta, "v2", &url), Profile::default(), settings())
                .unwrap();
            let resolved = handle.wait_metadata().await.unwrap();
            for index in 0..meta.layout().piece_count() {
                assert_eq!(resolved.verification(index), meta.verification(index));
            }
            assert!(handle.outcome().await.cleanup_complete);
            session.shutdown().await.unwrap();
            bad_task.abort();
            good_task.abort();
            tracker.abort();
        }
    })
    .await
    .expect("honest peer survives invalid metadata and hash replies");
}

#[tokio::test]
async fn full_v2_topics_are_checked_even_when_the_wire_prefix_matches() {
    use crate::v2_peer as raw;
    use spill::protocol::bencode::{self, Value};
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(HYBRID).unwrap();
        let mut wrong = *meta.info_hash().v2().unwrap().as_bytes();
        wrong[31] ^= 1;
        let wrong = spill::protocol::identity::InfoHashV2::from_bytes(wrong);
        for dual in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (url, tracker) = metadata_peer::tracker(&[listener.local_addr().unwrap()]).await;
            let uri = format!(
                "magnet:?xt=urn:btmh:1220{wrong}{}&tr={url}",
                if dual {
                    format!("&xt=urn:btih:{}", meta.info_hash().v1().unwrap())
                } else {
                    String::new()
                }
            );
            let magnet = Magnet::parse(&uri, Default::default()).unwrap();
            let key = magnet.info_hash().wire_key();
            let info = meta.info_bytes().to_vec();
            let source = tokio::spawn(async move {
                let (mut peer, _) = listener.accept().await.unwrap();
                raw::accept(&mut peer, key, 96).await;
                let hello = format!("d1:md11:ut_metadatai3ee13:metadata_sizei{}ee", info.len());
                raw::send(&mut peer, 20, &[&[0], hello.as_bytes()].concat()).await;
                let mut metadata_id = None;
                loop {
                    let frame = raw::frame(&mut peer).await.unwrap();
                    if frame.starts_with(&[20, 0]) {
                        let doc = bencode::decode(&frame[2..], Default::default()).unwrap();
                        let Value::Integer(id) =
                            doc.get(b"m").unwrap().get(b"ut_metadata").unwrap().value()
                        else {
                            unreachable!()
                        };
                        metadata_id = Some(*id as u8);
                    } else if frame.starts_with(&[20, 3]) {
                        let mut reply = vec![metadata_id.unwrap()];
                        reply.extend_from_slice(
                            format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len())
                                .as_bytes(),
                        );
                        reply.extend_from_slice(&info);
                        raw::send(&mut peer, 20, &reply).await;
                        break;
                    }
                }
                peer.read_to_end(&mut Vec::new()).await.unwrap();
            });
            let session = session().await;
            let handle = session
                .inspect(magnet, Profile::default(), settings())
                .unwrap();
            source.await.unwrap();
            assert!(!handle.snapshot().progress.info_validated);
            assert!(handle.metainfo().is_none());
            handle.stop().await.unwrap();
            session.shutdown().await.unwrap();
            tracker.abort();
        }
    })
    .await
    .expect("full magnet topics, not only routing keys, authenticate metadata");
}

#[tokio::test]
async fn large_layers_use_bounded_ranges_proofs_and_a_padded_final_pair() {
    use sha2::{Digest, Sha256};
    use spill::protocol::bencode::{self, Node, Value};
    use std::collections::BTreeMap;
    tokio::time::timeout(Duration::from_secs(10), async {
        let hashes: Vec<[u8; 32]> = (0u32..1025)
            .map(|index| Sha256::digest(index.to_be_bytes()).into())
            .collect();
        let mut nodes = hashes.clone();
        nodes.resize(2048, [0; 32]);
        while nodes.len() > 1 {
            nodes = nodes
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| Sha256::digest([pair[0], pair[1]].concat()).into())
                .collect();
        }
        let root = nodes[0];
        let file = Node::new(Value::Dictionary(BTreeMap::from([
            (
                b"length".as_slice(),
                Node::new(Value::Integer(1025 * 16384 - 123)),
            ),
            (b"pieces root".as_slice(), Node::new(Value::Bytes(&root))),
        ])));
        let tree = Node::new(Value::Dictionary(BTreeMap::from([(
            b"large".as_slice(),
            Node::new(Value::Dictionary(BTreeMap::from([(b"".as_slice(), file)]))),
        )])));
        let info = Node::new(Value::Dictionary(BTreeMap::from([
            (b"file tree".as_slice(), tree),
            (b"meta version".as_slice(), Node::new(Value::Integer(2))),
            (b"name".as_slice(), Node::new(Value::Bytes(b"large"))),
            (b"piece length".as_slice(), Node::new(Value::Integer(16384))),
        ])));
        let layer = hashes.concat();
        let doc = Node::new(Value::Dictionary(BTreeMap::from([
            (b"info".as_slice(), info),
            (
                b"piece layers".as_slice(),
                Node::new(Value::Dictionary(BTreeMap::from([(
                    root.as_slice(),
                    Node::new(Value::Bytes(&layer)),
                )]))),
            ),
        ])));
        let meta = Profile::default()
            .parse_owned(&bencode::encode(&doc, Default::default()).unwrap())
            .unwrap();
        let (address, source) = source(&meta, 0).await;
        let (url, tracker) = metadata_peer::tracker(&[address]).await;
        let session = session().await;
        let handle = session
            .inspect(magnet(&meta, "v2", &url), Profile::default(), settings())
            .unwrap();
        let resolved = handle.wait_metadata().await.unwrap();
        for index in 0..1025 {
            assert_eq!(resolved.verification(index), meta.verification(index));
        }
        assert_eq!(handle.snapshot().progress.hash_bytes, 1025 * 32);
        assert!(handle.outcome().await.cleanup_complete);
        session.shutdown().await.unwrap();
        source.abort();
        tracker.abort();
    })
    .await
    .expect("multi-range authenticated layer acquisition");
}

#[tokio::test]
async fn inbound_metadata_peer_is_reused_for_payload_without_tracker_peers() {
    use crate::v2_peer as raw;
    use tokio::net::TcpStream;
    tokio::time::timeout(Duration::from_secs(10), async {
        for fast in [false, true] {
            let destination = Temp::new();
            let meta = Profile::default()
                .parse_owned(&crate::payload::seed_metainfo("inbound", b"abc", None))
                .unwrap();
            let download = Session::bind(
                NetworkOptions {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    allow_local_network: true,
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            let (url, tracker) = metadata_peer::tracker(&[]).await;
            let mut options = TorrentOptions::new(
                TorrentSource::Magnet(magnet(&meta, "v1", &url)),
                Destination::Create(destination.0.clone()),
            );
            options.settings = settings();
            options.settings.pex = true;
            options.diagnostics = true;
            let handle = download.admit(options).unwrap();
            // This peer has no listener, advertises no p and never reconnects.
            let mut socket = TcpStream::connect(download.local_addr()).await.unwrap();
            raw::initiate(&mut socket, meta.info_hash().wire_key(), 92, fast).await;
            let (release, ready) = tokio::sync::oneshot::channel();
            let port = download.local_addr().port();
            let peer = tokio::spawn(async move {
                let mut ready = Some(ready);
                let info = meta.info_bytes();
                let hello = format!("d1:md11:ut_metadatai3ee13:metadata_sizei{}ee", info.len());
                raw::send(&mut socket, 5, &[0x80]).await;
                raw::send(&mut socket, 1, &[]).await;
                raw::send(&mut socket, 20, &[&[0], hello.as_bytes()].concat()).await;
                let mut metadata_id = None;
                while let Ok(body) = raw::read_frame(&mut socket).await {
                    match body.as_slice() {
                        [20, 0, payload @ ..] => {
                            let doc = spill::protocol::bencode::decode(payload, Default::default())
                                .unwrap();
                            if doc.get(b"metadata_size").is_some() {
                                assert_eq!(
                                    doc.get(b"m").unwrap().get(b"ut_holepunch").unwrap().value(),
                                    &spill::protocol::bencode::Value::Integer(3)
                                );
                            } else {
                                assert!(doc.get(b"m").unwrap().get(b"ut_holepunch").is_none());
                            }
                            assert_eq!(
                                doc.get(b"p").unwrap().value(),
                                &spill::protocol::bencode::Value::Integer(i64::from(port))
                            );
                            if let spill::protocol::bencode::Value::Integer(id) =
                                doc.get(b"m").unwrap().get(b"ut_metadata").unwrap().value()
                            {
                                metadata_id = Some(*id as u8);
                            }
                        }
                        [20, 3, ..] => {
                            let mut reply = vec![metadata_id.unwrap()];
                            reply.extend_from_slice(
                                format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len())
                                    .as_bytes(),
                            );
                            reply.extend_from_slice(info);
                            raw::send(&mut socket, 20, &reply).await;
                        }
                        [6, request @ ..] => {
                            assert_eq!(request, raw::request(0, 0, 3));
                            if let Some(ready) = ready.take() {
                                ready.await.unwrap();
                            }
                            raw::send(&mut socket, 7, &[&request[..8], b"abc"].concat()).await;
                        }
                        _ => {}
                    }
                }
            });
            while !handle
                .snapshot()
                .diagnostics
                .unwrap()
                .peers
                .iter()
                .any(|p| p.source == "metadata")
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            release.send(()).unwrap();
            handle.wait_complete().await.unwrap();
            assert_eq!(
                std::fs::read(destination.0.join("inbound")).unwrap(),
                b"abc"
            );
            assert_eq!(download.network_status().await.inbound_tcp.total, 1);
            download.shutdown().await.unwrap();
            peer.await.unwrap();
            tracker.abort();
        }
    })
    .await
    .expect("inbound metadata connection hands off to payload without reconnect");
}

#[tokio::test]
async fn stop_during_layers_and_duplicate_learned_alias_never_prepare_payload() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let meta = Profile::default().parse_owned(V2).unwrap();
        let (stalled, stalled_task) = source(&meta, 4).await;
        let (url, tracker) = metadata_peer::tracker(&[stalled]).await;
        let session = session().await;
        let root = Temp::new();
        let mut options = TorrentOptions::new(
            TorrentSource::Magnet(magnet(&meta, "v2", &url)),
            Destination::Create(root.0.clone()),
        );
        options.settings = settings();
        options.diagnostics = true;
        let mut pending = session.admit(options).unwrap();
        while !pending.snapshot().progress.info_validated {
            assert!(pending.changed().await, "{:?}", pending.snapshot());
        }
        assert_eq!(pending.snapshot().identity, meta.info_hash());
        assert!(!pending.snapshot().progress.metadata_ready);
        assert!(pending.snapshot().progress.hash_total_bytes > 0);
        loop {
            let snapshot = pending.snapshot();
            let diagnostics = snapshot.diagnostics.as_ref().unwrap();
            if diagnostics
                .trackers
                .iter()
                .any(|t| t.last_reply_ago_ms.is_some())
            {
                assert!(diagnostics.sampled_at.is_some());
                assert!(snapshot.progress.active_peers > 0);
                break;
            }
            assert!(pending.changed().await);
        }
        assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
        assert!(
            session
                .inspect(magnet(&meta, "v2", &url), Profile::default(), settings())
                .is_err()
        );
        assert!(
            session
                .torrent(TorrentIdentity::V2(meta.info_hash().v2().unwrap()))
                .is_some()
        );
        pending.stop().await.unwrap();
        assert!(pending.outcome().await.cleanup_complete);
        assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
        let meta = Profile::default().parse_owned(HYBRID).unwrap();
        let (good, good_task) = source(&meta, 0).await;
        let (good_url, good_tracker) = metadata_peer::tracker(&[good]).await;
        // Disjoint topics admit two executions; the first to validate info
        // must not become a second writer for an alias the other owns.
        let (silent_url, silent_tracker) = metadata_peer::tracker(&[]).await;
        let mut owner = TorrentOptions::new(
            TorrentSource::Magnet(magnet(&meta, "v2", &silent_url)),
            Destination::Create(root.0.clone()),
        );
        owner.settings = settings();
        let owner = session.admit(owner).unwrap();
        let newcomer_root = Temp::new();
        let mut newcomer = TorrentOptions::new(
            TorrentSource::Magnet(magnet(&meta, "v1", &good_url)),
            Destination::Create(newcomer_root.0.clone()),
        );
        newcomer.settings = settings();
        let newcomer = session.admit(newcomer).unwrap();
        assert!(newcomer.wait_metadata().await.is_err());
        let outcome = newcomer.outcome().await;
        assert!(outcome.cleanup_complete);
        assert_eq!(
            outcome.error.as_ref().unwrap().to_string(),
            "torrent already exists"
        );
        assert!(matches!(
            spill::engine::profile::AdmissionError::of(&newcomer.join().await.unwrap_err()),
            Some(spill::engine::profile::AdmissionError::Duplicate)
        ));
        assert_eq!(std::fs::read_dir(&newcomer_root.0).unwrap().count(), 0);
        assert!(!owner.snapshot().progress.info_validated);
        owner.stop().await.unwrap();
        session.shutdown().await.unwrap();
        for task in [
            stalled_task,
            tracker,
            good_task,
            good_tracker,
            silent_tracker,
        ] {
            task.abort();
        }
    })
    .await
    .expect("joined cancellation and learned alias collision");
}
