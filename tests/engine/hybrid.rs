use crate::{payload::Temp, v2_peer as raw};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, Session},
        torrent::{Destination, RunPolicy, TorrentHandle, TorrentOptions, TorrentSource},
    },
    protocol::{
        identity::{InfoHashV1, TorrentIdentity},
        magnet::Magnet,
        metainfo::OwnedMetainfo,
    },
};
use std::{io, time::Duration};
use tokio::net::{TcpListener, TcpStream};

const HYBRID: &[u8] = include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent");
const LENGTHS: &[usize] = &[16384, 0, 65659, 1, 32768, 123];

async fn session() -> Session {
    Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            utp: false,
            allow_local_network: true,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap()
}
fn options(meta: &OwnedMetainfo, root: &Temp) -> TorrentOptions {
    let mut opts = TorrentOptions::new(
        TorrentSource::Metainfo(meta.clone()),
        Destination::Open(root.0.clone()),
    );
    opts.diagnostics = true;
    opts.settings.trackers = false;
    opts.settings.pex = false;
    opts
}
async fn ready(handle: &mut TorrentHandle) {
    while !handle.snapshot().progress.ready {
        assert!(handle.changed().await, "{:?}", handle.snapshot());
    }
}
fn install(root: &Temp) {
    std::fs::create_dir(root.0.join("vectors")).unwrap();
    for (name, length) in ["block", "empty", "odd", "one", "piece", "tail"]
        .into_iter()
        .zip(LENGTHS)
    {
        std::fs::write(root.0.join("vectors").join(name), raw::payload(0, *length)).unwrap();
    }
}

#[tokio::test]
async fn pending_hybrid_accepts_a_v2_reply_without_offering_an_upgrade() {
    tokio::time::timeout(Duration::from_secs(15), async {
        use spill::protocol::peer::{HANDSHAKE_LEN, Handshake};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let full = Profile::default().parse_owned(HYBRID).unwrap();
        let meta =
            spill::protocol::metainfo::InfoMetainfo::parse(full.info_bytes(), Default::default())
                .unwrap()
                .complete(&Default::default())
                .unwrap();
        let v1 = *meta.info_hash().v1().unwrap().as_bytes();
        let v2 = meta.info_hash().v2().unwrap().wire_key();
        let root = Temp::new();
        let session = session().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut opts = options(&meta, &root);
        opts.peers.push(listener.local_addr().unwrap());
        let source = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; HANDSHAKE_LEN];
            socket.read_exact(&mut bytes).await.unwrap();
            let (hello, _) = Handshake::decode(&bytes).unwrap();
            assert_eq!(hello.info_hash, v1);
            assert_eq!(hello.reserved[7] & 0x10, 0);
            let reply = Handshake {
                reserved: [0; 8],
                info_hash: v2,
                peer_id: [122; 20],
            };
            socket.write_all(&reply.encode()).await.unwrap();
            raw::serve_advertised(socket, raw::pieces(LENGTHS, 32768), &[0xfe], false).await
        });
        let handle = session.admit(opts).unwrap();
        handle.wait_complete().await.unwrap();
        assert_eq!(handle.snapshot().progress.verified_pieces, 7);
        assert_eq!(
            std::fs::read(root.0.join("vectors/odd")).unwrap(),
            raw::payload(0, 65659)
        );
        handle.stop().await.unwrap();
        assert!(!source.await.unwrap().is_empty());
        session.shutdown().await.unwrap();
    })
    .await
    .expect("pending hybrid with a v2 handshake reply");
}

#[tokio::test]
async fn pending_hybrid_accepts_inbound_v2_and_rejects_hashes_through_completion() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let full = Profile::default().parse_owned(HYBRID).unwrap();
        let meta =
            spill::protocol::metainfo::InfoMetainfo::parse(full.info_bytes(), Default::default())
                .unwrap()
                .complete(&Default::default())
                .unwrap();
        let v1 = *meta.info_hash().v1().unwrap().as_bytes();
        let v2 = meta.info_hash().v2().unwrap().wire_key();
        let file = meta
            .layout()
            .files()
            .iter()
            .find(|file| file.length() == 65659)
            .unwrap();
        let root_hash = file.pieces_root().unwrap();
        let root = Temp::new();
        let session = session().await;
        let mut handle = session.admit(options(&meta, &root)).unwrap();
        ready(&mut handle).await;
        let mut v1_peer = TcpStream::connect(session.local_addr()).await.unwrap();
        raw::initiate_hybrid(&mut v1_peer, v1, v1, 123, true).await;
        let mut peer = TcpStream::connect(session.local_addr()).await.unwrap();
        raw::initiate(&mut peer, v2, 124, false).await;
        for base in [1u32, 0] {
            let mut request = root_hash.to_vec();
            for field in [base, 0, 2, 2 - base] {
                request.extend_from_slice(&field.to_be_bytes());
            }
            raw::send(&mut peer, 21, &request).await;
            assert_eq!(&raw::next(&mut peer, 23).await[1..], request);
        }
        assert!(!handle.snapshot().progress.completed);
        let source = tokio::spawn(raw::serve_advertised(
            peer,
            raw::pieces(LENGTHS, 32768),
            &[0xfe],
            false,
        ));
        handle.wait_complete().await.unwrap();
        while handle.snapshot().progress.layers_pending {
            assert!(handle.changed().await, "{:?}", handle.snapshot());
        }
        // The running core still lacks layers even after persistence publishes them.
        let mut peer = TcpStream::connect(session.local_addr()).await.unwrap();
        raw::initiate(&mut peer, v2, 125, false).await;
        let mut request = root_hash.to_vec();
        for field in [1u32, 0, 2, 1] {
            request.extend_from_slice(&field.to_be_bytes());
        }
        raw::send(&mut peer, 21, &request).await;
        assert_eq!(&raw::next(&mut peer, 23).await[1..], request);
        assert_eq!(
            std::fs::read(root.0.join("vectors/odd")).unwrap(),
            raw::payload(0, 65659)
        );
        handle.stop().await.unwrap();
        assert!(!source.await.unwrap().is_empty());
        session.shutdown().await.unwrap();
    })
    .await
    .expect("pending hybrid inbound v2 hash rejection and transfer");
}

#[tokio::test]
async fn pending_hybrid_restarts_with_partial_verification_and_serves_v1() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let full = Profile::default().parse_owned(HYBRID).unwrap();
        let meta =
            spill::protocol::metainfo::InfoMetainfo::parse(full.info_bytes(), Default::default())
                .unwrap()
                .complete(&Default::default())
                .unwrap();
        assert!(meta.layers_pending());
        let v1 = *meta.info_hash().v1().unwrap().as_bytes();
        let root = Temp::new();
        let session = session().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut opts = options(&meta, &root);
        opts.peers.push(listener.local_addr().unwrap());
        let source = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            raw::accept(&mut socket, v1, 120).await;
            raw::serve_advertised(socket, raw::pieces(LENGTHS, 32768), &[0x80], false).await
        });
        let mut handle = session.admit(opts).unwrap();
        while handle.snapshot().progress.verified_pieces < 2 {
            assert!(handle.changed().await, "{:?}", handle.snapshot());
        }
        handle.stop().await.unwrap();
        source.await.unwrap();
        let state = handle.recovery().unwrap();
        let saved = spill::protocol::resume::decode(&meta, state.resume.as_ref().unwrap()).unwrap();
        // The one-byte zero file also verifies against freshly provisioned disk.
        assert_eq!(saved.verified, [0x88]);
        assert!(handle.metainfo().unwrap().layers_pending());

        let mut opts = options(&meta, &root);
        opts.destination = Destination::Recover {
            root: root.0.clone(),
            state,
        };
        let mut recovered = session.admit(opts).unwrap();
        ready(&mut recovered).await;
        assert_eq!(recovered.snapshot().progress.verified_pieces, 2);
        assert!(recovered.snapshot().progress.layers_pending);
        assert!(!recovered.snapshot().progress.completed);
        let mut peer = TcpStream::connect(session.local_addr()).await.unwrap();
        raw::initiate_hybrid(&mut peer, v1, v1, 121, false).await;
        raw::send(&mut peer, 2, &[]).await;
        raw::next(&mut peer, 1).await;
        raw::send(&mut peer, 6, &raw::request(0, 0, 16384)).await;
        assert_eq!(&raw::next(&mut peer, 7).await[9..], raw::payload(0, 16384));
        recovered.stop().await.unwrap();
        session.shutdown().await.unwrap();
    })
    .await
    .expect("pending hybrid recovery");
}

#[tokio::test]
async fn pending_hybrid_checkpoints_before_rebuilding_and_publishes_before_full_checkpoint() {
    tokio::time::timeout(Duration::from_secs(15), async {
        use std::sync::{Arc, Mutex};
        let full = Profile::default().parse_owned(HYBRID).unwrap();
        let pending =
            spill::protocol::metainfo::InfoMetainfo::parse(full.info_bytes(), Default::default())
                .unwrap()
                .complete(&Default::default())
                .unwrap();
        let root = Temp::new();
        install(&root);
        let session = session().await;
        let mut opts = options(&pending, &root);
        opts.destination = Destination::Adopt(root.0.clone());
        let events = Arc::new(Mutex::new(Vec::new()));
        let checkpoints = events.clone();
        let complete = full.clone();
        opts.persistence.checkpoint = Some(Box::new(move |bytes| {
            let mode = if spill::protocol::resume::decode(&pending, &bytes).is_ok() {
                "pending"
            } else {
                spill::protocol::resume::decode(&complete, &bytes).unwrap();
                "full"
            };
            checkpoints.lock().unwrap().push(mode);
            Ok(())
        }));
        let publications = events.clone();
        let (publish, publication) = tokio::sync::oneshot::channel();
        let publication = Mutex::new(Some(publication));
        opts.persistence.resolved = Some(Box::new(move |meta| {
            publication
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .blocking_recv()
                .map_err(io::Error::other)?;
            assert!(!meta.layers_pending());
            let mut events = publications.lock().unwrap();
            assert_eq!(events.last(), Some(&"pending"));
            events.push("resolved");
            Ok(())
        }));
        let mut handle = session.admit(opts).unwrap();
        handle.wait_complete().await.unwrap();
        assert!(handle.snapshot().progress.layers_pending);
        publish.send(()).unwrap();
        while handle.snapshot().progress.layers_pending {
            assert!(handle.changed().await, "{:?}", handle.snapshot());
        }
        handle.stop().await.unwrap();
        let events = events.lock().unwrap().clone();
        let resolved = events
            .iter()
            .position(|event| *event == "resolved")
            .unwrap();
        assert!(events[..resolved].contains(&"pending"));
        assert!(events[resolved + 1..].iter().all(|event| *event == "full"));
        assert_eq!(events.last(), Some(&"full"));
        session.shutdown().await.unwrap();
    })
    .await
    .expect("pending-to-full checkpoint ordering");
}

#[tokio::test]
async fn mixed_v1_and_upgraded_v2_sources_share_verification_and_recover_one_payload() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let meta = Profile::default().parse_owned(HYBRID).unwrap();
        let identity = meta.info_hash();
        let v1 = *identity.v1().unwrap().as_bytes();
        let v2 = identity.v2().unwrap().wire_key();
        let root = Temp::new();
        let session = session().await;
        let mut opts = options(&meta, &root);
        let mut sources = Vec::new();
        for (reply, bits, id) in [(v1, 0x70, 101), (v2, 0x8e, 102)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            opts.peers.push(listener.local_addr().unwrap());
            sources.push(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                raw::accept_hybrid(&mut socket, v1, reply, id).await;
                raw::serve_advertised(socket, raw::pieces(LENGTHS, 32768), &[bits], false).await
            }));
        }
        let handle = session.admit(opts).unwrap();
        assert_eq!(session.torrents().len(), 1);
        assert_eq!(handle.snapshot().identity, identity);
        assert!(handle.snapshot().progress.metadata_ready);
        for lookup in [
            identity,
            TorrentIdentity::V1(identity.v1().unwrap()),
            TorrentIdentity::V2(identity.v2().unwrap()),
        ] {
            assert_eq!(session.torrent(lookup).unwrap().info_hash(), identity);
        }
        handle.wait_complete().await.unwrap();
        assert_eq!(handle.snapshot().progress.verified_pieces, 7);
        assert!(
            handle
                .snapshot()
                .diagnostics
                .as_ref()
                .unwrap()
                .trackers
                .is_empty()
        );
        for (name, length) in ["block", "empty", "odd", "one", "piece", "tail"]
            .into_iter()
            .zip(LENGTHS)
        {
            assert_eq!(
                std::fs::read(root.0.join("vectors").join(name)).unwrap(),
                raw::payload(0, *length)
            );
        }
        assert!(!root.0.join("vectors/.pad").exists());
        handle.stop().await.unwrap();
        let first = sources.remove(0).await.unwrap();
        let second = sources.remove(0).await.unwrap();
        assert!(first.iter().all(|i| (1..=3).contains(i)) && !first.is_empty());
        assert!(second.iter().all(|i| [0, 4, 5, 6].contains(i)) && !second.is_empty());
        assert!(session.torrent(identity).is_none());
        // The same owner can reacquire both aliases after joined stop.
        let mut reopen = options(&meta, &root);
        reopen.destination = Destination::Recover {
            root: root.0.clone(),
            state: handle.recovery().unwrap(),
        };
        let recovered = session.admit(reopen).unwrap();
        recovered.wait_complete().await.unwrap();
        recovered.stop().await.unwrap();
        let mut recheck = options(&meta, &root);
        recheck.destination = Destination::Adopt(root.0.clone());
        recheck.run = RunPolicy::Recheck;
        let outcome = session.admit(recheck).unwrap().outcome().await;
        assert!(outcome.full_complete && outcome.recheck_finished && outcome.cleanup_complete);
        assert!(outcome.error.is_none());
        session.shutdown().await.unwrap();
    })
    .await
    .expect("mixed hybrid transfer and recovery");
}

#[tokio::test]
async fn both_inbound_routes_and_upgrade_serve_hybrid_padding_without_losing_v2_duties() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let meta = Profile::default().parse_owned(HYBRID).unwrap();
        let v1 = *meta.info_hash().v1().unwrap().as_bytes();
        let v2 = meta.info_hash().v2().unwrap().wire_key();
        let root = Temp::new();
        install(&root);
        let session = session().await;
        let mut seed = options(&meta, &root);
        seed.destination = Destination::Adopt(root.0.clone());
        let handle = session.admit(seed).unwrap();
        handle.wait_complete().await.unwrap();
        let file = meta
            .layout()
            .files()
            .iter()
            .find(|f| f.path().last().unwrap().as_ref() == b"odd")
            .unwrap();
        let mut hash = file.pieces_root().unwrap().to_vec();
        for n in [1u32, 0, 2, 1] {
            hash.extend_from_slice(&n.to_be_bytes());
        }
        for (id, key, upgrade, expected) in [
            (103, v1, false, v1),
            (104, v1, true, v2),
            (105, v2, false, v2),
        ] {
            let mut peer = TcpStream::connect(session.local_addr()).await.unwrap();
            raw::initiate_hybrid(&mut peer, key, expected, id, upgrade).await;
            raw::send(&mut peer, 2, &[]).await;
            raw::next(&mut peer, 1).await;
            raw::send(&mut peer, 6, &raw::request(0, 0, 16384)).await;
            assert_eq!(&raw::next(&mut peer, 7).await[9..], raw::payload(0, 16384));
            // The final file has trailing v1 padding in this independent fixture.
            raw::send(&mut peer, 6, &raw::request(6, 123, 100)).await;
            assert_eq!(&raw::next(&mut peer, 7).await[9..], &[0; 100]);
            raw::send(&mut peer, 6, &raw::request(6, 100, 100)).await;
            let mut tail = raw::payload(100, 23);
            tail.resize(100, 0);
            assert_eq!(&raw::next(&mut peer, 7).await[9..], tail);
            if expected == v1 {
                raw::send(&mut peer, 21, &hash).await;
                assert!(
                    raw::frame(&mut peer).await.is_err(),
                    "v1 cannot exchange hashes"
                );
            } else {
                raw::send(&mut peer, 6, &raw::request(6, 32760, 16)).await;
                assert!(
                    raw::frame(&mut peer).await.is_err(),
                    "request past the piece disconnects"
                );
                let mut peer = TcpStream::connect(session.local_addr()).await.unwrap();
                raw::initiate_hybrid(&mut peer, key, expected, id + 10, upgrade).await;
                raw::send(&mut peer, 21, &hash).await;
                let reply = raw::next(&mut peer, 22).await;
                assert_eq!(&reply[1..49], hash);
                let mut unsolicited = raw::request(0, 0, 1)[..8].to_vec();
                unsolicited.push(0);
                raw::send(&mut peer, 7, &unsolicited).await;
                assert!(
                    raw::frame(&mut peer).await.is_err(),
                    "v2 must reject unsolicited pieces"
                );
            }
        }
        handle.stop().await.unwrap();
        session.shutdown().await.unwrap();
    })
    .await
    .expect("hybrid routes, upgrade and wire mode");
}

#[tokio::test]
async fn corrupt_hybrid_bytes_are_retried_without_failing_the_torrent() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(HYBRID).unwrap();
        let v1 = *meta.info_hash().v1().unwrap().as_bytes();
        let v2 = meta.info_hash().v2().unwrap().wire_key();
        let bad = TcpListener::bind("127.0.0.2:0").await.unwrap();
        let good = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = Temp::new();
        let session = session().await;
        let mut opts = options(&meta, &root);
        opts.peers = vec![bad.local_addr().unwrap(), good.local_addr().unwrap()];
        let bad = tokio::spawn(async move {
            let (mut socket, _) = bad.accept().await.unwrap();
            raw::accept_hybrid(&mut socket, v1, v2, 107).await;
            raw::serve_advertised(socket, raw::pieces(LENGTHS, 32768), &[0x80], true).await
        });
        let (repair, repairing) = tokio::sync::oneshot::channel();
        let good = tokio::spawn(async move {
            let (mut socket, _) = good.accept().await.unwrap();
            raw::accept_hybrid(&mut socket, v1, v1, 108).await;
            repairing.await.unwrap();
            raw::serve_advertised(socket, raw::pieces(LENGTHS, 32768), &[0xfe], false).await
        });
        let mut handle = session.admit(opts).unwrap();
        while handle.snapshot().progress.hash_failed_pieces == 0 {
            assert!(handle.changed().await);
        }
        repair.send(()).unwrap();
        handle.wait_complete().await.unwrap();
        assert_eq!(
            std::fs::read(root.0.join("vectors/block")).unwrap(),
            raw::payload(0, 16384)
        );
        handle.stop().await.unwrap();
        assert!(!bad.await.unwrap().is_empty());
        assert!(!good.await.unwrap().is_empty());
        session.shutdown().await.unwrap();
    })
    .await
    .expect("hybrid corruption retry");
}

#[tokio::test]
async fn route_collision_rejects_the_whole_hybrid_without_reserving_its_other_alias() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(HYBRID).unwrap();
        let session = session().await;
        let root = Temp::new();
        let prefix = InfoHashV1::from_bytes(meta.info_hash().v2().unwrap().wire_key());
        let magnet = |hash| {
            Magnet::parse(
                &format!("magnet:?xt=urn:btih:{hash}&tr=http://127.0.0.1:9/announce"),
                Default::default(),
            )
            .unwrap()
        };
        let blocker = session
            .inspect(magnet(prefix), Profile::default(), Default::default())
            .unwrap();
        assert_eq!(
            session.admit(options(&meta, &root)).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let other = session
            .inspect(
                magnet(meta.info_hash().v1().unwrap()),
                Profile::default(),
                Default::default(),
            )
            .unwrap();
        other.stop().await.unwrap();
        blocker.stop().await.unwrap();
        let mut handle = session.admit(options(&meta, &root)).unwrap();
        ready(&mut handle).await;
        for hash in [prefix, meta.info_hash().v1().unwrap()] {
            assert_eq!(
                session
                    .inspect(magnet(hash), Profile::default(), Default::default())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        handle.stop().await.unwrap();
        for hash in [prefix, meta.info_hash().v1().unwrap()] {
            session
                .inspect(magnet(hash), Profile::default(), Default::default())
                .unwrap()
                .stop()
                .await
                .unwrap();
        }
        session.shutdown().await.unwrap();
    })
    .await
    .expect("atomic hybrid aliases");
}

#[tokio::test]
async fn inconsistent_hybrid_commitments_fail_content_instead_of_banning_an_honest_peer() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for change_v1 in [true, false] {
            let mut bytes = HYBRID.to_vec();
            if change_v1 {
                let marker = b"6:pieces140:";
                let pos = bytes
                    .windows(marker.len())
                    .position(|b| b == marker)
                    .unwrap()
                    + marker.len();
                bytes[pos] ^= 1;
            } else {
                let original = Profile::default().parse_owned(HYBRID).unwrap();
                let root = original.layout().files()[0].pieces_root().unwrap();
                let pos = bytes.windows(32).position(|b| b == root).unwrap();
                bytes[pos] ^= 1;
            }
            let meta = Profile::default().parse_owned(&bytes).unwrap();
            let meta = if change_v1 {
                meta
            } else {
                spill::protocol::metainfo::InfoMetainfo::parse(
                    meta.info_bytes(),
                    Default::default(),
                )
                .unwrap()
                .complete(&std::collections::BTreeMap::new())
                .unwrap()
            };
            assert_eq!(meta.layers_pending(), !change_v1);
            let v1 = *meta.info_hash().v1().unwrap().as_bytes();
            let v2 = meta.info_hash().v2().unwrap().wire_key();
            let root = Temp::new();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let pending = meta.layers_pending();
            let source = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                if pending {
                    raw::accept(&mut socket, v1, 106).await;
                } else {
                    raw::accept_hybrid(&mut socket, v1, v2, 106).await;
                }
                raw::serve_advertised(socket, raw::pieces(LENGTHS, 32768), &[0xfe], false).await
            });
            let session = session().await;
            let mut opts = options(&meta, &root);
            opts.peers.push(address);
            let handle = session.admit(opts).unwrap();
            let outcome = handle.outcome().await;
            assert!(outcome.cleanup_complete);
            if change_v1 {
                assert!(!outcome.wanted_complete);
            }
            assert!(
                spill::engine::profile::AdmissionError::of(&handle.join().await.unwrap_err())
                    .is_some()
            );
            assert!(
                outcome
                    .error
                    .as_ref()
                    .unwrap()
                    .to_string()
                    .contains("inconsistent hybrid piece commitments"),
                "{outcome:?}"
            );
            source.await.unwrap();
            // Disk adoption must reject the same contradictory commitments.
            let disk = Temp::new();
            install(&disk);
            let mut opts = options(&meta, &disk);
            opts.destination = Destination::Adopt(disk.0.clone());
            // Pending layers are rebuilt by a persistent seed after recovery.
            if change_v1 {
                opts.run = RunPolicy::Recheck;
            }
            let outcome = session.admit(opts).unwrap().outcome().await;
            assert!(
                outcome
                    .error
                    .as_ref()
                    .unwrap()
                    .to_string()
                    .contains("inconsistent hybrid piece commitments")
            );
            assert!(outcome.cleanup_complete);
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("inconsistent hybrid content");
}
