use crate::payload::{Temp, seed_metainfo};
use spill::engine::{
    profile::{BLOCK_LEN, Profile},
    session::{Limits, NetworkOptions, Session},
    torrent::{Destination, RunPolicy, TorrentOptions, TorrentSource},
};
use spill::protocol::magnet::Magnet;
use std::{io, time::Duration};

#[tokio::test]
async fn offline_v2_magnets_are_refused_without_reserving_payload() {
    let root = Temp::new();
    let session = Session::offline(Limits {
        torrents: 1,
        ..Default::default()
    });
    let magnet = Magnet::parse(
        &format!("magnet:?xt=urn:btmh:1220{}", "00".repeat(32)),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        session
            .inspect(magnet, Profile::default(), Default::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
    let meta = Profile::default()
        .parse_owned(&seed_metainfo("x", b"abc", None))
        .unwrap();
    std::fs::write(root.0.join("x"), b"abc").unwrap();
    let mut options = TorrentOptions::new(
        TorrentSource::Metainfo(meta),
        Destination::Adopt(root.0.clone()),
    );
    options.run = RunPolicy::Recheck;
    let handle = session.admit(options).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(5), handle.outcome())
        .await
        .unwrap();
    assert!(outcome.recheck_finished && outcome.full_complete && outcome.cleanup_complete);
    assert!(outcome.error.is_none());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn padding_does_not_consume_the_data_file_admission_limit() {
    use sha1::{Digest, Sha1};
    use sha2::Sha256;
    use spill::protocol::metainfo::FileKind;
    tokio::time::timeout(Duration::from_secs(60), async {
        let root: [u8; 32] = Sha256::digest([0]).into();
        let padded_sha1 = Sha1::digest([0; 32768]);
        let tail_sha1 = Sha1::digest([0]);
        for hybrid in [false, true] {
            let mut bytes = b"d4:infod9:file treed".to_vec();
            for index in 0..50001 {
                bytes.extend_from_slice(
                    format!("5:{index:05}d0:d6:lengthi1e11:pieces root32:").as_bytes(),
                );
                bytes.extend_from_slice(&root);
                bytes.extend_from_slice(b"ee");
            }
            bytes.push(b'e');
            if hybrid {
                bytes.extend_from_slice(b"5:filesl");
                for index in 0..50001 {
                    bytes
                        .extend_from_slice(format!("d6:lengthi1e4:pathl5:{index:05}ee").as_bytes());
                    if index != 50000 {
                        bytes.extend_from_slice(b"d4:attr1:p6:lengthi32767e4:pathl4:.pad5:32767ee");
                    }
                }
                bytes.push(b'e');
            }
            bytes.extend_from_slice(b"12:meta versioni2e4:name4:many12:piece lengthi32768e");
            if hybrid {
                bytes.extend_from_slice(b"6:pieces1000020:");
                for _ in 0..50000 {
                    bytes.extend_from_slice(&padded_sha1);
                }
                bytes.extend_from_slice(&tail_sha1);
            }
            bytes.extend_from_slice(b"e12:piece layersdee");
            let meta = Profile::default().parse_owned(&bytes).unwrap();
            assert_eq!(meta.layout().files().len(), 100001);
            assert_eq!(
                meta.layout()
                    .files()
                    .iter()
                    .filter(|file| file.kind() == FileKind::Data)
                    .count(),
                50001
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (url, tracker) =
                crate::metadata_peer::tracker(&[listener.local_addr().unwrap()]).await;
            let source = tokio::spawn(crate::metadata_peer::serve_full(listener, meta.clone(), 0));
            let session = Session::bind(
                NetworkOptions {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    allow_local_network: true,
                    utp: false,
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            let magnet = Magnet::parse(
                &format!(
                    "magnet:?xt=urn:btmh:1220{}&tr={url}",
                    meta.info_hash().v2().unwrap()
                ),
                Default::default(),
            )
            .unwrap();
            let handle = session
                .inspect(
                    magnet,
                    Profile::default(),
                    spill::engine::settings::TorrentSettings {
                        dht: false,
                        pex: false,
                        web_seeds: false,
                        ..Default::default()
                    },
                )
                .unwrap();
            let resolved = handle.wait_metadata().await.unwrap();
            assert_eq!(resolved.info_hash(), meta.info_hash());
            assert_eq!(resolved.layout().files().len(), 100001);
            assert!(handle.outcome().await.cleanup_complete);
            session.shutdown().await.unwrap();
            source.abort();
            tracker.abort();
        }
    })
    .await
    .expect("many-file magnet admission excludes padding");
}

#[tokio::test]
async fn invalid_profiles_do_not_panic_or_consume_admission() {
    let profile = Profile {
        peers: 1,
        cache_bytes: BLOCK_LEN as usize,
        write_queue_bytes: BLOCK_LEN as usize,
    };
    let meta = profile
        .parse_owned(&seed_metainfo("x", b"abc", None))
        .unwrap();
    let session = Session::offline(Limits {
        torrents: 1,
        peers: 1,
        payload_bytes: profile.cache_bytes + profile.write_queue_bytes,
        ..Default::default()
    });
    let online = Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap();
    let root = Temp::new();
    let magnet = Magnet::parse(
        &format!("magnet:?xt=urn:btih:{}", meta.info_hash()),
        Default::default(),
    )
    .unwrap();
    for invalid in [
        Profile {
            peers: 0,
            ..profile
        },
        Profile {
            peers: 4097,
            ..profile
        },
        Profile {
            cache_bytes: BLOCK_LEN as usize - 1,
            ..profile
        },
        Profile {
            cache_bytes: 1024 * 1024 * 1024 + 1,
            ..profile
        },
        Profile {
            write_queue_bytes: BLOCK_LEN as usize - 1,
            ..profile
        },
        Profile {
            write_queue_bytes: 1024 * 1024 * 1024 + 1,
            ..profile
        },
    ] {
        let mut options = TorrentOptions::new(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Open(root.0.clone()),
        );
        options.profile = invalid;
        options.run = RunPolicy::Recheck;
        let error = session.admit(options).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "invalid torrent resource limits");
        assert!(session.torrent(meta.info_hash()).is_none());
        let error = online
            .inspect(magnet.clone(), invalid, Default::default())
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "invalid torrent resource limits");
    }
    online.shutdown().await.unwrap();

    std::fs::write(root.0.join("x"), b"abc").unwrap();
    let mut options = TorrentOptions::new(
        TorrentSource::Metainfo(meta),
        Destination::Open(root.0.clone()),
    );
    options.profile = profile;
    options.run = RunPolicy::Recheck;
    let observer = session.admit(options).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(5), observer.outcome())
        .await
        .expect("recheck completion");
    assert!(outcome.recheck_finished && outcome.wanted_complete && outcome.full_complete);
    assert!(outcome.cleanup_complete && outcome.error.is_none());
    assert!(session.torrent(observer.info_hash()).is_none());
    assert!(std::sync::Arc::ptr_eq(&outcome, &observer.outcome().await));
    assert!(observer.snapshot().progress.completed);
    assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
    session.shutdown().await.unwrap();
}
