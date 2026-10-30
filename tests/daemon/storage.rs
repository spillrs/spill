use crate::common::{Daemon, WebSeed, metainfo, multifile_metainfo};
use crate::payload::Temp;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn multi_block_download_and_parallel_recheck_preserve_piece_membership() {
    use sha1::{Digest, Sha1};
    let root = Temp::new();
    // Multiple pieces, each spanning several peer blocks, exercise queued write
    // batches and their hash barriers rather than just single-block transfers.
    const PIECE_LEN: usize = 128 * 1024;
    // Nine pieces exceed the four-job limit; full pieces span multiple hash
    // chunks and the final piece is short.
    let payload: Vec<u8> = (0..8 * PIECE_LEN + 37)
        .map(|i| ((i / 16384) ^ i) as u8)
        .collect();
    let mut source = WebSeed::files(&[("/payload", &payload)], false).await;
    source.url.push_str("payload");
    let mut bytes = format!(
        "d8:url-list{}:{}4:infod6:lengthi{}e4:name1:x12:piece lengthi{PIECE_LEN}e6:pieces{}:",
        source.url.len(),
        source.url,
        payload.len(),
        payload.len().div_ceil(PIECE_LEN) * 20,
    )
    .into_bytes();
    for piece in payload.chunks(PIECE_LEN) {
        bytes.extend_from_slice(&Sha1::digest(piece));
    }
    bytes.extend_from_slice(b"ee");
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], payload.len());
    // The merged bandwidth module must throttle both core request admission and
    // transport writes, including wakeups after the initial burst is exhausted.
    daemon
        .call(json!({"op":"set","id":id,"values":{"upload_limit":1048576}}))
        .await;
    daemon.state(&id, "seeding").await;
    // Exercise both the peer and writer readiness paths with simultaneous TCP
    // leechers and more blocks than one outbound queue can hold.
    let profile = spill::engine::profile::Profile {
        peers: 1,
        cache_bytes: spill::engine::profile::BLOCK_LEN as usize,
        write_queue_bytes: 2 * spill::engine::profile::BLOCK_LEN as usize,
    };
    let meta = profile.parse_owned(&bytes).unwrap();
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    let started = tokio::time::Instant::now();
    let mut downloads = tokio::task::JoinSet::new();
    for diagnostics in [false, true] {
        let meta = meta.clone();
        downloads.spawn(async move {
            let output = Temp::new();
            let session = spill::engine::session::Session::bind(
                spill::engine::session::NetworkOptions {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    allow_local_network: true,
                    ..Default::default()
                },
                spill::engine::session::Limits {
                    torrents: 1,
                    peers: 1,
                    payload_bytes: profile.cache_bytes + profile.write_queue_bytes,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            use spill::engine::torrent::{Destination, TorrentOptions, TorrentSource};
            let mut options = TorrentOptions::new(
                TorrentSource::Metainfo(meta),
                Destination::Open(output.0.clone()),
            );
            options.profile = profile;
            options.settings = spill::engine::settings::TorrentSettings {
                trackers: false,
                dht: false,
                pex: false,
                web_seeds: false,
                download_limit: 512 * 1024,
                ..Default::default()
            };
            options.peers = vec![address];
            options.diagnostics = diagnostics;
            let mut observer = session.admit(options).unwrap();
            assert_eq!(observer.snapshot().diagnostics.is_some(), diagnostics);
            tokio::time::timeout(Duration::from_secs(5), async {
                while !observer.snapshot().progress.completed {
                    assert!(
                        observer.changed().await,
                        "TCP torrent stopped before completion"
                    );
                }
            })
            .await
            .expect("TCP download progress");
            session.shutdown().await.unwrap();
            let snapshot = observer.snapshot();
            assert!(snapshot.progress.joined && snapshot.progress.completed);
            assert_eq!(
                snapshot.progress.verified_bytes,
                snapshot.progress.total_bytes
            );
            assert!(snapshot.progress.download_bytes_per_second.is_finite());
            assert!(snapshot.progress.upload_bytes_per_second.is_finite());
            assert_eq!(snapshot.diagnostics.is_some(), diagnostics);
            assert_eq!(&*snapshot.progress.verified_piece_map, &[true; 9]);
            std::fs::read(output.0.join("x")).unwrap()
        });
    }
    while let Some(download) = downloads.join_next().await {
        assert_eq!(download.unwrap(), payload);
    }
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "payload budgets must outlive the initial burst"
    );
    daemon.stop().await;
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), payload);
    let hits = source.hits.load(Ordering::Relaxed);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    daemon.state(&id, "seeding").await;
    assert_eq!(source.hits.load(Ordering::Relaxed), hits);
    daemon.call(json!({"op":"pause","id":id})).await;
    daemon.state(&id, "paused").await;
    let mut damaged = payload.clone();
    damaged[PIECE_LEN + 17] ^= 0xff;
    *damaged.last_mut().unwrap() ^= 0xff;
    std::fs::write(root.0.join("downloads/x"), damaged).unwrap();
    daemon.call(json!({"op":"recheck","id":id})).await;
    let row = daemon.state(&id, "paused").await;
    assert_eq!(row["verified_bytes"], 7 * PIECE_LEN);
    assert_eq!(
        source.hits.load(Ordering::Relaxed),
        hits,
        "recheck must not download"
    );
    let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
    let resume: Vec<u8> = db
        .query_row("SELECT resume FROM torrents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        spill::protocol::resume::resume_piece_map(&meta, &resume).unwrap(),
        [true, false, true, true, true, true, true, true, false]
    );
    daemon.call(json!({"op":"resume","id":id})).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], payload.len());
    daemon.stop().await;
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), payload);
}

#[tokio::test]
async fn recheck_completion_controls_download_and_seed_admission() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&metainfo(Some(&source.url)), None).await;
    daemon.state(&id, "seeding").await;
    daemon
        .call(json!({"op":"set","id":id,"values":{"web_seeds":false}}))
        .await;
    daemon.call(json!({"op":"pause","id":id})).await;
    std::fs::write(root.0.join("downloads/x"), b"xbc").unwrap();
    daemon.call(json!({"op":"recheck","id":id})).await;
    let row = daemon.state(&id, "paused").await;
    assert_eq!(row["verified_bytes"], 0);
    let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
    let complete: bool = db
        .query_row("SELECT complete FROM torrents", [], |r| r.get(0))
        .unwrap();
    assert!(!complete);
    daemon.call(json!({"op":"resume","id":id})).await;
    daemon.state(&id, "downloading").await;
    daemon.call(json!({"op":"pause","id":id})).await;
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    daemon.call(json!({"op":"recheck","id":id})).await;
    let row = daemon.state(&id, "paused").await;
    assert_eq!(row["verified_bytes"], 3);
    let complete: bool = db
        .query_row("SELECT complete FROM torrents", [], |r| r.get(0))
        .unwrap();
    assert!(complete);
    daemon.stop().await;

    let mut daemon = Daemon::start_with_caps(&root.0, "web_seeds=false", None, true, 0, 1).await;
    daemon.call(json!({"op":"resume","id":id})).await;
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
}

#[tokio::test]
async fn directory_conflicts_are_rejected_without_claiming_payload_paths() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/test/a", b"abc"), ("/test/b", b"def")], false).await;
    let bytes = multifile_metainfo(&source.url, &[("a", b"abc", false), ("b", b"def", false)]);
    for retry in [false, true] {
        let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
        let conflict = root.0.join("downloads/test/b.part");
        std::fs::create_dir_all(&conflict).unwrap();
        let id = daemon.add(&bytes, None).await;
        let row = daemon.failed(&id).await;
        let error = row["last_error"].as_str().unwrap();
        assert!(error.contains("payload test/b"), "{error}");
        assert!(error.contains("file already exists"), "{error}");
        assert!(!root.0.join("downloads/test/a.part").exists());
        daemon.stop().await;

        let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
        if retry {
            std::fs::remove_dir(&conflict).unwrap();
            daemon.call(json!({"op":"resume","id":id})).await;
            daemon.state(&id, "seeding").await;
            assert_eq!(
                std::fs::read(root.0.join("downloads/test/a")).unwrap(),
                b"abc"
            );
            assert_eq!(
                std::fs::read(root.0.join("downloads/test/b")).unwrap(),
                b"def"
            );
        }
        daemon
            .call(json!({"op":"remove","id":id,"delete_payload":true}))
            .await;
        if retry {
            assert!(!root.0.join("downloads/test").exists());
        } else {
            assert!(conflict.is_dir());
            std::fs::remove_dir_all(root.0.join("downloads/test")).unwrap();
        }
        assert_eq!(daemon.raw(json!({"op":"get","id":id})).await["code"], 3);
        daemon.stop().await;
    }
}

#[tokio::test]
async fn failed_add_does_not_claim_foreign_partial_files() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/test/a", b"abc"), ("/test/b", b"def")], false).await;
    let bytes = multifile_metainfo(&source.url, &[("a", b"abc", false), ("b", b"def", false)]);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let foreign = root.0.join("downloads/test/a.part");
    std::fs::create_dir_all(foreign.parent().unwrap()).unwrap();
    std::fs::write(&foreign, b"unrelated").unwrap();
    let id = daemon.add(&bytes, None).await;
    let row = daemon.failed(&id).await;
    assert!(
        row["last_error"]
            .as_str()
            .unwrap()
            .contains("file already exists")
    );
    daemon
        .call(json!({"op":"remove","id":id,"delete_payload":true}))
        .await;
    assert_eq!(std::fs::read(&foreign).unwrap(), b"unrelated");
    assert!(!root.0.join("downloads/test/b.part").exists());
    assert_eq!(daemon.raw(json!({"op":"get","id":id})).await["code"], 3);
    daemon.stop().await;
}

#[tokio::test]
async fn payload_removal_preserves_unrelated_files_and_removes_catalog_entry() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/test/a", b"abc")], false).await;
    let bytes = multifile_metainfo(&source.url, &[("a", b"abc", false)]);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    let stray = root.0.join("downloads/test/keep");
    std::fs::write(&stray, b"unrelated").unwrap();
    daemon
        .call(json!({"op":"remove","id":id,"delete_payload":true}))
        .await;
    assert!(!root.0.join("downloads/test/a").exists());
    assert!(!root.0.join("downloads/test/a.part").exists());
    assert_eq!(std::fs::read(&stray).unwrap(), b"unrelated");
    assert_eq!(daemon.raw(json!({"op":"get","id":id})).await["code"], 3);
    assert!(
        daemon.call(json!({"op":"list"})).await["torrents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    daemon.stop().await;
}
