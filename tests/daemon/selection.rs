use crate::common::{Daemon, DaemonConfig, WebSeed, multifile_metainfo};
use crate::payload::Temp;
use serde_json::json;

#[tokio::test]
async fn padding_is_synthesized_across_pieces_and_never_materialized() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/test/a", b"abc"), ("/test/b", b"WXYZ")], false).await;
    let bytes = multifile_metainfo(
        &source.url,
        &[
            ("a", b"abc", false),
            ("pad", b"\0\0", true),
            ("empty", b"", false),
            ("b", b"WXYZ", false),
        ],
    );
    let mut daemon = Daemon::start(&root.0, DaemonConfig::default()).await;
    let id = daemon.add(&bytes, None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 9);
    let output = root.0.join("downloads/test");
    assert_eq!(std::fs::read(output.join("a")).unwrap(), b"abc");
    assert_eq!(std::fs::read(output.join("b")).unwrap(), b"WXYZ");
    assert_eq!(std::fs::metadata(output.join("empty")).unwrap().len(), 0);
    assert!(!output.join("pad").exists());
    let files = daemon.call(json!({"op":"files","id":id})).await;
    assert_eq!(files["files"][1]["priority"], "skip");
    assert!(
        source
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(p, _, _)| p == "/test/a" || p == "/test/b")
    );
    daemon.stop().await;
}

#[tokio::test]
async fn file_selection_fetches_boundary_bytes_but_not_skipped_pieces() {
    let root = Temp::new();
    let source = WebSeed::files(
        &[
            ("/test/a", b"abc"),
            ("/test/b", b"DEFGH"),
            ("/test/c", b"ijkl"),
        ],
        false,
    )
    .await;
    let bytes = multifile_metainfo(
        &source.url,
        &[
            ("a", b"abc", false),
            ("b", b"DEFGH", false),
            ("c", b"ijkl", false),
        ],
    );
    let mut daemon = Daemon::start(
        &root.0,
        DaemonConfig {
            web_seeds: false,
            ..Default::default()
        },
    )
    .await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "downloading").await;
    daemon
        .call(json!({"op":"set","id":id,"values":{
            "file_priorities":["high","skip","skip"],"web_seeds":true
        }}))
        .await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(
        row["verified_bytes"], 4,
        "only the shared first piece is needed"
    );
    assert_eq!(
        std::fs::read(root.0.join("downloads/test/a")).unwrap(),
        b"abc"
    );
    {
        let requests = source.requests.lock().unwrap();
        assert!(requests.iter().any(|r| r == &("/test/a".into(), 0, 2)));
        assert!(requests.iter().any(|r| r == &("/test/b".into(), 0, 0)));
        assert_eq!(
            requests.len(),
            2,
            "no wholly skipped pieces should be fetched"
        );
    }
    // Expanding the selection must leave seeding and fetch the missing pieces.
    daemon
        .call(json!({"op":"set","id":id,"values":{
            "file_priorities":["normal","normal","normal"]
        }}))
        .await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 12);
    assert_eq!(
        std::fs::read(root.0.join("downloads/test/b")).unwrap(),
        b"DEFGH"
    );
    assert_eq!(
        std::fs::read(root.0.join("downloads/test/c")).unwrap(),
        b"ijkl"
    );
    daemon.stop().await;
}

#[tokio::test]
async fn magnet_selection_merges_before_metadata_and_survives_restart() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let root = Temp::new();
        let meta = spill::engine::profile::Profile::default().parse_owned(
            include_bytes!("../protocol/fixtures/v2.torrent"),
        ).unwrap();
        let last = meta.layout().files().iter().rposition(|f| f.kind() == spill::protocol::metainfo::FileKind::Data).unwrap();
        let middle = meta.layout().files().iter().position(|f| f.length() > u64::from(meta.layout().piece_length())).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (url, tracker) = crate::metadata_peer::tracker(&[listener.local_addr().unwrap()]).await;
        let source = tokio::spawn(crate::metadata_peer::serve_full(listener, meta.clone(), 0));
        let magnet = format!("magnet:?xt=urn:btmh:1220{}&tr={url}", meta.info_hash().v2().unwrap());
        let mut daemon = Daemon::start(&root.0, DaemonConfig {
            trackers: true, web_seeds: false, utp: Some(false), ..Default::default()
        }).await;
        let id = daemon.call(json!({"op":"add","magnet":format!("{magnet}&so=0,4294967295"),"paused":true})).await["id"].as_str().unwrap().to_owned();
        for _ in 0..128 {
            assert_eq!(daemon.call(json!({"op":"add","magnet":format!("{magnet}&so=0")})).await["id"], id);
        }
        assert_eq!(daemon.call(json!({"op":"add","magnet":format!("{magnet}&so={last}"),"output_dir":root.0.join("ignored")})).await["id"], id);
        assert!(!root.0.join("ignored").exists());
        daemon.state(&id, "paused").await;
        daemon.call(json!({"op":"resume","id":id})).await;
        let completed = daemon.state(&id, "seeding").await;
        assert_eq!(completed["wanted_bytes"], meta.layout().files()[0].length() + meta.layout().files()[last].length());
        assert!(completed["verified_bytes"].as_u64().unwrap() < completed["total_bytes"].as_u64().unwrap());
        let files = daemon.call(json!({"op":"files","id":id})).await;
        for (index, file) in files["files"].as_array().unwrap().iter().enumerate() {
            assert_eq!(file["priority"], if index == 0 || index == last { "normal" } else { "skip" });
        }
        let address = daemon.call(json!({"op":"health"})).await["listen"].as_str().unwrap().to_owned();
        let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
        crate::v2_peer::initiate(&mut peer, meta.info_hash().wire_key(), 77, false).await;
        for selection in ["0", "4294967295"] {
            assert_eq!(daemon.call(json!({"op":"add","magnet":format!("{magnet}&so={selection}")})).await["id"], id);
        }
        assert_eq!(daemon.raw(json!({"op":"add","magnet":format!("{magnet}&so=2-1")})).await["code"], 2);
        assert!(tokio::time::timeout(std::time::Duration::from_millis(200), async {
            loop { crate::v2_peer::read_frame(&mut peer).await.expect("no-op add disconnected the peer"); }
        }).await.is_err());
        drop(peer);
        daemon.call(json!({"op":"pause","id":id})).await;
        daemon.stop().await;
        source.abort();
        tracker.abort();
        let mut daemon = Daemon::start_config(&root.0).await;
        assert_eq!(daemon.call(json!({"op":"files","id":id})).await, files);
        daemon.call(json!({"op":"resume","id":id})).await;
        assert_eq!(daemon.state(&id, "seeding").await["wanted_bytes"], completed["wanted_bytes"]);
        daemon.call(json!({"op":"pause","id":id})).await;
        let mut priorities: Vec<_> = files["files"].as_array().unwrap().iter().map(|f| f["priority"].clone()).collect();
        priorities[0] = json!("high");
        daemon.call(json!({"op":"set","id":id,"values":{"file_priorities":priorities}})).await;
        assert_eq!(daemon.call(json!({"op":"add","magnet":format!("{magnet}&so={middle}")})).await["id"], id);
        daemon.state(&id, "paused").await;
        let merged = daemon.call(json!({"op":"files","id":id})).await;
        assert_eq!(merged["files"][0]["priority"], "high");
        assert_eq!(merged["files"][middle]["priority"], "normal");
        assert_eq!(merged["files"][last]["priority"], "normal");
        assert_eq!(daemon.raw(json!({"op":"add","magnet":magnet})).await["code"], 2);
        assert_eq!(daemon.raw(json!({"op":"add","magnet":format!("{magnet}&xt=urn:btih:{}&so=0", "00".repeat(20))})).await["code"], 2);
        assert_eq!(daemon.call(json!({"op":"add","magnet":format!("{magnet}&so=4294967295")})).await["id"], id);
        assert_eq!(daemon.call(json!({"op":"files","id":id})).await, merged);
        daemon.state(&id, "paused").await;
        daemon.stop().await;
    }).await.expect("durable magnet selection and additive duplicate handling");
}
