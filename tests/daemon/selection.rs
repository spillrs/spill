use crate::common::{Daemon, WebSeed, multifile_metainfo};
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
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
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
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
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
