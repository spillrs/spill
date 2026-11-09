use crate::{common::BIN, payload::Temp};
use serde_json::Value;
use sha1::{Digest, Sha1};
use spill::{
    engine::profile::Profile,
    protocol::bencode::{self, Limits, Value as Bencode},
};
use std::{os::unix::fs::symlink, process::Command};

#[test]
fn offline_creation_supports_mktorrent_options_and_preserves_info_identity() {
    let root = Temp::new();
    let payload = (0..100003).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    std::fs::write(root.0.join("payload"), &payload).unwrap();
    let args = [
        "-a",
        "https://one.example/announce,https://two.example/announce",
        "--announce",
        "udp://backup.example:80/announce",
        "-c",
        "a comment",
        "-d",
        "-l",
        "15",
        "-n",
        "renamed",
        "-p",
        "-s",
        "source",
        "-w",
        "https://seed.example/a,https://seed.example/b",
        "--web-seed",
        "https://seed.example/c",
    ];
    let run = |extra: &[&str]| {
        Command::new(BIN)
            .current_dir(&root.0)
            .args([
                "--socket",
                "nonexistent.sock",
                "--json",
                "create",
                "payload",
            ])
            .args(args)
            .args(extra)
            .output()
            .unwrap()
    };
    let output = run(&["-o", "one.torrent", "-t", "1", "-v"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("Hashed 4 of 4 pieces"));
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["output"], "one.torrent");
    assert_eq!(response["total_bytes"], payload.len());
    assert_eq!(response["piece_count"], 4);
    let bytes = std::fs::read(root.0.join("one.torrent")).unwrap();
    let meta = Profile::default().parse(&bytes).unwrap();
    assert_eq!(response["info_hash"], meta.info_hash().to_string());
    assert_eq!(meta.name(), b"renamed");
    assert!(meta.is_private());
    assert!(!meta.layout().is_multi_file());
    let doc = meta.document();
    assert!(doc.get(b"creation date").is_none());
    assert!(doc.get(b"source").is_none());
    assert_eq!(
        doc.get(b"comment").unwrap().value(),
        &Bencode::Bytes(b"a comment")
    );
    let info = doc.get(b"info").unwrap();
    assert_eq!(
        info.get(b"source").unwrap().value(),
        &Bencode::Bytes(b"source")
    );
    assert!(info.get(b"comment").is_none());
    let Bencode::List(tiers) = doc.get(b"announce-list").unwrap().value() else {
        panic!("tracker tiers")
    };
    assert_eq!(tiers.len(), 2);
    let Bencode::List(first) = tiers[0].value() else {
        panic!("first tier")
    };
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].value(), doc.get(b"announce").unwrap().value());
    let Bencode::List(seeds) = doc.get(b"url-list").unwrap().value() else {
        panic!("web seeds")
    };
    assert_eq!(seeds.len(), 3);
    for (index, piece) in payload.chunks(1 << 15).enumerate() {
        assert_eq!(
            meta.piece_hash(index as u32).unwrap().as_slice(),
            Sha1::digest(piece).as_slice()
        );
    }
    let hybrid = run(&["-F", "hybrid", "--json", "-o", "hybrid.torrent"]);
    assert!(
        hybrid.status.success(),
        "{}",
        String::from_utf8_lossy(&hybrid.stderr)
    );
    let response: Value = serde_json::from_slice(&hybrid.stdout).unwrap();
    let hybrid_bytes = std::fs::read(root.0.join("hybrid.torrent")).unwrap();
    let hybrid_meta = Profile::default().parse(&hybrid_bytes).unwrap();
    assert!(matches!(
        hybrid_meta.info_hash(),
        spill::protocol::identity::TorrentIdentity::Hybrid(..)
    ));
    assert_eq!(response["format"], "hybrid");
    assert_eq!(
        response["v1"],
        hybrid_meta.info_hash().v1().unwrap().to_string()
    );
    assert_eq!(
        response["v2"],
        hybrid_meta.info_hash().v2().unwrap().to_string()
    );
    assert_eq!(response["info_hash"], response["v1"]);
    let second = run(&["--output", "two.torrent", "--threads", "4"]);
    assert!(second.status.success());
    assert_eq!(std::fs::read(root.0.join("two.torrent")).unwrap(), bytes);
    assert!(!run(&["-o", "one.torrent"]).status.success());
    assert_eq!(std::fs::read(root.0.join("one.torrent")).unwrap(), bytes);
    assert!(!run(&["-o", "payload"]).status.success());
    assert_eq!(std::fs::read(root.0.join("payload")).unwrap(), payload);
    assert!(
        std::fs::read_dir(&root.0).unwrap().all(|p| !p
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp"))
    );
}

#[test]
fn offline_inspection_supports_all_formats_stdin_and_magnets() {
    let root = Temp::new();
    std::fs::write(root.0.join("payload"), vec![42; 70000]).unwrap();
    let inspect = |source: &str| {
        Command::new(BIN)
            .current_dir(&root.0)
            .args(["--socket", "missing.sock", "inspect", source, "--json"])
            .output()
            .unwrap()
    };
    for format in ["v1", "v2", "hybrid"] {
        let name = format!("{format}.torrent");
        let created = Command::new(BIN)
            .current_dir(&root.0)
            .args([
                "create",
                "payload",
                "-F",
                format,
                "-o",
                &name,
                "-p",
                "-a",
                "https://tracker.example/announce",
                "-c",
                "a comment",
                "-s",
                "source",
                "--json",
            ])
            .output()
            .unwrap();
        assert!(
            created.status.success(),
            "{}",
            String::from_utf8_lossy(&created.stderr)
        );
        let created: Value = serde_json::from_slice(&created.stdout).unwrap();
        let output = inspect(&name);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let data: Value = serde_json::from_slice(&output.stdout).unwrap();
        for key in [
            "format",
            "info_hash",
            "v1",
            "v2",
            "piece_count",
            "total_bytes",
        ] {
            assert_eq!(data[key], created[key], "{format}: {key}");
        }
        assert_eq!(data["private"], true);
        assert_eq!(data["data_bytes"], 70000);
        assert_eq!(data["files"][0]["path"], "payload");
        assert_eq!(data["files"][0]["length"], 70000);
        assert_eq!(data["announce"], "https://tracker.example/announce");
        assert_eq!(data["comment"], "a comment");
        assert_eq!(data["source"], "source");
        let input = std::fs::File::open(root.0.join(&name)).unwrap();
        let stdin = Command::new(BIN)
            .args(["inspect", "-", "--json"])
            .stdin(input)
            .output()
            .unwrap();
        assert!(stdin.status.success());
        assert_eq!(
            serde_json::from_slice::<Value>(&stdin.stdout).unwrap(),
            data
        );
    }
    let hash = "0123456789012345678901234567890123456789";
    let output = inspect(&format!(
        "MAGNET:?xt=urn:btih:{hash}&dn=hello%20world&so=1-3"
    ));
    assert!(output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["kind"], "magnet");
    assert_eq!(data["info_hash"], hash);
    assert_eq!(data["name"], "hello world");
    assert_eq!(data["parameters"][2]["value"], "1-3");
    assert_eq!(inspect("magnet:?xt=invalid").status.code(), Some(2));
    std::fs::write(root.0.join("bad.torrent"), b"not bencode").unwrap();
    let invalid = inspect("bad.torrent");
    assert_eq!(invalid.status.code(), Some(2));
    assert!(invalid.stdout.is_empty());
    assert_eq!(inspect("missing.torrent").status.code(), Some(1));
}

#[test]
fn offline_creation_defaults_trackerless_web_seeds_and_output_errors() {
    let root = Temp::new();
    std::fs::create_dir(root.0.join("bundle")).unwrap();
    std::fs::write(root.0.join("bundle/empty"), []).unwrap();
    let run = |args: &[&str]| {
        Command::new(BIN)
            .current_dir(&root.0)
            .arg("create")
            .args(args)
            .output()
            .unwrap()
    };
    let output = run(&["bundle", "--web-seed", "https://seed.example/"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bytes = std::fs::read(root.0.join("bundle.torrent")).unwrap();
    let meta = Profile::default().parse(&bytes).unwrap();
    assert_eq!(meta.name(), b"bundle");
    assert_eq!(meta.layout().piece_length(), 1 << 18);
    assert_eq!(meta.layout().piece_count(), 0);
    assert!(meta.layout().is_multi_file());
    assert!(meta.document().get(b"announce").is_none());
    assert!(meta.document().get(b"announce-list").is_none());
    assert!(matches!(
        meta.document().get(b"creation date").unwrap().value(),
        Bencode::Integer(_)
    ));
    assert_eq!(
        meta.document().get(b"url-list").unwrap().value(),
        &Bencode::Bytes(b"https://seed.example/")
    );
    assert!(!run(&["bundle"]).status.success());
    for invalid in [
        vec!["bundle", "-l", "14"],
        vec!["bundle", "-t", "0"],
        vec!["bundle", "-n", "../escape"],
        vec!["bundle", "-a", "https://tracker.example/,"],
        vec!["bundle", "-w", "ftp://seed.example/"],
    ] {
        let output = run(&invalid);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{invalid:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    symlink("missing-target", root.0.join("dangling.torrent")).unwrap();
    assert!(!run(&["bundle", "-o", "dangling.torrent"]).status.success());
    assert!(!root.0.join("missing-target").exists());
    assert!(
        !run(&["bundle", "-o", "missing-parent/out.torrent"])
            .status
            .success()
    );
    std::fs::create_dir(root.0.join("empty-directory")).unwrap();
    assert_eq!(run(&["empty-directory"]).status.code(), Some(2));
    assert!(!root.0.join("empty-directory.torrent").exists());
    // Both publication failures and validation failures leave no temporary output.
    assert!(
        std::fs::read_dir(&root.0).unwrap().all(|p| !p
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp"))
    );
    assert_eq!(
        bencode::encode(meta.document().get(b"info").unwrap(), Limits::default()).unwrap(),
        meta.info_bytes()
    );
}
