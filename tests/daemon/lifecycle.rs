use crate::common::{BIN, Daemon, WebSeed, metainfo};
use crate::payload::{Temp, seed_metainfo};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn add_download_seed() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&metainfo(Some(&source.url)), None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 3);
    assert!(row["transfer"]["trackers"].is_array());
    assert!(row["transfer"]["download_bytes_per_second"].is_number());
    assert!(row["transfer"]["upload_bytes_per_second"].is_number());
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
    assert_eq!(source.hits.load(Ordering::Relaxed), 1);
    daemon.stop().await;
}

#[tokio::test]
async fn resuming_a_queued_torrent_does_not_preempt_a_running_download() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = Temp::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/payload", listener.local_addr().unwrap());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let gate = release.clone();
    let seed = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await.unwrap());
        }
        gate.notified().await;
        stream.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 0-2/3\r\nConnection: close\r\n\r\nabc").await.unwrap();
    });
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let a = daemon.add(&seed_metainfo("a", b"abc", None), None).await;
    daemon.state(&a, "downloading").await;
    daemon.call(json!({"op":"pause", "id":a})).await;
    let b = daemon.add(&metainfo(Some(&url)), None).await;
    daemon.state(&b, "downloading").await;
    daemon.call(json!({"op":"resume", "id":a})).await;
    for _ in 0..5 {
        assert_eq!(
            daemon.call(json!({"op":"get", "id":a})).await["state"],
            "queued"
        );
        assert_eq!(
            daemon.call(json!({"op":"get", "id":b})).await["state"],
            "downloading"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    release.notify_one();
    daemon.state(&b, "seeding").await;
    daemon.state(&a, "downloading").await;
    daemon.stop().await;
    seed.await.unwrap();
}

#[tokio::test]
async fn ipv6_restart_keeps_torrents_running_when_lazy_dht_fails() {
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    let mut daemon = Daemon::start(&root.0, "dht=true\nweb_seeds=false", None).await;
    let id = daemon.add(&seed_metainfo("x", b"abc", None), None).await;
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
    let config = root.0.join("config.toml");
    let contents = std::fs::read_to_string(&config)
        .unwrap()
        .replace("127.0.0.1:0", "[::1]:0")
        .replace("dht=true", "dht=false");
    std::fs::write(&config, contents).unwrap();
    let log = root.0.join("stderr");
    daemon.child = tokio::process::Command::new(BIN)
        .arg("serve")
        .arg("--config")
        .arg(config)
        .stderr(std::fs::File::create(&log).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while tokio::net::UnixStream::connect(&daemon.socket)
            .await
            .is_err()
        {
            assert!(daemon.child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["settings"]["dht"], true);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(daemon.child.try_wait().unwrap().is_none());
    assert!(
        daemon.call(json!({"op":"health"})).await["listen"]
            .as_str()
            .unwrap()
            .starts_with("[::1]:")
    );
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
    assert_eq!(
        std::fs::read_to_string(log)
            .unwrap()
            .matches("DHT disabled:")
            .count(),
        1
    );
}

#[tokio::test]
async fn restart_resumes_from_verified_checkpoint_and_rejects_old_catalogs() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let bytes = metainfo(Some(&source.url));
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
    let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        5
    );
    let resume: Vec<u8> = db
        .query_row("SELECT resume FROM torrents", [], |r| r.get(0))
        .unwrap();
    assert!(!resume.is_empty());
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    daemon.state(&id, "seeding").await;
    assert_eq!(
        source.hits.load(Ordering::Relaxed),
        1,
        "resume should not download again"
    );
    daemon.stop().await;
    // A supplied resume must still be checked against the reopened files.
    // Same-length damage must invalidate the saved verified bit and be repaired.
    let payload = root.0.join("downloads/x");
    let modified = std::fs::metadata(&payload).unwrap().modified().unwrap();
    std::fs::write(&payload, b"bad").unwrap();
    let modified = modified + Duration::from_nanos(1);
    std::fs::File::options()
        .write(true)
        .open(&payload)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    assert_eq!(
        std::fs::metadata(&payload).unwrap().modified().unwrap(),
        modified
    );
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    daemon.state(&id, "seeding").await;
    assert_eq!(source.hits.load(Ordering::Relaxed), 2);
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
    daemon.call(json!({"op":"recheck","id":id})).await;
    daemon.state(&id, "paused").await;
    daemon.stop().await;
    db.execute_batch("PRAGMA user_version=3").unwrap();
    drop(db);
    let status = tokio::process::Command::new(BIN)
        .arg("serve")
        .arg("--config")
        .arg(root.0.join("config.toml"))
        .output()
        .await
        .unwrap();
    assert_eq!(status.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&status.stderr).contains("fresh state directory"));
}

#[tokio::test]
async fn pause_resume_remove_keep_and_delete_payload() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let bytes = metainfo(Some(&source.url));
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    daemon.call(json!({"op":"pause","id":id})).await;
    daemon.state(&id, "paused").await;
    daemon.call(json!({"op":"resume","id":id})).await;
    daemon.state(&id, "seeding").await;
    daemon
        .call(json!({"op":"remove","id":id,"delete_payload":false}))
        .await;
    assert!(root.0.join("downloads/x").exists());
    assert_eq!(daemon.raw(json!({"op":"get","id":id})).await["code"], 3);
    let output = root.0.join("second");
    let id = daemon.add(&bytes, Some(&output)).await;
    daemon.state(&id, "seeding").await;
    daemon
        .call(json!({"op":"remove","id":id,"delete_payload":true}))
        .await;
    assert!(!output.join("x").exists());
    assert!(root.0.join("downloads/x").exists());
    daemon.stop().await;
}

#[tokio::test]
async fn get_set_settings_take_effect_and_cli_uses_the_same_schema() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let id = daemon.add(&metainfo(Some(&source.url)), None).await;
    daemon.state(&id, "downloading").await;
    assert_eq!(source.hits.load(Ordering::Relaxed), 0);
    assert_eq!(
        daemon
            .raw(json!({"op":"set","id":id,"values":{"seed_ratio":-1.0}}))
            .await["code"],
        2
    );
    assert_eq!(
        daemon.call(json!({"op":"get","id":id})).await["settings"]["seed_ratio"],
        0.0
    );
    let result = tokio::process::Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .args([
            "set",
            &id,
            "download_limit=100",
            "upload_limit=200",
            "seed_ratio=0",
            "seed_time=1",
            "file_priorities=[\"high\"]",
            "trackers=false",
            "dht=false",
            "pex=false",
            "web_seeds=true",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let row = daemon.call(json!({"op":"get","id":id})).await;
    assert_eq!(row["settings"]["download_limit"], 100);
    assert_eq!(row["settings"]["file_priorities"], json!(["high"]));
    daemon.state(&id, "seeding").await;
    daemon.state(&id, "paused").await;
    let files = daemon.call(json!({"op":"files","id":id})).await;
    assert_eq!(files["files"][0]["priority"], "high");
    assert_eq!(
        daemon
            .raw(json!({"op":"set","id":id,"values":{"unknown_key":true}}))
            .await["code"],
        2
    );
    let output = tokio::process::Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .args(["list", "--json"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["torrents"][0]["id"],
        id
    );
    daemon.stop().await;
}
