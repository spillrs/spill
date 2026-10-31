use crate::common::{BIN, Daemon, WebSeed, metainfo};
use crate::payload::Temp;
use serde_json::Value;
use std::time::Duration;
use tokio::process::Command;

#[tokio::test]
async fn client_commands_are_top_level_without_a_ctl_alias() {
    async fn client(daemon: &Daemon, args: &[&str]) -> Value {
        let output = Command::new(BIN)
            .arg("--socket")
            .arg(&daemon.socket)
            .arg("--json")
            .args(args)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let torrent = root.0.join("x.torrent");
    std::fs::write(&torrent, metainfo(Some(&source.url))).unwrap();
    let added = client(
        &daemon,
        &[
            "add",
            torrent.to_str().unwrap(),
            "--paused",
            "--set",
            "web_seeds=false",
        ],
    )
    .await;
    let id = added["id"].as_str().unwrap();
    let paused = daemon.state(id, "paused").await;
    assert_eq!(paused["settings"]["web_seeds"], false);
    assert!(!root.0.join("downloads/x").exists());
    client(&daemon, &["set", id, "web_seeds=true"]).await;
    client(&daemon, &["resume", id]).await;
    daemon.state(id, "seeding").await;

    assert_eq!(client(&daemon, &["get", id]).await["verified_bytes"], 3);
    let details = Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .args(["get", id])
        .output()
        .await
        .unwrap();
    assert!(details.status.success());
    let details: Value = serde_json::from_slice(&details.stdout).unwrap();
    assert_eq!(details["id"], id);
    assert_eq!(details["state"], "seeding");
    assert_eq!(details["verified_bytes"], 3);
    assert!(details["settings"].is_object());
    assert!(details["transfer"].is_object());
    let settings = Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .args(["set", id, "upload_limit=123"])
        .output()
        .await
        .unwrap();
    assert!(settings.status.success());
    assert!(
        String::from_utf8(settings.stdout)
            .unwrap()
            .contains("upload_limit=123")
    );
    assert_eq!(
        client(&daemon, &["files", id]).await["files"][0]["path"],
        "x"
    );
    for command in ["list", "status"] {
        assert_eq!(client(&daemon, &[command]).await["torrents"][0]["id"], id);
    }
    assert_eq!(
        client(&daemon, &["health"]).await["pid"],
        daemon.child.id().unwrap()
    );
    for (command, state) in [
        ("pause", "paused"),
        ("recheck", "paused"),
        ("resume", "seeding"),
    ] {
        client(&daemon, &[command, id]).await;
        daemon.state(id, state).await;
    }
    client(&daemon, &["remove", id]).await;
    assert!(
        client(&daemon, &["list"]).await["torrents"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let rejected = Command::new(BIN)
        .args(["ctl", "list"])
        .output()
        .await
        .unwrap();
    assert_eq!(rejected.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("unrecognized subcommand 'ctl'"));

    assert_eq!(client(&daemon, &["shutdown"]).await["stopping"], true);
    assert!(
        tokio::time::timeout(Duration::from_secs(4), daemon.child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}
