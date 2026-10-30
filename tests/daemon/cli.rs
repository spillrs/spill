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
    let added = client(&daemon, &["add", torrent.to_str().unwrap()]).await;
    let id = added["id"].as_str().unwrap();
    daemon.state(id, "seeding").await;

    assert_eq!(client(&daemon, &["get", id]).await["verified_bytes"], 3);
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
