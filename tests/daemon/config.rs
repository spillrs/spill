#[cfg(target_os = "macos")]
use crate::common::multifile_metainfo;
use crate::common::{BIN, Daemon};
use crate::payload::{Temp, seed_metainfo};
use std::time::Duration;

#[tokio::test]
async fn invalid_configuration_is_rejected_before_runtime_start() {
    let root = Temp::new();
    let config = root.0.join("invalid.toml");
    for (invalid, message) in [
        ("active_downloads=0\nactive_seeds=0\n", "nonzero active cap"),
        (
            "port_mapping_gateway=\"https://192.168.1.1/root.xml\"\n",
            "http URL",
        ),
        (
            "port_mapping_gateway=\"http://239.255.255.250/root.xml\"\n",
            "unicast host",
        ),
        (
            "port_mapping_gateway=\"http://0.0.0.0/root.xml\"\n",
            "unicast host",
        ),
        (
            "port_mapping_gateway=\"localhost\"\n",
            "invalid socket address",
        ),
        ("port_mapping_gateway=\"127.0.0.1:0\"\n", "nonzero port"),
        ("port_mapping_gateway=\"::1\"\n", "IPv4"),
        ("[defaults]\nseed_ratio=-1.0\n", "seed_ratio"),
        ("dht_bootstrap=[\"missing-port\"]\n", "invalid DHT routers"),
        ("peers=0\n", "invalid torrent resource limits"),
        ("cache_bytes=0\n", "invalid torrent resource limits"),
        ("write_queue_bytes=0\n", "invalid torrent resource limits"),
    ] {
        std::fs::write(&config, format!(
            "state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"127.0.0.1:0\"\n{invalid}",
            root.0.join("state"), root.0.join("downloads"), root.0.join("control.sock"),
        )).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            tokio::process::Command::new(BIN)
                .arg("serve")
                .arg("--config")
                .arg(&config)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("invalid config must not start a runtime")
        .unwrap();
        assert_eq!(result.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&result.stderr).contains(message));
        assert!(!root.0.join("control.sock").exists());
    }
}

#[tokio::test]
async fn platform_default_paths_and_port_work_for_daemon_and_cli() {
    let root = Temp::new();
    // Keep Darwin's default socket path within sockaddr_un's pathname limit.
    let home = Temp(std::path::Path::new("/tmp").join(root.0.file_name().unwrap()));
    std::fs::create_dir_all(home.0.join("Downloads")).unwrap();
    std::fs::write(home.0.join("Downloads/x"), b"abc").unwrap();
    let runtime = root.0.join("runtime");
    std::fs::create_dir_all(&runtime).unwrap();
    #[cfg(target_os = "macos")]
    let state = home.0.join("Library/Application Support/spill");
    #[cfg(not(target_os = "macos"))]
    let state = home.0.join(".local/state/spill");
    #[cfg(target_os = "macos")]
    let socket = state.join("spill.sock");
    #[cfg(not(target_os = "macos"))]
    let socket = runtime.join("spill.sock");
    let config = root.0.join("defaults.toml");
    std::fs::write(
        &config,
        "port_mapping=false\ndht_bootstrap=[]\ndefaults = {\n\
         dht=false,\ntrackers=false,\npex=false,\nweb_seeds=false,\n}\n",
    )
    .unwrap();
    let child = tokio::process::Command::new(BIN)
        .args(["serve", "--config"])
        .arg(config)
        .env("HOME", &home.0)
        .env("XDG_RUNTIME_DIR", &runtime)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut daemon = Daemon { child, socket };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !daemon.socket.exists() {
            assert!(daemon.child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let id = daemon.add(&seed_metainfo("x", b"abc", None), None).await;
    daemon.state(&id, "seeding").await;
    assert!(state.join("catalog.sqlite").exists());
    let health = daemon.call(serde_json::json!({"op":"health"})).await;
    assert_eq!(health["listen"], "0.0.0.0:6881");
    assert_eq!(health["advertised_port"], 6881);
    assert_eq!(health["admission_closed"], false);
    let output = tokio::process::Command::new(BIN)
        .args(["--json", "get", &id])
        .env("HOME", &home.0)
        .env("XDG_RUNTIME_DIR", runtime)
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(row["id"], id);
    daemon.stop().await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn macos_raises_file_limit_for_many_payload_files() {
    let root = Temp::new();
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    daemon.stop().await;
    let child = tokio::process::Command::new("/bin/sh")
        .args([
            "-c",
            "ulimit -S -n 256 && exec \"$@\"",
            "spill",
            BIN,
            "serve",
            "--config",
        ])
        .arg(root.0.join("config.toml"))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut daemon = Daemon {
        child,
        socket: root.0.join("control.sock"),
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !daemon.socket.exists() {
            assert!(daemon.child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    std::fs::create_dir_all(root.0.join("downloads/test")).unwrap();
    let names: Vec<_> = (0..300).map(|n| format!("file{n}")).collect();
    for name in &names {
        std::fs::write(root.0.join("downloads/test").join(name), b"abc").unwrap();
    }
    let files: Vec<_> = names
        .iter()
        .map(|name| (name.as_str(), b"abc".as_slice(), false))
        .collect();
    let bytes = multifile_metainfo("http://127.0.0.1:1/", &files);
    let id = daemon.add(&bytes, None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 900);
    daemon.stop().await;
}
