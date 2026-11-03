use crate::common::{Daemon, DaemonConfig, WebSeed, http, metainfo};
use crate::payload::Temp;
use base64::{Engine, prelude::BASE64_STANDARD};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn web_smoke_dispatches_in_process_with_csrf_protection() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut daemon = Daemon::start(
        &root.0,
        DaemonConfig {
            web_listen: Some(address),
            ..Default::default()
        },
    )
    .await;
    let index = http(address, "GET", "/", None, b"").await;
    assert!(index.starts_with("HTTP/1.1 200"));
    assert!(index.contains("Selected torrent details"));
    assert!(index.contains("data-sort=\"downloaded_bytes\""));
    assert!(index.contains("data-sort=\"uploaded_bytes\""));
    for asset in ["/app.js", "/details.js"] {
        let response = http(address, "GET", asset, None, b"").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{asset}: {response}");
        assert!(response.contains("text/javascript"));
    }
    let style = http(address, "GET", "/style.css", None, b"").await;
    assert!(style.starts_with("HTTP/1.1 200"));
    assert!(style.contains("text/css"));
    assert!(style.contains("--paper:#f3f6fa"));
    assert!(style.contains("ui-monospace"));
    let logo = http(address, "GET", "/logo.svg", None, b"").await;
    assert!(logo.starts_with("HTTP/1.1 200"));
    assert!(logo.contains("image/svg+xml"));
    let token = index
        .split("const token='")
        .nth(1)
        .unwrap()
        .split('\'')
        .next()
        .unwrap();
    let denied = http(address, "POST", "/api", None, b"{\"op\":\"shutdown\"}").await;
    assert!(denied.starts_with("HTTP/1.1 403"));
    let bytes = metainfo(Some(&source.url));
    let denied = http(address, "POST", "/api/upload", None, &bytes).await;
    assert!(denied.starts_with("HTTP/1.1 403"));
    assert!(
        daemon.call(json!({"op":"list"})).await["torrents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let output = root.0.join("web output");
    let query = format!(
        "output_dir={}",
        output.to_str().unwrap().replace(' ', "%20")
    );
    let uploaded = http(
        address,
        "POST",
        &format!("/api/upload?{query}"),
        Some(token),
        &bytes,
    )
    .await;
    assert!(uploaded.starts_with("HTTP/1.1 200"), "{uploaded}");
    let result: serde_json::Value =
        serde_json::from_str(uploaded.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(result["code"], 0, "{result}");
    let id = result["data"]["id"].as_str().unwrap().to_owned();
    daemon.state(&id, "seeding").await;
    assert_eq!(std::fs::read(output.join("x")).unwrap(), b"abc");
    let response = http(
        address,
        "POST",
        "/api",
        Some(token),
        json!({"op":"set","id":id,"values":{"upload_limit":321}})
            .to_string()
            .as_bytes(),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("321"));
    assert_eq!(
        daemon.call(json!({"op":"get","id":id})).await["settings"]["upload_limit"],
        321
    );
    let health = http(address, "POST", "/api", Some(token), b"{\"op\":\"health\"}").await;
    assert!(health.contains(&daemon.child.id().unwrap().to_string()));
    daemon.stop().await;
}

#[tokio::test]
async fn web_add_applies_defaults_and_paused_policy_before_starting_on_ipv6() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut daemon = Daemon::start(
        &root.0,
        DaemonConfig {
            listen: "[::1]:0".parse().unwrap(),
            web_listen: Some(address),
            pex: true,
            upload_limit: 321,
            active_downloads: 4,
            active_seeds: 16,
            peers: None,
            cache_bytes: None,
            write_queue_bytes: None,
            ..Default::default()
        },
    )
    .await;
    let index = http(address, "GET", "/", None, b"").await;
    let defaults: serde_json::Value = serde_json::from_str(
        index
            .split("const addDefaults=")
            .nth(1)
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(defaults["settings"]["dht"], false);
    assert_eq!(defaults["settings"]["pex"], true);
    assert_eq!(defaults["settings"]["upload_limit"], 321);
    let token = index
        .split("const token='")
        .nth(1)
        .unwrap()
        .split('\'')
        .next()
        .unwrap();
    let bytes = metainfo(Some(&source.url));
    for settings in [
        json!({"unknown":false}),
        json!({"seed_ratio":-1}),
        json!({"file_priorities":["normal","skip"]}),
    ] {
        let response = http(
            address,
            "POST",
            "/api",
            Some(token),
            json!({
                "op":"add", "metainfo":BASE64_STANDARD.encode(&bytes), "settings":settings
            })
            .to_string()
            .as_bytes(),
        )
        .await;
        let result: serde_json::Value =
            serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(result["code"], 2, "{result}");
        assert!(
            daemon.call(json!({"op":"list"})).await["torrents"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(!root.0.join("downloads").exists());
    }
    let settings = json!({"trackers":false,"dht":true,"pex":false,"web_seeds":false}).to_string();
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("paused", "true")
        .append_pair("settings", &settings)
        .finish();
    let uploaded = http(
        address,
        "POST",
        &format!("/api/upload?{query}"),
        Some(token),
        &bytes,
    )
    .await;
    let added: serde_json::Value =
        serde_json::from_str(uploaded.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(added["code"], 0, "{added}");
    let id = added["data"]["id"].as_str().unwrap();
    let row = daemon.state(id, "paused").await;
    assert_eq!(row["settings"]["web_seeds"], false);
    assert_eq!(row["settings"]["dht"], true);
    assert_eq!(row["settings"]["pex"], false);
    assert_eq!(row["settings"]["upload_limit"], 321);
    assert!(!root.0.join("downloads/x").exists());
    assert_eq!(daemon.call(json!({"op":"health"})).await["running"], 0);
    daemon.call(json!({"op":"resume","id":id})).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(source.hits.load(Ordering::Relaxed), 0);
    daemon
        .call(json!({"op":"set","id":id,"values":{"web_seeds":true}}))
        .await;
    daemon.state(id, "seeding").await;
    assert_eq!(source.hits.load(Ordering::Relaxed), 1);
    daemon.stop().await;
}
