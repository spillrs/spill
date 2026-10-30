use crate::common::{Daemon, WebSeed, http, metainfo};
use crate::payload::Temp;
use serde_json::json;

#[tokio::test]
async fn web_smoke_dispatches_in_process_with_csrf_protection() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", Some(address)).await;
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
