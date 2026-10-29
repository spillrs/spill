use crate::common::{BIN, Daemon, Temp, http, metainfo};
use base64::{Engine, prelude::BASE64_STANDARD};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
    task::{JoinHandle, JoinSet},
};

struct Source {
    url: String,
    hits: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Source {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let count = hits.clone();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let count = count.clone();
                clients.spawn(async move {
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") { request.push(stream.read_u8().await.unwrap()); }
                    count.fetch_add(1, Ordering::SeqCst);
                    let request = String::from_utf8(request).unwrap();
                    let path = request.split_whitespace().nth(1).unwrap();
                    let mut bytes = metainfo(None);
                    let response = match path {
                        "/download?key=secret" => format!("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n", bytes.len()),
                        "/private" => {
                            bytes.splice(bytes.len()-2..bytes.len()-2, b"7:privatei1e".iter().copied());
                            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n", bytes.len())
                        }
                        "/redirect" => format!("HTTP/1.1 302 Found\r\nLocation: http://localhost:{}/download?key=secret\r\nContent-Length: 0\r\n", address.port()),
                        "/loop" => "HTTP/1.1 302 Found\r\nLocation: /loop\r\nContent-Length: 0\r\n".into(),
                        "/bad-redirect" => "HTTP/1.1 302 Found\r\nLocation: file:///secret\r\nContent-Length: 0\r\n".into(),
                        "/large" => "HTTP/1.1 200 OK\r\nContent-Length: 16777217\r\n".into(),
                        "/stream" => {
                            stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
                            let chunk = vec![b'x'; 65536];
                            for _ in 0..257 {
                                if stream.write_all(b"10000\r\n").await.is_err() || stream.write_all(&chunk).await.is_err() || stream.write_all(b"\r\n").await.is_err() { return; }
                            }
                            let _ = stream.write_all(b"0\r\n\r\n").await;
                            return;
                        }
                        "/invalid" => {
                            bytes = b"not a torrent".to_vec();
                            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n", bytes.len())
                        }
                        _ => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n".into(),
                    };
                    let _ = stream.write_all(format!("{response}Connection: close\r\n\r\n").as_bytes()).await;
                    if matches!(path, "/download?key=secret" | "/private" | "/invalid") { let _ = stream.write_all(&bytes).await; }
                });
            }
        });
        Self {
            url: format!("http://{address}"),
            hits,
            task,
        }
    }
}

#[tokio::test]
async fn cli_fetches_redirected_url_and_restart_uses_saved_metainfo() {
    let root = Temp::new();
    let source = Source::new().await;
    let mut daemon = Daemon::start(&root.0, "", None).await;
    let output = root.0.join("url output");
    std::fs::create_dir(&output).unwrap();
    std::fs::write(output.join("x"), b"abc").unwrap();
    let result = Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .args([
            "--json",
            "add",
            &format!("{}/redirect", source.url),
            "--output",
        ])
        .arg(&output)
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let added: Value = serde_json::from_slice(&result.stdout).unwrap();
    let id = added["id"].as_str().unwrap();
    daemon.state(id, "seeding").await;
    assert_eq!(
        daemon.call(json!({"op":"get","id":id})).await["output_dir"],
        output.to_str().unwrap()
    );
    let duplicate = daemon
        .raw(json!({"op":"add","url":format!("{}/download?key=secret",source.url)}))
        .await;
    assert_eq!(duplicate["code"], 2);
    assert!(
        duplicate["error"]
            .as_str()
            .unwrap()
            .contains("already exists")
    );
    daemon.stop().await;
    let hits = source.hits.load(Ordering::SeqCst);
    drop(source);
    let mut daemon = Daemon::start_config(&root.0).await;
    daemon.state(id, "seeding").await;
    assert_eq!(
        daemon.call(json!({"op":"files","id":id})).await["files"][0]["path"],
        "x"
    );
    assert_eq!(hits, 3);
    daemon.stop().await;
}

#[tokio::test]
async fn url_failures_leave_catalog_empty_and_do_not_expose_query_secrets() {
    let root = Temp::new();
    let source = Source::new().await;
    let mut daemon = Daemon::start(&root.0, "", None).await;
    for path in [
        "/missing?key=secret",
        "/invalid",
        "/large",
        "/stream",
        "/loop",
        "/bad-redirect",
    ] {
        let response = daemon
            .raw(json!({"op":"add","url":format!("{}{path}",source.url)}))
            .await;
        assert_ne!(response["code"], 0, "{path}: {response}");
        assert!(
            !response["error"].as_str().unwrap().contains("secret"),
            "{response}"
        );
    }
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/download?key=secret",
        closed.local_addr().unwrap()
    );
    drop(closed);
    let response = daemon.raw(json!({"op":"add","url":url})).await;
    assert_eq!(response["code"], 1);
    assert!(!response["error"].as_str().unwrap().contains("secret"));
    let before = source.hits.load(Ordering::SeqCst);
    for fields in [
        json!({}),
        json!({"url":"file:///tmp/x.torrent"}),
        json!({"url":"ftp://example.org/x"}),
        json!({"url":format!("{}/download?key=secret",source.url),"magnet":"magnet:?"}),
        json!({"url":source.url,"metainfo":BASE64_STANDARD.encode(metainfo(None))}),
        json!({"url":source.url,"output_dir":"relative"}),
        json!({"url":source.url.replace("http://", "http://user:password@")}),
    ] {
        let mut request = fields;
        request["op"] = json!("add");
        assert_eq!(daemon.raw(request).await["code"], 2);
    }
    assert_eq!(source.hits.load(Ordering::SeqCst), before);
    assert!(
        daemon.call(json!({"op":"list"})).await["torrents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    daemon.stop().await;
}

#[tokio::test]
async fn web_url_add_requires_auth_and_applies_private_torrent_policy() {
    let root = Temp::new();
    let source = Source::new().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut daemon = Daemon::start(&root.0, "dht=true\npex=true", Some(address)).await;
    let index = http(address, "GET", "/", None, b"").await;
    assert!(index.contains("Magnet link or torrent URL"));
    let token = index
        .split("const token='")
        .nth(1)
        .unwrap()
        .split('\'')
        .next()
        .unwrap();
    let body = json!({"op":"add","url":format!("{}/private",source.url)}).to_string();
    let denied = http(address, "POST", "/api", None, body.as_bytes()).await;
    assert!(denied.starts_with("HTTP/1.1 403"));
    assert_eq!(source.hits.load(Ordering::SeqCst), 0);
    let response = http(address, "POST", "/api", Some(token), body.as_bytes()).await;
    let added: Value = serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(added["code"], 0, "{added}");
    let row = daemon
        .call(json!({"op":"get","id":added["data"]["id"]}))
        .await;
    assert_eq!(row["private"], true);
    assert_eq!(row["settings"]["dht"], false);
    assert_eq!(row["settings"]["pex"], false);
    daemon.stop().await;
}

#[tokio::test]
async fn url_body_deadline_applies_even_while_data_keeps_arriving() {
    let root = Temp::new();
    let mut daemon = Daemon::start(&root.0, "", None).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/download?key=secret",
        listener.local_addr().unwrap()
    );
    let client = Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .args(["add", &url])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (mut connection, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let source = tokio::spawn(async move {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(connection.read_u8().await.unwrap());
        }
        connection
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n")
            .await
            .unwrap();
        loop {
            if connection.write_all(b"x").await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let result = tokio::time::timeout(Duration::from_secs(35), client.wait_with_output())
        .await
        .expect("URL deadline")
        .unwrap();
    source.abort();
    assert_eq!(result.status.code(), Some(1));
    let error = String::from_utf8(result.stderr).unwrap();
    assert!(error.contains("torrent URL download timed out"), "{error}");
    assert!(!error.contains("secret"));
    assert!(
        daemon.call(json!({"op":"list"})).await["torrents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    daemon.stop().await;
}

#[tokio::test]
async fn slow_url_fetch_does_not_block_control_and_is_cancelled_on_shutdown() {
    let root = Temp::new();
    let mut daemon = Daemon::start(&root.0, "", None).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/slow", listener.local_addr().unwrap());
    let client = Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .args(["add", &url])
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (mut connection, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        request.push(connection.read_u8().await.unwrap());
    }
    tokio::time::timeout(Duration::from_secs(1), daemon.call(json!({"op":"status"})))
        .await
        .expect("status blocked by URL fetch");
    daemon.stop().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), connection.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    let result = client.wait_with_output().await.unwrap();
    assert!(!result.status.success());
}
