use base64::{Engine, prelude::BASE64_STANDARD};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, UnixStream},
    process::{Child, Command},
    task::JoinHandle,
};
pub const BIN: &str = env!("CARGO_BIN_EXE_spill");
pub struct Temp(pub PathBuf);
impl Temp {
    pub fn new() -> Self {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "spill-e2e-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub struct Daemon {
    pub child: Child,
    pub socket: PathBuf,
}
impl Daemon {
    pub async fn start(root: &Path, defaults: &str, web: Option<std::net::SocketAddr>) -> Self {
        Self::start_with_policy(root, defaults, web, true).await
    }
    pub async fn start_with_policy(
        root: &Path,
        defaults: &str,
        web: Option<std::net::SocketAddr>,
        allow_local_network: bool,
    ) -> Self {
        Self::start_with_caps(root, defaults, web, allow_local_network, 1, 1).await
    }
    pub async fn start_with_caps(
        root: &Path,
        defaults: &str,
        web: Option<std::net::SocketAddr>,
        allow_local_network: bool,
        active_downloads: usize,
        active_seeds: usize,
    ) -> Self {
        let socket = root.join("control.sock");
        let config = root.join("config.toml");
        let defaults = format!("trackers=false\ndht=false\npex=false\n{defaults}");
        let mut settings = std::collections::BTreeMap::new();
        for line in defaults.lines() {
            let (key, value) = line.split_once('=').unwrap();
            settings.insert(key, value);
        }
        let defaults = settings
            .into_iter()
            .map(|(key, value)| format!("{key}={value}\n"))
            .collect::<String>();
        std::fs::write(&config,format!("state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"127.0.0.1:0\"\nport_mapping=false\nallow_local_network={allow_local_network}\npeers=4\ncache_bytes=65536\nwrite_queue_bytes=32768\nactive_downloads={active_downloads}\nactive_seeds={active_seeds}\ndht_bootstrap=[]\n{}\n[defaults]\n{defaults}\n",root.join("state"),root.join("downloads"),socket,web.map(|a|format!("web_listen=\"{a}\"\n")).unwrap_or_default())).unwrap();
        Self::start_config(root).await
    }
    pub async fn start_config(root: &Path) -> Self {
        let socket = root.join("control.sock");
        let config = root.join("config.toml");
        let child = Command::new(BIN)
            .arg("serve")
            .arg("--config")
            .arg(config)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut daemon = Self { child, socket };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = daemon.child.try_wait().unwrap() {
                    panic!("daemon exited: {status}");
                }
                if UnixStream::connect(&daemon.socket).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("daemon readiness");
        // A bound control socket can precede the HTTP listener's startup.
        daemon.call(json!({"op":"health"})).await;
        daemon
    }
    pub async fn raw(&self, request: Value) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut stream = UnixStream::connect(&self.socket).await.unwrap();
            let mut bytes = serde_json::to_vec(&request).unwrap();
            bytes.push(b'\n');
            stream.write_all(&bytes).await.unwrap();
            let mut bytes = Vec::new();
            BufReader::new(stream)
                .read_until(b'\n', &mut bytes)
                .await
                .unwrap();
            serde_json::from_slice(&bytes).unwrap()
        })
        .await
        .expect("control deadline")
    }
    pub async fn call(&self, request: Value) -> Value {
        let response = self.raw(request).await;
        assert_eq!(response["code"], 0, "{response}");
        response["data"].clone()
    }
    pub async fn add(&self, bytes: &[u8], output: Option<&Path>) -> String {
        self.call(json!({"op":"add","metainfo":BASE64_STANDARD.encode(bytes),"magnet":null,"output_dir":output}))
            .await["id"]
            .as_str()
            .unwrap()
            .into()
    }
    pub async fn state(&self, id: &str, state: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = self.call(json!({"op":"get","id":id})).await;
                assert!(row["last_error"].is_null(), "{row}");
                if row["state"] == state {
                    return row;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("waiting for {state}"))
    }
    pub async fn failed(&self, id: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = self.call(json!({"op":"get","id":id})).await;
                if row["last_error"].is_string() {
                    return row;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("waiting for torrent failure")
    }
    pub async fn stop(&mut self) {
        self.call(json!({"op":"shutdown"})).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(4), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}
pub async fn peer_frame(peer: &mut tokio::net::TcpStream) -> Vec<u8> {
    let length = peer.read_u32().await.unwrap();
    let mut bytes = length.to_be_bytes().to_vec();
    bytes.resize(4 + length as usize, 0);
    peer.read_exact(&mut bytes[4..]).await.unwrap();
    bytes
}

pub async fn raw_peer(address: &str, hash: [u8; 20], identity: u8) -> tokio::net::TcpStream {
    let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
    peer.write_all(
        &spill::protocol::peer::Handshake {
            reserved: [0; 8],
            info_hash: hash,
            peer_id: [identity; 20],
        }
        .encode(),
    )
    .await
    .unwrap();
    let mut hello = [0; 68];
    peer.read_exact(&mut hello).await.unwrap();
    peer
}

pub fn seed_metainfo(name: &str, payload: &[u8], tracker: Option<&str>) -> Vec<u8> {
    let mut bytes = b"d".to_vec();
    if let Some(url) = tracker {
        bytes.extend_from_slice(format!("8:announce{}:{url}", url.len()).as_bytes());
    }
    bytes.extend_from_slice(
        format!(
            "4:infod6:lengthi{}e4:name{}:{name}12:piece lengthi{}e6:pieces20:",
            payload.len(),
            name.len(),
            payload.len()
        )
        .as_bytes(),
    );
    bytes.extend_from_slice(&Sha1::digest(payload));
    bytes.extend_from_slice(b"ee");
    bytes
}

pub struct WebSeed {
    pub url: String,
    pub hits: Arc<AtomicUsize>,
    pub requests: Arc<Mutex<Vec<(String, usize, usize)>>>,
    task: JoinHandle<()>,
}
impl Drop for WebSeed {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl WebSeed {
    pub async fn new() -> Self {
        let mut seed = Self::files(&[("/payload", b"abc")], false).await;
        seed.url.push_str("payload");
        seed
    }

    // The first response can carry incorrect bytes with otherwise valid HTTP
    // framing, so recovery must pass through piece hashing rather than HTTP retry.
    pub async fn files(files: &[(&str, &[u8])], corrupt_first: bool) -> Self {
        let files: Vec<_> = files
            .iter()
            .map(|(p, b)| (p.to_string(), b.to_vec()))
            .collect();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let hits = Arc::new(AtomicUsize::new(0));
        let count = hits.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    bytes.push(stream.read_u8().await.unwrap());
                }
                let request = String::from_utf8(bytes).unwrap();
                let path = request.split_whitespace().nth(1).unwrap();
                let range = request
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("range").then_some(value.trim())
                    })
                    .unwrap()
                    .strip_prefix("bytes=")
                    .unwrap();
                let (start, end) = range.split_once('-').unwrap();
                let (start, end): (usize, usize) = (start.parse().unwrap(), end.parse().unwrap());
                log.lock().unwrap().push((path.to_owned(), start, end));
                let (_, content) = files
                    .iter()
                    .find(|(p, _)| p == path)
                    .expect("unexpected web-seed path");
                let mut body = content[start..=end].to_vec();
                if count.fetch_add(1, Ordering::Relaxed) == 0 && corrupt_first {
                    body[0] ^= 0xff;
                }
                let header = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nConnection: close\r\n\r\n",
                    body.len(),
                    content.len()
                );
                let _ = stream.write_all(header.as_bytes()).await;
                let _ = stream.write_all(&body).await;
            }
        });
        Self {
            url,
            hits,
            requests,
            task,
        }
    }
}
pub fn metainfo(web: Option<&str>) -> Vec<u8> {
    let mut bytes = b"d".to_vec();
    if let Some(url) = web {
        bytes.extend_from_slice(format!("8:url-list{}:{url}", url.len()).as_bytes());
    }
    bytes.extend_from_slice(b"4:infod6:lengthi3e4:name1:x12:piece lengthi4e6:pieces20:");
    bytes.extend_from_slice(&Sha1::digest(b"abc"));
    bytes.extend_from_slice(b"ee");
    bytes
}
// Tiny multi-file torrents with four-byte pieces, including cross-file pieces.
// Padding participates in hashes but must never be requested or materialized.
pub fn multifile_metainfo(web: &str, files: &[(&str, &[u8], bool)]) -> Vec<u8> {
    let mut bytes = format!("d8:url-list{}:{web}4:infod5:filesl", web.len()).into_bytes();
    let mut content = Vec::new();
    for (name, data, padding) in files {
        bytes.push(b'd');
        if *padding {
            bytes.extend_from_slice(b"4:attr1:p");
        }
        bytes.extend_from_slice(
            format!("6:lengthi{}e4:pathl{}:{name}ee", data.len(), name.len()).as_bytes(),
        );
        content.extend_from_slice(data);
    }
    bytes.extend_from_slice(
        format!(
            "e4:name4:test12:piece lengthi4e6:pieces{}:",
            content.len().div_ceil(4) * 20
        )
        .as_bytes(),
    );
    for piece in content.chunks(4) {
        bytes.extend_from_slice(&Sha1::digest(piece));
    }
    bytes.extend_from_slice(b"ee");
    bytes
}

pub async fn http(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &[u8],
) -> String {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let token = token
        .map(|t| format!("X-Spill-Token: {t}\r\n"))
        .unwrap_or_default();
    let content_type = if path.starts_with("/api/upload") {
        "application/x-bittorrent"
    } else {
        "application/json"
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\n{token}Content-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    String::from_utf8(bytes).unwrap()
}
