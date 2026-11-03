use crate::{
    common::{BIN, Daemon, http, peer_frame, raw_peer},
    payload::Temp,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use serde_json::{Value, json};
use spill::{
    engine::profile::Profile,
    protocol::{
        bencode::{self, Node, Value as BValue},
        metainfo::OwnedMetainfo,
    },
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::Command,
};

const V2: &[u8] = include_bytes!("../protocol/fixtures/v2.torrent");
const HYBRID: &[u8] = include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent");
const LENGTHS: &[usize] = &[16384, 0, 65659, 1, 32768, 123];

fn payload(begin: usize, length: usize) -> Vec<u8> {
    (begin..begin + length).map(|i| (i % 251) as u8).collect()
}
fn tracked(fixture: &[u8], address: SocketAddr) -> OwnedMetainfo {
    let doc = bencode::decode(fixture, Default::default()).unwrap();
    let BValue::Dictionary(fields) = doc.value() else {
        unreachable!()
    };
    let mut fields = fields.clone();
    let url = format!("http://{address}/announce");
    fields.insert(b"announce", Node::new(BValue::Bytes(url.as_bytes())));
    let bytes =
        bencode::encode(&Node::new(BValue::Dictionary(fields)), Default::default()).unwrap();
    let meta = Profile::default().parse_owned(&bytes).unwrap();
    assert_eq!(
        meta.info_hash(),
        Profile::default().parse_owned(fixture).unwrap().info_hash()
    );
    meta
}
fn decode(value: &str) -> Vec<u8> {
    let mut bytes = value.bytes();
    let mut out = Vec::new();
    while let Some(byte) = bytes.next() {
        out.push(if byte == b'%' {
            let hex = [bytes.next().unwrap(), bytes.next().unwrap()];
            u8::from_str_radix(std::str::from_utf8(&hex).unwrap(), 16).unwrap()
        } else {
            byte
        });
    }
    out
}
async fn send(socket: &mut TcpStream, id: u8, body: &[u8]) {
    socket
        .write_all(&((body.len() + 1) as u32).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(&[id]).await.unwrap();
    socket.write_all(body).await.unwrap();
}
async fn serve(mut socket: TcpStream, lengths: &[usize], bits: u8) {
    let pieces: Vec<_> = lengths
        .iter()
        .flat_map(|&length| {
            (0..length)
                .step_by(32768)
                .map(move |begin| payload(begin, (length - begin).min(32768)))
        })
        .collect();
    send(&mut socket, 5, &[bits]).await;
    send(&mut socket, 1, &[]).await;
    while let Ok(length) = socket.read_u32().await {
        assert!(length < 1024 * 1024);
        let mut body = vec![0; length as usize];
        if socket.read_exact(&mut body).await.is_err() {
            break;
        }
        if body.first() != Some(&6) {
            continue;
        }
        let index = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
        let begin = u32::from_be_bytes(body[5..9].try_into().unwrap()) as usize;
        let length = u32::from_be_bytes(body[9..13].try_into().unwrap()) as usize;
        let reply = [&body[1..9], &pieces[index][begin..begin + length]].concat();
        send(&mut socket, 7, &reply).await;
    }
}

#[tokio::test]
async fn metainfo_downloads_resume_recheck_seed_and_remove_through_full_hash_aliases() {
    tokio::time::timeout(Duration::from_secs(50), async {
        for fixture in [V2, HYBRID] {
            let tracker = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let meta = tracked(fixture, tracker.local_addr().unwrap());
            let identity = meta.info_hash();
            let v1 = identity.v1().map(|hash| hash.to_string());
            let v2 = identity.v2().unwrap().to_string();
            let v2_key = identity.v2().unwrap().wire_key();
            let keys: Vec<_> = identity
                .v1()
                .map(|hash| *hash.as_bytes())
                .into_iter()
                .chain(Some(v2_key))
                .collect();
            let lengths = if v1.is_some() { LENGTHS } else { &LENGTHS[..5] };
            let events = Arc::new(Mutex::new(Vec::new()));
            let recorded = events.clone();
            let tracker = tokio::spawn(async move {
                loop {
                    let (socket, _) = tracker.accept().await.unwrap();
                    let mut socket = BufReader::new(socket);
                    let mut line = String::new();
                    socket.read_line(&mut line).await.unwrap();
                    let query = line
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .split_once('?')
                        .unwrap()
                        .1;
                    let fields: BTreeMap<_, _> = query
                        .split('&')
                        .map(|pair| pair.split_once('=').unwrap())
                        .collect();
                    let key: [u8; 20] = decode(fields["info_hash"]).try_into().unwrap();
                    recorded
                        .lock()
                        .unwrap()
                        .push((key, fields.get("event").unwrap_or(&"none").to_string()));
                    loop {
                        line.clear();
                        socket.read_line(&mut line).await.unwrap();
                        if line == "\r\n" {
                            break;
                        }
                    }
                    let body = b"d8:intervali60e5:peers0:e";
                    let header =
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
                    socket.write_all(header.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                }
            });
            let root = Temp::new();
            let mut daemon =
                Daemon::start(&root.0, "trackers=true\ndht=true\nweb_seeds=false", None).await;
            let id = daemon.add(meta.source_bytes(), None).await;
            assert_eq!(id, identity.to_string());
            let listen = daemon.call(json!({"op":"health"})).await["listen"]
                .as_str()
                .unwrap()
                .to_string();
            let mut sources = Vec::new();
            for (index, &key) in keys.iter().enumerate() {
                let bits = if keys.len() == 1 {
                    0xfc
                } else if index == 0 {
                    0x70
                } else {
                    0x8e
                };
                let socket = raw_peer(&listen, key, 91 + index as u8).await;
                sources.push(tokio::spawn(serve(socket, lengths, bits)));
            }
            let row = daemon.state(&v2, "seeding").await;
            assert_eq!(row["id"], id);
            assert_eq!(row["v1"], json!(v1));
            assert_eq!(row["v2"], v2);
            assert_eq!(row["format"], if v1.is_some() { "hybrid" } else { "v2" });
            assert_eq!(row["metadata_ready"], true);
            assert_eq!(
                row["transfer"]["trackers"].as_array().unwrap().len(),
                keys.len()
            );
            assert_eq!(row["transfer"]["dht"].as_array().unwrap().len(), keys.len());
            for (name, &length) in ["block", "empty", "odd", "one", "piece", "tail"]
                .iter()
                .zip(lengths)
            {
                assert_eq!(
                    std::fs::read(root.0.join("downloads/vectors").join(name)).unwrap(),
                    payload(0, length)
                );
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let row = daemon.call(json!({"op":"get","id":v2})).await;
                    let delivered = row["completed_announces"].as_array().unwrap();
                    if delivered.len() == keys.len()
                        && delivered.iter().all(|entry| entry["delivered"] == true)
                    {
                        for key in &keys {
                            let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
                            assert!(delivered.iter().any(|entry| entry["swarm"] == hex));
                            assert!(
                                row["transfer"]["trackers"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .any(|entry| entry["swarm"] == hex)
                            );
                            assert!(
                                row["transfer"]["dht"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .any(|entry| entry["swarm"] == hex)
                            );
                        }
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("completion acknowledged for every swarm");
            daemon.call(json!({"op":"pause","id":v2})).await;
            for source in sources {
                source.await.unwrap();
            }
            daemon.stop().await;
            let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
            let saved: String = db
                .query_row("SELECT completed_announces FROM torrents", [], |r| r.get(0))
                .unwrap();
            let saved: Vec<([u8; 20], bool)> = serde_json::from_str(&saved).unwrap();
            for key in &keys {
                assert!(saved.contains(&(*key, true)));
            }
            drop(db);
            let mut daemon = Daemon::start_config(&root.0).await;
            assert_eq!(daemon.state(&v2, "paused").await["id"], id);
            daemon.call(json!({"op":"resume","id":v2})).await;
            daemon.state(&v2, "seeding").await;
            daemon.call(json!({"op":"reannounce","id":v2})).await;
            let listen = daemon.call(json!({"op":"health"})).await["listen"]
                .as_str()
                .unwrap()
                .to_string();
            let mut reader = raw_peer(&listen, identity.v2().unwrap().wire_key(), 95).await;
            send(&mut reader, 2, &[]).await;
            loop {
                if peer_frame(&mut reader).await[4] == 1 {
                    break;
                }
            }
            let request = [
                0u32.to_be_bytes(),
                0u32.to_be_bytes(),
                16384u32.to_be_bytes(),
            ]
            .concat();
            send(&mut reader, 6, &request).await;
            loop {
                let frame = peer_frame(&mut reader).await;
                if frame[4] == 7 {
                    assert_eq!(&frame[13..], payload(0, 16384));
                    break;
                }
            }
            daemon.call(json!({"op":"recheck","id":v2})).await;
            let row = daemon.state(&v2, "paused").await;
            assert_eq!(row["completed"], true);
            assert_eq!(
                row["completed_announces"].as_array().unwrap().len(),
                keys.len()
            );
            for key in &keys {
                assert_eq!(
                    events
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(k, e)| k == key && e == "completed")
                        .count(),
                    1
                );
            }
            assert_eq!(
                daemon
                    .call(json!({"op":"remove","id":v2,"delete_payload":true}))
                    .await["removed"],
                id
            );
            assert!(!root.0.join("downloads/vectors").exists());
            assert_eq!(
                daemon.call(json!({"op":"list"})).await["torrents"],
                json!([])
            );
            daemon.stop().await;
            tracker.abort();
        }
    })
    .await
    .expect("daemon v2/hybrid lifecycle with raw peers");
}

async fn client(daemon: &Daemon, args: &[&str]) -> Value {
    let result = Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .arg("--json")
        .args(args)
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
fn http_data(response: &str) -> Value {
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap()
}
#[tokio::test]
async fn cli_http_and_catalog_preserve_stable_ids_and_both_hybrid_aliases() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let root = Temp::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let web = listener.local_addr().unwrap();
        drop(listener);
        let mut daemon = Daemon::start(&root.0, "web_seeds=false", Some(web)).await;
        let index = http(web, "GET", "/", None, b"").await;
        let token = index
            .split("const token='")
            .nth(1)
            .unwrap()
            .split('\'')
            .next()
            .unwrap()
            .to_string();
        let details = http(web, "GET", "/details.js", None, b"").await;
        assert!(details.contains("SHA-256") && details.contains("completed_announces"));
        let mut ids = Vec::new();
        for fixture in [V2, HYBRID] {
            let meta = Profile::default().parse_owned(fixture).unwrap();
            let identity = meta.info_hash();
            let v2 = identity.v2().unwrap().to_string();
            let file = root.0.join(format!("{v2}.torrent"));
            std::fs::write(&file, fixture).unwrap();
            let added = if identity.v1().is_none() {
                client(&daemon, &["add", file.to_str().unwrap(), "--paused"]).await
            } else {
                let response = http(
                    web,
                    "POST",
                    "/api/upload?paused=true",
                    Some(&token),
                    fixture,
                )
                .await;
                let result = http_data(&response);
                assert_eq!(result["code"], 0);
                result["data"].clone()
            };
            let id = added["id"].as_str().unwrap().to_string();
            assert_eq!(id, identity.to_string());
            assert_eq!(id.len(), if identity.v1().is_some() { 40 } else { 64 });
            ids.push((id.clone(), v2.clone()));
            let row = client(&daemon, &["get", &v2.to_ascii_uppercase()]).await;
            assert_eq!(row["id"], id);
            assert_eq!(row["v2"], v2);
            assert_eq!(row["metadata_ready"], true);
            assert_eq!(client(&daemon, &["get", &v2[..16]]).await["id"], id);
            assert!(!root.0.join("downloads/vectors").exists());
            let request = json!({"op":"get","id":v2}).to_string();
            let response = http(web, "POST", "/api", Some(&token), request.as_bytes()).await;
            let read = http_data(&response);
            assert_eq!(read["data"]["id"], id);
            let add = json!({
                "op":"add", "metainfo":BASE64_STANDARD.encode(fixture), "paused":true
            });
            let duplicate = daemon.raw(add).await;
            assert_eq!(duplicate["code"], 2);
            assert!(
                duplicate["error"]
                    .as_str()
                    .unwrap()
                    .contains("already exists")
            );
            let topics = identity
                .v1()
                .map(|hash| format!("urn:btih:{hash}"))
                .into_iter()
                .chain(Some(format!("urn:btmh:1220{v2}")));
            for topic in topics {
                let output = root.0.join("duplicate");
                let add = json!({
                    "op":"add", "magnet":format!("magnet:?xt={topic}"), "output_dir":output
                });
                let duplicate = daemon.raw(add).await;
                assert_eq!(duplicate["code"], 2);
                assert!(
                    duplicate["error"]
                        .as_str()
                        .unwrap()
                        .contains("already exists")
                );
                assert!(!output.exists());
            }
        }
        let rows = client(&daemon, &["list"]).await;
        assert_eq!(rows["torrents"].as_array().unwrap().len(), 2);
        let listed = Command::new(BIN)
            .arg("--socket")
            .arg(&daemon.socket)
            .arg("list")
            .output()
            .await
            .unwrap();
        assert!(listed.status.success());
        let listed = String::from_utf8(listed.stdout).unwrap();
        for (id, _) in &ids {
            assert!(listed.contains(id));
        }
        daemon.stop().await;
        let mut daemon = Daemon::start_config(&root.0).await;
        for (id, v2) in ids {
            assert_eq!(daemon.state(&v2, "paused").await["id"], id);
            assert_eq!(client(&daemon, &["files", &v2]).await["id"], id);
            assert_eq!(client(&daemon, &["remove", &v2]).await["removed"], id);
        }
        daemon.stop().await;
    })
    .await
    .expect("full daemon IDs across CLI, HTTP and catalog restart");
}
