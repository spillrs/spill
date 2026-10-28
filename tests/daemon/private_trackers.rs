use crate::common::{Daemon, Temp, WebSeed};
use serde_json::json;
use sha1::Digest;
use std::time::Duration;

#[tokio::test]
async fn private_torrent_isolates_discovery_and_orders_tracker_events() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let root = Temp::new();
    let source = WebSeed::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let announce = format!(
        "http://{}/announce?passkey=secret",
        listener.local_addr().unwrap()
    );
    let (sent, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let gate = release.clone();
    let tracker = tokio::spawn(async move {
        let mut first = true;
        let mut fail_completion = true;
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            let query: std::collections::HashMap<String, String> = line
                .split_whitespace()
                .nth(1)
                .unwrap()
                .split_once('?')
                .unwrap()
                .1
                .split('&')
                .map(|pair| {
                    let (key, value) = pair.split_once('=').unwrap();
                    (key.to_owned(), value.to_owned())
                })
                .collect();
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                headers.push_str(&line);
            }
            assert!(headers.to_ascii_lowercase().contains("user-agent: spill/"));
            let drop_reply =
                query.get("event").is_some_and(|event| event == "completed") && fail_completion;
            sent.send(query).unwrap();
            if drop_reply {
                fail_completion = false;
                continue;
            }
            if first {
                first = false;
                gate.notified().await;
            }
            let body = b"d8:intervali30e12:min intervali120e5:peers0:10:tracker id3:tage";
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(body).await.unwrap();
        }
    });
    let mut bytes = format!("d8:announce{}:{}8:url-list{}:{}4:infod6:lengthi3e4:name1:x12:piece lengthi16384e6:pieces20:", announce.len(), announce, source.url.len(), source.url).into_bytes();
    bytes.extend_from_slice(&sha1::Sha1::digest(b"abc"));
    bytes.extend_from_slice(b"7:privatei1eee");
    let mut daemon = Daemon::start(
        &root.0,
        "web_seeds=true\ntrackers=true\ndht=true\npex=true",
        None,
    )
    .await;
    let id = daemon.add(&bytes, None).await;
    let started = tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started["event"], "started");
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["settings"]["dht"], false);
    assert_eq!(row["settings"]["pex"], false);
    for setting in ["dht", "pex"] {
        let denied = daemon
            .raw(json!({"op":"set","id":id,"values":{setting:true}}))
            .await;
        assert_ne!(denied["code"], 0);
    }
    let meta = spill::engine::profile::Profile::DEFAULT
        .parse_owned(&bytes)
        .unwrap();
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut reserved = [0; 8];
    reserved[5] = 0x10;
    reserved[7] = 1;
    peer.write_all(
        &spill::protocol::peer::Handshake {
            reserved,
            info_hash: *meta.info_hash().as_bytes(),
            peer_id: [42; 20],
        }
        .encode(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut hello = [0; 68];
        peer.read_exact(&mut hello).await.unwrap();
        assert_eq!(hello[27] & 1, 0);
        assert_eq!(&hello[48..56], b"-sP0510-");
        loop {
            let len = peer.read_u32().await.unwrap();
            if len == 0 {
                continue;
            }
            let mut frame = vec![0; len as usize];
            peer.read_exact(&mut frame).await.unwrap();
            if frame.starts_with(&[20, 0]) {
                let hello =
                    spill::protocol::bencode::decode(&frame[2..], Default::default()).unwrap();
                assert!(
                    hello
                        .get(b"m")
                        .unwrap()
                        .get(b"ut_pex")
                        .is_none_or(|v| matches!(
                            v.value(),
                            spill::protocol::bencode::Value::Integer(0)
                        ))
                );
                break;
            }
        }
    })
    .await
    .unwrap();
    release.notify_one();
    let completed = tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed["event"], "completed");
    assert_eq!(completed["trackerid"], "tag");
    assert_eq!(completed["left"], "0");
    assert_eq!(completed["downloaded"], "3");
    assert_eq!(completed["uploaded"], "0");
    peer.write_all(&[0, 0, 0, 1, 2]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let len = peer.read_u32().await.unwrap();
            let mut frame = vec![0; len as usize];
            peer.read_exact(&mut frame).await.unwrap();
            if frame == [1] {
                break;
            }
        }
        let mut request = vec![0, 0, 0, 13, 6];
        request.extend_from_slice(&[0; 8]);
        request.extend_from_slice(&3u32.to_be_bytes());
        peer.write_all(&request).await.unwrap();
        loop {
            let len = peer.read_u32().await.unwrap();
            let mut frame = vec![0; len as usize];
            peer.read_exact(&mut frame).await.unwrap();
            if frame.first() == Some(&7) {
                assert_eq!(&frame[9..], b"abc");
                break;
            }
        }
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["transfer"]["uploaded_bytes"] == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    daemon.stop().await;
    let stopped = requests.recv().await.unwrap();
    assert_eq!(stopped["event"], "stopped");
    assert_eq!(stopped["trackerid"], "tag");
    assert_eq!(stopped["uploaded"], "3");
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    daemon.state(&id, "seeding").await;
    let restarted = tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restarted["event"], "started");
    let retried = tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried["event"], "completed");
    assert_eq!(retried["downloaded"], "0");
    assert_eq!(retried["uploaded"], "0");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["completed_announce"] == true
                && row["transfer"]["trackers"][0]["next_announce_ms"]
                    .as_u64()
                    .is_some_and(|ms| ms > 110_000)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    daemon.call(json!({"op":"reannounce","id":id})).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), requests.recv())
            .await
            .is_err()
    );
    daemon.stop().await;
    assert_eq!(requests.recv().await.unwrap()["event"], "stopped");
    // Acknowledged completion must not be replayed by another fresh session.
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    daemon.state(&id, "seeding").await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), requests.recv())
            .await
            .unwrap()
            .unwrap()["event"],
        "started"
    );
    daemon.stop().await;
    assert_eq!(requests.recv().await.unwrap()["event"], "stopped");
    tracker.abort();
}

#[tokio::test]
async fn adopted_private_seed_never_announces_completed() {
    use spill::protocol::{compact::PeersV4, tracker::udp};
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let announce = format!("udp://{}/announce", socket.local_addr().unwrap());
    let (sent, mut events) = tokio::sync::mpsc::unbounded_channel();
    let tracker = tokio::spawn(async move {
        let codec = udp::Limits {
            max_packet_len: 65507,
            max_peers: 200,
        };
        let mut bytes = [0; 4096];
        loop {
            let (len, source) = socket.recv_from(&mut bytes).await.unwrap();
            let request = udp::Request::decode(&bytes[..len], codec).unwrap();
            let kind = match request.kind {
                udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                    connection_id: 17,
                    extensions: &[],
                },
                udp::RequestKind::Announce { announce, .. } => {
                    sent.send(announce).unwrap();
                    udp::ResponseKind::Announce {
                        interval: 30,
                        seeders: 1,
                        leechers: 0,
                        peers: udp::Peers::V4(PeersV4::parse(&[], 0).unwrap()),
                    }
                }
                _ => panic!("unexpected tracker request"),
            };
            let reply = udp::Response {
                transaction_id: request.transaction_id,
                kind,
            }
            .encode(codec)
            .unwrap();
            socket.send_to(&reply, source).await.unwrap();
        }
    });
    let mut bytes = format!(
        "d8:announce{}:{}4:infod6:lengthi3e4:name1:x12:piece lengthi4e6:pieces20:",
        announce.len(),
        announce
    )
    .into_bytes();
    bytes.extend_from_slice(&sha1::Sha1::digest(b"abc"));
    bytes.extend_from_slice(b"7:privatei1eee");
    let mut daemon = Daemon::start(&root.0, "trackers=true\nweb_seeds=false", None).await;
    let id = daemon.add(&bytes, None).await;
    for attempt in 0..2 {
        let row = daemon.state(&id, "seeding").await;
        assert!(row["completed_announce"].is_null(), "{row}");
        let started = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(started.event, udp::EVENT_STARTED);
        assert_eq!(
            (started.downloaded, started.uploaded, started.left),
            (0, 0, 0)
        );
        daemon.call(json!({"op":"pause","id":id})).await;
        let stopped = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stopped.event, udp::EVENT_STOPPED);
        assert_eq!(
            (stopped.downloaded, stopped.uploaded, stopped.left),
            (0, 0, 0)
        );
        if attempt == 0 {
            daemon.call(json!({"op":"resume","id":id})).await;
        }
    }
    daemon.stop().await;
    tracker.abort();
}

#[tokio::test]
async fn private_tracker_failure_and_warning_reach_status() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let root = Temp::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let announce = format!("http://{}/announce", listener.local_addr().unwrap());
    let tracker = tokio::spawn(async move {
        let mut failed = false;
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            let body: &[u8] = if failed {
                b"d8:intervali30e5:peers0:15:warning message9:Low ratioe"
            } else {
                failed = true;
                b"d14:failure reason15:Passkey invalide"
            };
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(body).await.unwrap();
        }
    });
    let mut bytes = format!(
        "d8:announce{}:{}4:infod6:lengthi3e4:name1:x12:piece lengthi16384e6:pieces20:",
        announce.len(),
        announce
    )
    .into_bytes();
    bytes.extend_from_slice(&sha1::Sha1::digest(b"abc"));
    bytes.extend_from_slice(b"7:privatei1eee");
    let mut daemon = Daemon::start(&root.0, "web_seeds=false\ntrackers=true", None).await;
    let id = daemon.add(&bytes, None).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["last_error"] == "Passkey invalid" {
                assert_eq!(row["transfer"]["trackers"][0]["error"], "Passkey invalid");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    daemon.call(json!({"op":"pause","id":id})).await;
    daemon.call(json!({"op":"resume","id":id})).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["tracker_message"] == "Low ratio" {
                assert!(row["last_error"].is_null(), "{row}");
                assert_eq!(row["transfer"]["trackers"][0]["warning"], "Low ratio");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    daemon.stop().await;
    tracker.abort();
}
