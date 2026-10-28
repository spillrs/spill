use crate::common::{Daemon, Temp, peer_frame, raw_peer, seed_metainfo};
use serde_json::json;
use sha1::Digest;
use std::time::Duration;

#[tokio::test]
async fn tracker_dials_do_not_duplicate_an_inbound_peer_identity() {
    use spill::protocol::{
        compact::PeersV4,
        peer::{Handshake, message::Message},
        tracker::udp,
    };
    use std::sync::Arc;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, UdpSocket},
        sync::Notify,
    };
    for identity in [0, 255] {
        let root = Temp::new();
        std::fs::create_dir_all(root.0.join("downloads")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port().to_be_bytes();
        let contacts = [127, 0, 0, 1, port[0], port[1]];
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = format!("udp://{}/announce", socket.local_addr().unwrap());
        let release = Arc::new(Notify::new());
        let announced = release.clone();
        let tracker = tokio::spawn(async move {
            let codec = udp::Limits {
                max_packet_len: 65507,
                max_peers: 200,
            };
            let mut bytes = [0; 4096];
            let mut started = false;
            loop {
                let (len, source) = socket.recv_from(&mut bytes).await.unwrap();
                let request = udp::Request::decode(&bytes[..len], codec).unwrap();
                let kind = match request.kind {
                    udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                        connection_id: 17,
                        extensions: &[],
                    },
                    udp::RequestKind::Announce { .. } => {
                        if !started {
                            announced.notified().await;
                            started = true;
                        }
                        udp::ResponseKind::Announce {
                            interval: 30,
                            seeders: 0,
                            leechers: 1,
                            peers: udp::Peers::V4(PeersV4::parse(&contacts, 1).unwrap()),
                        }
                    }
                    _ => panic!("unexpected request"),
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
            "d8:announce{}:{url}4:infod6:lengthi6e4:name1:x12:piece lengthi3e6:pieces40:",
            url.len()
        )
        .into_bytes();
        bytes.extend_from_slice(&sha1::Sha1::digest(b"abc"));
        bytes.extend_from_slice(&sha1::Sha1::digest(b"def"));
        bytes.extend_from_slice(b"ee");
        let hash = *spill::engine::profile::Profile::DEFAULT
            .parse_owned(&bytes)
            .unwrap()
            .info_hash()
            .as_bytes();
        let mut daemon = Daemon::start(&root.0, "trackers=true\nweb_seeds=false", None).await;
        let id = daemon.add(&bytes, None).await;
        daemon.state(&id, "downloading").await;
        let address = daemon.call(json!({"op":"health"})).await["listen"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut inbound = raw_peer(&address, hash, identity).await;
        release.notify_one();
        let (mut outbound, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut hello = [0; 68];
        outbound.read_exact(&mut hello).await.unwrap();
        outbound
            .write_all(
                &Handshake {
                    reserved: [0; 8],
                    info_hash: hash,
                    peer_id: [identity; 20],
                }
                .encode(),
            )
            .await
            .unwrap();
        let expected = if identity == 0 { "inbound" } else { "outbound" };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = daemon.call(json!({"op":"get", "id":id})).await;
                let peers = row["transfer"]["peers"].as_array().unwrap();
                let live: Vec<_> = peers
                    .iter()
                    .filter(|p| {
                        p["detail"]["established"] == true && p["detail"]["retiring"] == false
                    })
                    .collect();
                if live.len() == 1
                    && live[0]["direction"] == expected
                    && row["transfer"]["active_peers"] == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("duplicate peer ID must leave one connection in the deterministic direction");
        let (survivor, rejected) = if identity == 0 {
            (&mut inbound, &mut outbound)
        } else {
            (&mut outbound, &mut inbound)
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut discarded = Vec::new();
            rejected.read_to_end(&mut discarded).await.unwrap();
            survivor
                .write_all(&[0, 0, 0, 2, 5, 0xc0, 0, 0, 0, 1, 1])
                .await
                .unwrap();
            for _ in 0..2 {
                loop {
                    let frame = peer_frame(survivor).await;
                    if let Message::Request(request) = Message::decode(&frame, 16384).unwrap().0 {
                        let block = if request.index == 0 { b"abc" } else { b"def" };
                        let message = Message::Piece {
                            index: request.index,
                            begin: 0,
                            block,
                        };
                        let mut bytes = vec![0; message.encoded_len(16384).unwrap()];
                        message.encode(&mut bytes, 16384).unwrap();
                        survivor.write_all(&bytes).await.unwrap();
                        break;
                    }
                }
            }
        })
        .await
        .expect("retained connection must still transfer payload");
        daemon.state(&id, "seeding").await;
        daemon.stop().await;
        tracker.abort();
    }
}

#[tokio::test]
async fn inbound_handshake_bursts_fill_available_peer_slots() {
    use spill::protocol::peer::Handshake;
    use std::sync::Arc;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpSocket,
        sync::Barrier,
    };
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    let bytes = seed_metainfo("x", b"abc", None);
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&bytes)
        .unwrap()
        .info_hash()
        .as_bytes();
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    // macOS only configures 127.0.0.1; two at a time respect the per-IP cap.
    #[cfg(target_os = "macos")]
    const BURST: u8 = 2;
    #[cfg(not(target_os = "macos"))]
    const BURST: u8 = 4;
    for _ in 0..5 {
        let mut peers = Vec::new();
        for wave in 0..4 / BURST {
            let barrier = Arc::new(Barrier::new(usize::from(BURST) + 1));
            let mut connections = tokio::task::JoinSet::new();
            for identity in wave * BURST + 1..=(wave + 1) * BURST {
                let barrier = barrier.clone();
                connections.spawn(async move {
                    let socket = TcpSocket::new_v4().unwrap();
                    #[cfg(target_os = "macos")]
                    let source = "127.0.0.1:0".to_owned();
                    #[cfg(not(target_os = "macos"))]
                    let source = format!("127.0.0.{}:0", identity + 1);
                    socket.bind(source.parse().unwrap()).unwrap();
                    let mut peer = socket.connect(address).await.unwrap();
                    barrier.wait().await;
                    peer.write_all(
                        &Handshake {
                            reserved: [0; 8],
                            info_hash: hash,
                            peer_id: [identity; 20],
                        }
                        .encode(),
                    )
                    .await
                    .unwrap();
                    let mut hello = [0; 68];
                    peer.read_exact(&mut hello)
                        .await
                        .expect("burst must not drop an admitted inbound handshake");
                    peer
                });
            }
            barrier.wait().await;
            tokio::time::timeout(Duration::from_secs(3), async {
                while let Some(peer) = connections.join_next().await {
                    peers.push(peer.unwrap());
                }
            })
            .await
            .unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let row = daemon.call(json!({"op":"get", "id":id})).await;
                    if row["transfer"]["peers"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|p| p["detail"]["established"] == true)
                        .count()
                        == usize::from((wave + 1) * BURST)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("all available peer slots must attach");
        }
        drop(peers);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let row = daemon.call(json!({"op":"get", "id":id})).await;
                if row["transfer"]["active_peers"] == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    daemon.stop().await;
}
