use crate::common::{Daemon, Temp, seed_metainfo};
use serde_json::json;
use sha1::Digest;
use std::time::Duration;

#[tokio::test]
async fn dht_and_udp_tracker_share_the_listen_port() {
    use spill::protocol::{compact::PeersV4, tracker::udp};
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", socket.local_addr().unwrap());
    let mut daemon = Daemon::start(&root.0, "trackers=true\ndht=true\nweb_seeds=false", None).await;
    let address: std::net::SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let tracker = tokio::spawn(async move {
        let codec = udp::Limits {
            max_packet_len: 65507,
            max_peers: 200,
        };
        let mut bytes = [0; 4096];
        let mut sent = Some(sent);
        loop {
            let (len, source) = socket.recv_from(&mut bytes).await.unwrap();
            assert_eq!(source, address);
            let request = udp::Request::decode(&bytes[..len], codec).unwrap();
            let kind = match request.kind {
                udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                    connection_id: 17,
                    extensions: &[],
                },
                udp::RequestKind::Announce { .. } => udp::ResponseKind::Announce {
                    interval: 30,
                    seeders: 1,
                    leechers: 0,
                    peers: udp::Peers::V4(PeersV4::parse(&[], 0).unwrap()),
                },
                _ => panic!("unexpected tracker request"),
            };
            let reply = udp::Response {
                transaction_id: request.transaction_id,
                kind,
            }
            .encode(codec)
            .unwrap();
            socket.send_to(&reply, source).await.unwrap();
            if matches!(request.kind, udp::RequestKind::Announce { .. })
                && let Some(sent) = sent.take()
            {
                sent.send(()).unwrap();
            }
        }
    });
    let id = daemon
        .add(&seed_metainfo("x", b"abc", Some(&url)), None)
        .await;
    tokio::time::timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    daemon.state(&id, "seeding").await;
    let ping = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    ping.send_to(
        b"d1:ad2:id20:abcdefghijklmnopqrste1:q4:ping1:t2:aa1:y1:qe",
        address,
    )
    .await
    .unwrap();
    let mut bytes = [0; 4096];
    let (len, source) = tokio::time::timeout(Duration::from_secs(5), ping.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source, address);
    let reply = spill::protocol::dht::krpc::Message::parse(
        &bytes[..len],
        spill::protocol::bencode::Limits::default(),
        128,
        200,
    )
    .unwrap();
    assert_eq!(reply.transaction_id(), b"aa");
    assert!(matches!(
        reply.kind(),
        spill::protocol::dht::krpc::Kind::Response { .. }
    ));
    daemon.stop().await;
    tracker.abort();
}

#[tokio::test]
async fn dual_stack_listener_announces_to_an_ipv4_udp_tracker() {
    use spill::protocol::{compact::PeersV4, tracker::udp};
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    std::fs::write(root.0.join("config.toml"), format!(
        "state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"[::]:0\"\nport_mapping=false\nallow_local_network=true\n[defaults]\ndht=false\ntrackers=true\nweb_seeds=false\n",
        root.0.join("state"), root.0.join("downloads"), root.0.join("control.sock")
    )).unwrap();
    let mut daemon = Daemon::start_config(&root.0).await;
    let address: std::net::SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(address.is_ipv6());
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", socket.local_addr().unwrap());
    let tracker = tokio::spawn(async move {
        let codec = udp::Limits {
            max_packet_len: 65507,
            max_peers: 200,
        };
        let mut bytes = [0; 4096];
        loop {
            let (length, source) = socket.recv_from(&mut bytes).await.unwrap();
            assert_eq!(source.port(), address.port());
            let request = udp::Request::decode(&bytes[..length], codec).unwrap();
            let kind = match request.kind {
                udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                    connection_id: 17,
                    extensions: &[],
                },
                udp::RequestKind::Announce { .. } => udp::ResponseKind::Announce {
                    interval: 30,
                    seeders: 1,
                    leechers: 0,
                    peers: udp::Peers::V4(PeersV4::parse(&[], 0).unwrap()),
                },
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
    let id = daemon
        .add(&seed_metainfo("x", b"abc", Some(&url)), None)
        .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = daemon.call(json!({"op":"get", "id":id})).await;
            assert!(row["last_error"].is_null(), "{row}");
            if row["transfer"]["trackers"][0]["state"] == "Success" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
    tracker.abort();
    assert!(tracker.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn udp_trackers_are_independent_and_retry_lost_packets() {
    use spill::protocol::{compact::PeersV4, tracker::udp};
    use tokio::net::UdpSocket;
    async fn tracker(
        socket: UdpSocket,
        drop_first: bool,
        sent: tokio::sync::mpsc::UnboundedSender<bool>,
    ) {
        let codec = udp::Limits {
            max_packet_len: 65507,
            max_peers: 200,
        };
        let mut first = None;
        let mut dropped = false;
        let mut bytes = [0; 4096];
        loop {
            let (len, source) = socket.recv_from(&mut bytes).await.unwrap();
            let request = udp::Request::decode(&bytes[..len], codec).unwrap();
            if drop_first && !dropped {
                dropped = true;
                first = Some(bytes[..len].to_vec());
                sent.send(false).unwrap();
                continue;
            }
            if let Some(packet) = first.take() {
                assert_eq!(&bytes[..len], packet, "retry must preserve the transaction");
            }
            let kind = match request.kind {
                udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                    connection_id: 17,
                    extensions: &[],
                },
                udp::RequestKind::Announce { announce, .. } => {
                    if announce.event == udp::EVENT_STARTED {
                        sent.send(true).unwrap();
                    }
                    udp::ResponseKind::Announce {
                        interval: 30,
                        seeders: 1,
                        leechers: 0,
                        peers: udp::Peers::V4(PeersV4::parse(&[], 0).unwrap()),
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
    }
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    let slow = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let fast = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let slow_url = format!("udp://{}/announce", slow.local_addr().unwrap());
    let fast_url = format!("udp://{}/announce", fast.local_addr().unwrap());
    let (slow_sent, mut slow_events) = tokio::sync::mpsc::unbounded_channel();
    let (fast_sent, mut fast_events) = tokio::sync::mpsc::unbounded_channel();
    let slow_task = tokio::spawn(tracker(slow, true, slow_sent));
    let fast_task = tokio::spawn(tracker(fast, false, fast_sent));
    let mut daemon =
        Daemon::start_with_caps(&root.0, "trackers=true\nweb_seeds=false", None, true, 2, 2).await;
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    std::fs::write(root.0.join("downloads/y"), b"abc").unwrap();
    let slow_id = daemon
        .add(&seed_metainfo("x", b"abc", Some(&slow_url)), None)
        .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), slow_events.recv())
            .await
            .unwrap(),
        Some(false)
    );
    let fast_id = daemon
        .add(&seed_metainfo("y", b"abc", Some(&fast_url)), None)
        .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), fast_events.recv())
            .await
            .expect("silent tracker must not block another torrent"),
        Some(true)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(18), slow_events.recv())
            .await
            .expect("lost UDP connect must be retransmitted"),
        Some(true)
    );
    daemon.state(&slow_id, "seeding").await;
    daemon.state(&fast_id, "seeding").await;
    daemon.stop().await;
    slow_task.abort();
    fast_task.abort();
}

#[tokio::test]
async fn udp_tracker_failure_text_reaches_status_in_both_exchanges() {
    use spill::protocol::tracker::udp;
    for fail_connect in [true, false] {
        let root = Temp::new();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let announce = format!("udp://{}/announce", socket.local_addr().unwrap());
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
                    udp::RequestKind::Connect { .. } if !fail_connect => {
                        udp::ResponseKind::Connect {
                            connection_id: 17,
                            extensions: &[],
                        }
                    }
                    _ => udp::ResponseKind::Error(b"Unregistered torrent"),
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
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = daemon.call(json!({"op":"get","id":id})).await;
                if row["last_error"] == "Unregistered torrent" {
                    assert_eq!(
                        row["transfer"]["trackers"][0]["error"],
                        "Unregistered torrent"
                    );
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
}
