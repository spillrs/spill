use super::transport::{utp_tracker, wait_transport};
use crate::common::{Daemon, Temp, WebSeed, metainfo, seed_metainfo};
use serde_json::json;
use sha1::Digest;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn discovery_accepts_new_contacts_after_4096_known_addresses() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = Temp::new();
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent = tokio::net::UdpSocket::bind(peer.local_addr().unwrap())
        .await
        .unwrap();
    let (url, tracker_task) = utp_tracker(peer.local_addr().unwrap()).await;
    let frame = |id: u8, payload: &[u8]| {
        let mut bytes = ((payload.len() + 2) as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(&[20, id]);
        bytes.extend_from_slice(payload);
        bytes
    };
    std::fs::write(root.0.join("config.toml"), format!(
        "state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"127.0.0.1:0\"\nallow_local_network=true\nport_mapping=false\nutp=true\npeers=33\ndht_bootstrap=[]\n[defaults]\ndht=false\npex=true\nweb_seeds=false\ntrackers=true\n",
        root.0.join("state"), root.0.join("downloads"), root.0.join("control.sock"),
    )).unwrap();
    let mut daemon = Daemon::start_config(&root.0).await;
    let id = daemon
        .add(&seed_metainfo("x", b"abc", Some(&url)), None)
        .await;
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(6), peer.accept())
        .await
        .unwrap()
        .unwrap();
    let mut hello = [0; 68];
    socket.read_exact(&mut hello).await.unwrap();
    let (mut handshake, _) = spill::protocol::peer::Handshake::decode(&hello).unwrap();
    handshake.peer_id = [46; 20];
    socket.write_all(&handshake.encode()).await.unwrap();
    wait_transport(&daemon, &id, "tcp").await;
    socket
        .write_all(&frame(0, b"d1:md6:ut_pexi2eee"))
        .await
        .unwrap();
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut sources = Vec::new();
    // Fill all peer slots before introducing hints, so none are dialed away.
    for i in 0..32 {
        let mut source = tokio::net::TcpStream::connect(&address).await.unwrap();
        handshake.peer_id = [50 + i; 20];
        source.write_all(&handshake.encode()).await.unwrap();
        source.read_exact(&mut hello).await.unwrap();
        source
            .write_all(&frame(0, b"d1:md6:ut_pexi2eee"))
            .await
            .unwrap();
        sources.push(source);
    }
    for (i, source) in sources.iter_mut().enumerate() {
        let count = if i == 31 { 127 } else { 128 };
        let mut peers = Vec::new();
        for n in 0..count {
            peers.extend_from_slice(&[127, 0, 0, 1]);
            peers.extend_from_slice(&(1024 + i as u16 * 128 + n).to_be_bytes());
        }
        let mut payload = format!("d5:added{}:", peers.len()).into_bytes();
        payload.extend_from_slice(&peers);
        payload.push(b'e');
        source.write_all(&frame(2, &payload)).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["transfer"]["pending_contacts"] == 4095 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("initial bounded contact queue");
    socket
        .write_all(&frame(2, b"d5:added6:\x7f\x00\x00\x01\x30\x39e"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["transfer"]["pending_contacts"] == 4096 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a retired uTP failure must not exclude a fresh tracker contact");
    daemon.stop().await;
    tracker_task.abort();
    drop(silent);
}

#[tokio::test]
async fn session_network_policy_controls_trackers_and_web_seeds() {
    use std::sync::{Arc, atomic::AtomicUsize};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    for allow_local_network in [false, true] {
        let root = Temp::new();
        let source = WebSeed::new().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let announce = format!(
            "http://localhost:{}/announce",
            listener.local_addr().unwrap().port()
        );
        let hits = Arc::new(AtomicUsize::new(0));
        let tracker_hits = hits.clone();
        let tracker = tokio::spawn(async move {
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
                tracker_hits.fetch_add(1, Ordering::Relaxed);
                let body = b"d8:intervali30e5:peers0:e";
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
        let mut bytes = format!("d8:announce{}:{}8:url-list{}:{}4:infod6:lengthi3e4:name1:x12:piece lengthi4e6:pieces20:", announce.len(), announce, source.url.len(), source.url).into_bytes();
        bytes.extend_from_slice(&sha1::Sha1::digest(b"abc"));
        bytes.extend_from_slice(b"ee");
        let mut daemon = Daemon::start_with_policy(
            &root.0,
            "trackers=true\nweb_seeds=true",
            None,
            allow_local_network,
        )
        .await;
        let id = daemon.add(&bytes, None).await;
        if allow_local_network {
            daemon.state(&id, "seeding").await;
            tokio::time::timeout(Duration::from_secs(5), async {
                while hits.load(Ordering::Relaxed) == 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(source.hits.load(Ordering::Relaxed), 1);
        } else {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let row = daemon.call(json!({"op":"get","id":id})).await;
                    if row["transfer"]["trackers"][0]["state"] == "Error" {
                        assert_eq!(row["verified_bytes"], 0);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(hits.load(Ordering::Relaxed), 0);
            assert_eq!(source.hits.load(Ordering::Relaxed), 0);
        }
        daemon.stop().await;
        tracker.abort();
    }
}

#[tokio::test]
async fn magnet_resolves_metadata_then_downloads_and_seeds() {
    use spill::engine::{
        profile::Profile,
        provisioning::NamedDestination,
        session::{Limits, Session},
        torrent::TorrentOptions,
    };
    use spill::protocol::{compact::PeersV4, tracker::udp};
    use tokio::net::UdpSocket;
    let profile = Profile::DEFAULT;
    let meta = profile.parse_owned(&metainfo(None)).unwrap();
    let hash = meta.info_hash();
    let mut seed = Session::bind(
        "127.0.0.1:0".parse().unwrap(),
        Limits {
            allow_local_network: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let seed_root = Temp::new();
    std::fs::write(seed_root.0.join("x"), b"abc").unwrap();
    let storage = NamedDestination::open_cancellable(
        std::fs::File::open(&seed_root.0).unwrap(),
        &meta,
        |_| Ok(()),
        || false,
    )
    .unwrap();
    let resume = storage.verified_resume().map(<[u8]>::to_vec);
    seed.admit(
        TorrentOptions::new(meta, profile)
            .storage(storage)
            .resume(resume),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !seed.torrent(hash).unwrap().snapshot().progress.ready {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let address = seed.local_addr();
    let peer = match address {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let mut contacts = peer.ip().octets().to_vec();
    contacts.extend_from_slice(&peer.port().to_be_bytes());
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let tracker = socket.local_addr().unwrap();
    let task = tokio::spawn(async move {
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
                udp::RequestKind::Announce { .. } => udp::ResponseKind::Announce {
                    interval: 30,
                    seeders: 1,
                    leechers: 0,
                    peers: udp::Peers::V4(PeersV4::parse(&contacts, 1).unwrap()),
                },
                _ => panic!(),
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
    let root = Temp::new();
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let magnet = format!("magnet:?xt=urn:btih:{hash}&tr=udp://{tracker}/announce");
    let id = daemon
        .call(json!({"op":"add","metainfo":null,"magnet":magnet,"output_dir":null}))
        .await["id"]
        .as_str()
        .unwrap()
        .to_owned();
    daemon
        .call(json!({"op":"set","id":id,"values":{"trackers":true}}))
        .await;
    daemon.state(&id, "seeding").await;
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
    daemon.stop().await;
    seed.shutdown().await.unwrap();
    task.abort();
    let _ = task.await;
}
