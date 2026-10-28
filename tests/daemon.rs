mod common;
use common::*;
use serde_json::json;
use sha1::Digest;
use std::{sync::atomic::Ordering, time::Duration};

async fn utp_tracker(address: std::net::SocketAddr) -> (String, tokio::task::JoinHandle<()>) {
    utp_tracker_gated(address, None).await
}
async fn utp_tracker_gated(
    address: std::net::SocketAddr,
    mut gate: Option<std::sync::Arc<tokio::sync::Notify>>,
) -> (String, tokio::task::JoinHandle<()>) {
    use spill::protocol::{compact::PeersV4, tracker::udp};
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", socket.local_addr().unwrap());
    let address = match address {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let port = address.port().to_be_bytes();
    let mut peers = address.ip().octets().to_vec();
    peers.extend_from_slice(&port);
    let task = tokio::spawn(async move {
        let limits = udp::Limits {
            max_packet_len: 65507,
            max_peers: 200,
        };
        let mut bytes = [0; 4096];
        loop {
            let (n, source) = socket.recv_from(&mut bytes).await.unwrap();
            let request = udp::Request::decode(&bytes[..n], limits).unwrap();
            let kind = match request.kind {
                udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                    connection_id: 17,
                    extensions: &[],
                },
                udp::RequestKind::Announce { .. } => {
                    if let Some(gate) = gate.take() {
                        gate.notified().await;
                    }
                    udp::ResponseKind::Announce {
                        interval: 30,
                        seeders: 1,
                        leechers: 1,
                        peers: udp::Peers::V4(PeersV4::parse(&peers, 1).unwrap()),
                    }
                }
                _ => panic!("unexpected tracker request"),
            };
            let reply = udp::Response {
                transaction_id: request.transaction_id,
                kind,
            }
            .encode(limits)
            .unwrap();
            socket.send_to(&reply, source).await.unwrap();
        }
    });
    (url, task)
}

async fn transport_daemon(root: &std::path::Path, utp: bool) -> Daemon {
    transport_daemon_at(root, utp, "127.0.0.1:0").await
}
async fn transport_daemon_at(root: &std::path::Path, utp: bool, listen: &str) -> Daemon {
    std::fs::write(root.join("config.toml"), format!(
        "state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"{listen}\"\nallow_local_network=true\nport_mapping=false\nutp={utp}\npeers=4\nwrite_queue_bytes=16384\ndht_bootstrap=[]\n[defaults]\ndht=false\npex=false\nweb_seeds=false\ntrackers=true\nupload_limit=262144\n",
        root.join("state"), root.join("downloads"), root.join("control.sock"),
    )).unwrap();
    Daemon::start_config(root).await
}

async fn wait_transport(daemon: &Daemon, id: &str, transport: &str) {
    let mut last = json!(null);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["transfer"]["peers"].as_array().is_some_and(|peers| {
                peers
                    .iter()
                    .any(|p| p["transport"] == transport && p["detail"]["established"] == true)
            }) {
                break;
            }
            last = row;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("established {transport} peer transport in diagnostics: {last}"));
}

async fn utp_transfer(chaos: bool, dual_stack: bool, strip_sack: bool) {
    let seed_root = Temp::new();
    let leech_root = Temp::new();
    let size = if chaos {
        4 * 1024 * 1024
    } else {
        2 * 1024 * 1024
    };
    let payload: Vec<u8> = (0..size).map(|i| (i * 31 % 251) as u8).collect();
    std::fs::create_dir_all(seed_root.0.join("downloads")).unwrap();
    std::fs::write(seed_root.0.join("downloads/x"), &payload).unwrap();
    let listen = if dual_stack { "[::]:0" } else { "127.0.0.1:0" };
    let mut seed = transport_daemon_at(&seed_root.0, true, listen).await;
    let mut seed_address: std::net::SocketAddr = seed.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    seed_address.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    let relay = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let contact = relay.local_addr().unwrap();
    let tiny_piece_headers = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_headers = tiny_piece_headers.clone();
    let relay_task = tokio::spawn(async move {
        let mut bytes = [0; 65536];
        let mut leech = None;
        let mut count = 0;
        let mut data = [0usize; 2];
        let mut held = None;
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tokio::select! {
                received = relay.recv_from(&mut bytes) => {
                    let (n, source) = received.unwrap();
                    let destination = if source == seed_address {
                        let Some(destination) = leech else { continue; }; destination
                    } else { leech = Some(source); seed_address };
                    let decoded = spill::protocol::utp::Packet::decode(&bytes[..n]).unwrap();
                    if source == seed_address && decoded.payload.len() == 13 && decoded.payload[4] == 7 {
                        observed_headers.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    if !chaos {
                        relay.send_to(&bytes[..n], destination).await.unwrap();
                        continue;
                    }
                    count += 1;
                    if n >= 20 && bytes[0] == 1 {
                        let direction = usize::from(source == seed_address);
                        data[direction] += 1;
                        if (100..110).contains(&(data[direction] % 2048)) { continue; }
                    }
                    if count % 20 == 0 { continue; }
                    let mut packet = Vec::new();
                    let mut decoded = spill::protocol::utp::Packet::decode(&bytes[..n]).unwrap();
                    if strip_sack { decoded.sack = None; }
                    decoded.encode(&mut packet);
                    if held.is_none() && count % 7 == 0 { held = Some((packet, destination)); continue; }
                    relay.send_to(&packet, destination).await.unwrap();
                    if count % 13 == 0 { relay.send_to(&packet, destination).await.unwrap(); }
                    if let Some((packet, target)) = held.take() { relay.send_to(&packet, target).await.unwrap(); }
                }
                _ = tick.tick() => {
                    if let Some((packet, target)) = held.take() { relay.send_to(&packet, target).await.unwrap(); }
                }
            }
        }
    });
    let (url, tracker) = utp_tracker(contact).await;
    let meta = seed_metainfo("x", &payload, Some(&url));
    let id = seed.add(&seed_metainfo("x", &payload, None), None).await;
    seed.state(&id, "seeding").await;
    let mut leech = transport_daemon_at(&leech_root.0, true, listen).await;
    leech.add(&meta, None).await;
    wait_transport(&leech, &id, "utp").await;
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let row = leech.call(json!({"op":"get","id":id})).await;
            assert!(row["last_error"].is_null(), "{row}");
            if row["state"] == "seeding" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("uTP download completion");
    let downloaded = std::fs::read(leech_root.0.join("downloads/x")).unwrap();
    assert_eq!(
        sha1::Sha1::digest(&downloaded),
        sha1::Sha1::digest(&payload)
    );
    assert_eq!(
        tiny_piece_headers.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "vectored piece headers must be coalesced with their payload"
    );
    if chaos {
        let row = seed.call(json!({"op":"get","id":id})).await;
        assert!(
            row["transfer"]["utp_retransmits"].as_u64().unwrap() > 0,
            "{row}"
        );
    }
    leech.stop().await;
    seed.stop().await;
    tracker.abort();
    relay_task.abort();
}

#[tokio::test]
async fn utp_transfer_between_two_daemons() {
    utp_transfer(false, false, false).await;
}

#[tokio::test]
async fn utp_survives_loss_and_reordering() {
    utp_transfer(true, false, false).await;
}

#[tokio::test]
async fn utp_recovers_burst_loss_without_selective_acks() {
    utp_transfer(true, false, true).await;
}

#[tokio::test]
async fn utp_ipv4_transfer_on_dual_stack_listeners() {
    utp_transfer(false, true, false).await;
}

async fn tcp_fallback(utp: bool) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = Temp::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    // Bind the UDP side without reading it, making silence deterministic on every OS.
    let silent = tokio::net::UdpSocket::bind(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (url, tracker) = utp_tracker(listener.local_addr().unwrap()).await;
    let meta = seed_metainfo("x", b"abc", Some(&url));
    let mut daemon = transport_daemon(&root.0, utp).await;
    let at = std::time::Instant::now();
    let id = daemon.add(&meta, None).await;
    let (mut peer, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    if utp {
        assert!(at.elapsed() >= Duration::from_millis(2900));
    } else {
        assert!(at.elapsed() < Duration::from_secs(2));
    }
    let mut hello = [0; 68];
    peer.read_exact(&mut hello).await.unwrap();
    let (mut handshake, _) = spill::protocol::peer::Handshake::decode(&hello).unwrap();
    handshake.peer_id = [42; 20];
    peer.write_all(&handshake.encode()).await.unwrap();
    wait_transport(&daemon, &id, "tcp").await;
    if utp {
        let mut packet = [0; 1500];
        let (n, _) = silent.try_recv_from(&mut packet).expect("outgoing SYN");
        let syn = spill::protocol::utp::Packet::decode(&packet[..n]).unwrap();
        assert_eq!(syn.header.kind, spill::protocol::utp::Kind::Syn);
        assert_ne!(syn.header.timestamp_us, 0, "SYN uses the mux clock");
        while silent.try_recv_from(&mut packet).is_ok() {}
        assert!(
            tokio::time::timeout(Duration::from_millis(1200), silent.recv_from(&mut packet))
                .await
                .is_err(),
            "a failed uTP dial must not keep sending SYNs after TCP fallback"
        );
    }
    if !utp {
        let address: std::net::SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut bytes = Vec::new();
        spill::protocol::utp::Packet {
            header: spill::protocol::utp::Header {
                kind: spill::protocol::utp::Kind::Syn,
                connection_id: 55,
                timestamp_us: 0,
                timestamp_diff_us: 0,
                wnd_size: 65536,
                seq_nr: 7,
                ack_nr: 0,
            },
            sack: None,
            payload: &[],
        }
        .encode(&mut bytes);
        socket.send_to(&bytes, address).await.unwrap();
        let mut reply = [0; 1500];
        let (n, _) = tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            spill::protocol::utp::Packet::decode(&reply[..n])
                .unwrap()
                .header
                .kind,
            spill::protocol::utp::Kind::Reset
        );
    }
    daemon.stop().await;
    tracker.abort();
}

#[tokio::test]
async fn dial_falls_back_to_tcp_when_utp_is_silent() {
    tcp_fallback(true).await;
}

#[tokio::test]
async fn utp_disabled_resets_syn_and_dials_tcp_directly() {
    tcp_fallback(false).await;
}

#[tokio::test]
async fn utp_inbound_respects_pending_handshake_caps() {
    use spill::protocol::utp::{Header, Kind, Packet};
    let root = Temp::new();
    let mut daemon = transport_daemon(&root.0, true).await;
    let meta = seed_metainfo("x", b"abc", None);
    let id = daemon.add(&meta, None).await;
    let address: std::net::SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut reply = [0; 1500];
    for i in 0..40 {
        let mut bytes = Vec::new();
        Packet {
            header: Header {
                kind: Kind::Syn,
                connection_id: 100 + i * 2,
                timestamp_us: 0,
                timestamp_diff_us: 0,
                wnd_size: 65536,
                seq_nr: 7,
                ack_nr: 0,
            },
            sack: None,
            payload: &[],
        }
        .encode(&mut bytes);
        socket.send_to(&bytes, address).await.unwrap();
        let (n, _) = tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            Packet::decode(&reply[..n]).unwrap().header.kind,
            if i < 2 { Kind::State } else { Kind::Reset }
        );
        if i == 0 {
            socket.send_to(&bytes, address).await.unwrap();
            let (n, _) = socket.recv_from(&mut reply).await.unwrap();
            assert_eq!(
                Packet::decode(&reply[..n]).unwrap().header.kind,
                Kind::State
            );
        }
    }
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&meta)
        .unwrap()
        .info_hash()
        .as_bytes();
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut tcp = tokio::net::TcpStream::connect(address).await.unwrap();
        let _ = tcp
            .write_all(
                &spill::protocol::peer::Handshake {
                    reserved: [0; 8],
                    info_hash: hash,
                    peer_id: [43; 20],
                }
                .encode(),
            )
            .await;
        let read = tokio::time::timeout(Duration::from_secs(1), tcp.read(&mut [0; 1]))
            .await
            .expect("TCP shares the uTP per-IP cap");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "capped TCP handshake must not be answered"
        );
    }
    tokio::time::sleep(Duration::from_secs(16)).await;
    let _peer = raw_peer(&address.to_string(), hash, 42).await;
    wait_transport(&daemon, &id, "tcp").await;
    daemon.stop().await;
}

#[tokio::test]
async fn utp_zero_window_probe_recovers_and_reset_accepts_send_id() {
    use spill::protocol::utp::{Header, Kind, Packet};
    let root = Temp::new();
    let mut daemon = transport_daemon(&root.0, true).await;
    let meta = seed_metainfo("x", b"abc", None);
    let id = daemon.add(&meta, None).await;
    daemon.state(&id, "downloading").await;
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&meta)
        .unwrap()
        .info_hash()
        .as_bytes();
    let address: std::net::SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let encode = |header, payload: &[u8]| {
        let mut bytes = Vec::new();
        Packet {
            header,
            sack: None,
            payload,
        }
        .encode(&mut bytes);
        bytes
    };
    let mut header = Header {
        kind: Kind::Syn,
        connection_id: 500,
        timestamp_us: 1,
        timestamp_diff_us: 0,
        wnd_size: 0,
        seq_nr: 7,
        ack_nr: 0,
    };
    socket.send_to(&encode(header, &[]), address).await.unwrap();
    let mut bytes = [0; 1500];
    let (n, _) = tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let state = Packet::decode(&bytes[..n]).unwrap();
    assert_eq!(state.header.kind, Kind::State);
    header.kind = Kind::Data;
    header.connection_id += 1;
    header.seq_nr += 1;
    header.ack_nr = state.header.seq_nr.wrapping_sub(1);
    let hello = spill::protocol::peer::Handshake {
        reserved: [0; 8],
        info_hash: hash,
        peer_id: [44; 20],
    }
    .encode();
    socket
        .send_to(&encode(header, &hello), address)
        .await
        .unwrap();
    let probe_seq = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
            let packet = Packet::decode(&bytes[..n]).unwrap();
            if packet.header.kind == Kind::Data {
                assert_eq!(packet.payload, &hello[..1]);
                break packet.header.seq_nr;
            }
        }
    })
    .await
    .expect("one-byte zero-window probe");
    let first_probe = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
            let packet = Packet::decode(&bytes[..n]).unwrap();
            if packet.header.kind == Kind::Data {
                assert_eq!(packet.header.seq_nr, probe_seq);
                break;
            }
        }
    })
    .await
    .expect("silent zero-window peer receives a backed-off probe");
    assert!(
        first_probe.elapsed() >= Duration::from_millis(1700),
        "zero-window probes must back off from the initial one-second RTO"
    );
    header.kind = Kind::State;
    header.seq_nr += 1;
    header.ack_nr = probe_seq.wrapping_sub(1);
    socket.send_to(&encode(header, &[]), address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
            let packet = Packet::decode(&bytes[..n]).unwrap();
            if packet.header.kind == Kind::Data {
                assert_eq!(packet.header.seq_nr, probe_seq);
                break;
            }
        }
    })
    .await
    .expect("an answered zero-window probe is not a failed retransmission");
    header.ack_nr = probe_seq;
    header.wnd_size = 65536;
    socket.send_to(&encode(header, &[]), address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
            let packet = Packet::decode(&bytes[..n]).unwrap();
            if packet.header.kind == Kind::Data {
                assert!(
                    packet.payload.len() > 1,
                    "window reopening resumes normal sends"
                );
                break;
            }
        }
    })
    .await
    .unwrap();
    wait_transport(&daemon, &id, "utp").await;
    // Keep the peer reader waiting inside a frame while two datagrams arrive.
    // Reopening a mostly empty receive window must not manufacture duplicate ACKs.
    header.kind = Kind::Data;
    let mut partial = vec![0; 1000];
    partial[..5].copy_from_slice(&[0, 0, 0x17, 0x70, 99]);
    socket
        .send_to(&encode(header, &partial), address)
        .await
        .unwrap();
    header.seq_nr += 1;
    socket
        .send_to(&encode(header, &[0; 1000]), address)
        .await
        .unwrap();
    let mut acks = 0;
    let _ = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
            let packet = Packet::decode(&bytes[..n]).unwrap();
            if packet.header.kind == Kind::State && matches!(packet.header.ack_nr, 9 | 10) {
                acks += 1;
            }
        }
    })
    .await;
    assert_eq!(
        acks, 1,
        "reads must not turn one delayed ACK into a duplicate-ACK train"
    );
    header.kind = Kind::Reset;
    // A reset can echo our receive ID, which is the server's send ID.
    header.connection_id -= 1;
    socket.send_to(&encode(header, &[]), address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["transfer"]["active_peers"] == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("RESET closes the peer");
    header.kind = Kind::Data;
    header.connection_id += 1;
    socket
        .send_to(&encode(header, b"x"), address)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
            if Packet::decode(&bytes[..n]).unwrap().header.kind == Kind::Reset {
                break;
            }
        }
    })
    .await
    .expect("unknown connection gets RESET");
    daemon.stop().await;
}

#[tokio::test]
async fn utp_reassembly_leaves_room_for_the_missing_datagram() {
    use spill::protocol::utp::{Header, Kind, Packet};
    let root = Temp::new();
    let mut daemon = transport_daemon(&root.0, true).await;
    let meta = seed_metainfo("x", b"abc", None);
    let id = daemon.add(&meta, None).await;
    daemon.state(&id, "downloading").await;
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&meta)
        .unwrap()
        .info_hash()
        .as_bytes();
    let address: std::net::SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let encode = |header, payload: &[u8]| {
        let mut bytes = Vec::new();
        Packet {
            header,
            sack: None,
            payload,
        }
        .encode(&mut bytes);
        bytes
    };
    let mut header = Header {
        kind: Kind::Syn,
        connection_id: 600,
        timestamp_us: 1,
        timestamp_diff_us: 0,
        wnd_size: 0,
        seq_nr: 7,
        ack_nr: 0,
    };
    socket.send_to(&encode(header, &[]), address).await.unwrap();
    let mut bytes = [0; 1500];
    let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
    let state = Packet::decode(&bytes[..n]).unwrap();
    assert_eq!(state.header.kind, Kind::State);
    header.kind = Kind::Data;
    header.connection_id += 1;
    header.ack_nr = state.header.seq_nr.wrapping_sub(1);
    // Libtorrent/libutp can send larger payloads than Spill's 1200-byte ceiling.
    // Hold back the first datagram while the reassembly buffer fills.
    for seq in 9..210 {
        header.seq_nr = seq;
        socket
            .send_to(&encode(header, &[0; 1452]), address)
            .await
            .unwrap();
        let (n, _) = tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        let packet = Packet::decode(&bytes[..n]).unwrap();
        assert_eq!(packet.header.ack_nr, 7);
        if seq == 9 {
            assert_eq!(packet.sack.unwrap()[0] & 1, 1);
        }
    }
    let mut first = [0; 1452];
    first[..68].copy_from_slice(
        &spill::protocol::peer::Handshake {
            reserved: [0; 8],
            info_hash: hash,
            peer_id: [45; 20],
        }
        .encode(),
    );
    header.seq_nr = 8;
    socket
        .send_to(&encode(header, &first), address)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
            if Packet::decode(&bytes[..n]).unwrap().header.ack_nr > 8 {
                break;
            }
        }
    })
    .await
    .expect("the missing datagram must fit and advance the cumulative ACK");
    daemon.stop().await;
}

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
async fn utp_mux_capacity_is_reclaimed_and_does_not_poison_contacts() {
    use spill::protocol::{
        peer::Handshake,
        utp::{Header, Kind, Packet},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = Temp::new();
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let udp_target = tokio::net::UdpSocket::bind(target.local_addr().unwrap())
        .await
        .unwrap();
    let gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let (url, tracker) = utp_tracker_gated(target.local_addr().unwrap(), Some(gate.clone())).await;
    std::fs::write(root.0.join("config.toml"), format!(
        "state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"127.0.0.1:0\"\nallow_local_network=true\nport_mapping=false\nutp=true\npeers=1\nactive_downloads=1\nactive_seeds=1\ndht_bootstrap=[]\n[defaults]\ndht=false\npex=false\nweb_seeds=false\ntrackers=true\n",
        root.0.join("state"), root.0.join("downloads"), root.0.join("control.sock"),
    )).unwrap();
    let mut daemon = Daemon::start_config(&root.0).await;
    let meta = seed_metainfo("x", b"abc", Some(&url));
    let id = daemon.add(&meta, None).await;
    daemon.state(&id, "downloading").await;
    let address: std::net::SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&meta)
        .unwrap()
        .info_hash()
        .as_bytes();
    // Self-connection rejection leaves FINs waiting for ACKs, filling the small mux budget.
    let mut frame = [0; 68];
    let mut hello = Handshake {
        reserved: [0; 8],
        info_hash: hash,
        peer_id: [98; 20],
    };
    let mut identity = tokio::net::TcpStream::connect(address).await.unwrap();
    identity.write_all(&hello.encode()).await.unwrap();
    identity.read_exact(&mut frame).await.unwrap();
    hello.peer_id = Handshake::decode(&frame).unwrap().0.peer_id;
    drop(identity);
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let encode = |header, payload: &[u8]| {
        let mut bytes = Vec::new();
        Packet {
            header,
            sack: None,
            payload,
        }
        .encode(&mut bytes);
        bytes
    };
    let mut bytes = [0; 1500];
    let mut header = Header {
        kind: Kind::Syn,
        connection_id: 1000,
        timestamp_us: 1,
        timestamp_diff_us: 0,
        wnd_size: 65536,
        seq_nr: 7,
        ack_nr: 0,
    };
    let filled = tokio::time::Instant::now();
    for i in 0..34 {
        header.kind = Kind::Syn;
        header.connection_id = 1000 + i * 2;
        header.seq_nr = 7;
        client.send_to(&encode(header, &[]), address).await.unwrap();
        let seq = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let (n, _) = client.recv_from(&mut bytes).await.unwrap();
                let packet = Packet::decode(&bytes[..n]).unwrap();
                if packet.header.connection_id == header.connection_id
                    && packet.header.kind == Kind::State
                {
                    break packet.header.seq_nr;
                }
            }
        })
        .await
        .expect("mux SYN admission");
        header.kind = Kind::Data;
        header.connection_id += 1;
        header.seq_nr = 8;
        header.ack_nr = seq.wrapping_sub(1);
        client
            .send_to(&encode(header, &hello.encode()), address)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let (n, _) = client.recv_from(&mut bytes).await.unwrap();
                let packet = Packet::decode(&bytes[..n]).unwrap();
                if packet.header.connection_id == header.connection_id - 1
                    && packet.header.kind == Kind::Fin
                {
                    break;
                }
            }
        })
        .await
        .expect("rejected self-connection sends FIN");
    }
    header.kind = Kind::Syn;
    header.connection_id = 5000;
    client.send_to(&encode(header, &[]), address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (n, _) = client.recv_from(&mut bytes).await.unwrap();
            let packet = Packet::decode(&bytes[..n]).unwrap();
            if packet.header.connection_id == 5000 {
                assert_eq!(packet.header.kind, Kind::Reset);
                break;
            }
        }
    })
    .await
    .expect("the mux budget is exhausted before releasing tracker peers");
    gate.notify_one();
    let (mut fallback, _) = tokio::time::timeout(Duration::from_secs(3), target.accept())
        .await
        .unwrap()
        .unwrap();
    fallback.read_exact(&mut frame).await.unwrap();
    let (mut reply, _) = Handshake::decode(&frame).unwrap();
    reply.peer_id = [97; 20];
    fallback.write_all(&reply.encode()).await.unwrap();
    wait_transport(&daemon, &id, "tcp").await;
    for i in 0..34 {
        header.kind = Kind::Reset;
        header.connection_id = 1001 + i * 2;
        client.send_to(&encode(header, &[]), address).await.unwrap();
    }
    header.kind = Kind::Syn;
    header.connection_id = 5002;
    client.send_to(&encode(header, &[]), address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (n, _) = client.recv_from(&mut bytes).await.unwrap();
            let packet = Packet::decode(&bytes[..n]).unwrap();
            if packet.header.connection_id == 5002 {
                assert_eq!(
                    packet.header.kind,
                    Kind::State,
                    "RESET must reclaim detached connections immediately"
                );
                break;
            }
        }
    })
    .await
    .unwrap();
    header.kind = Kind::Reset;
    header.connection_id = 5003;
    client.send_to(&encode(header, &[]), address).await.unwrap();
    drop(fallback);
    tokio::time::sleep_until(filled + Duration::from_secs(31)).await;
    daemon.call(json!({"op":"reannounce","id":id})).await;
    let (n, _) = tokio::time::timeout(Duration::from_secs(5), udp_target.recv_from(&mut bytes))
        .await
        .expect("a full mux must not disable uTP for the next dial")
        .unwrap();
    assert_eq!(Packet::decode(&bytes[..n]).unwrap().header.kind, Kind::Syn);
    daemon.stop().await;
    tracker.abort();
}

#[tokio::test]
async fn utp_shutdown_allows_fin_ack_before_reset() {
    use spill::protocol::{
        peer::Handshake,
        utp::{Header, Kind, Packet},
    };
    let root = Temp::new();
    let mut daemon = transport_daemon(&root.0, true).await;
    let meta = seed_metainfo("x", b"abc", None);
    let id = daemon.add(&meta, None).await;
    daemon.state(&id, "downloading").await;
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&meta)
        .unwrap()
        .info_hash()
        .as_bytes();
    let address: std::net::SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let encode = |header, payload: &[u8]| {
        let mut bytes = Vec::new();
        Packet {
            header,
            sack: None,
            payload,
        }
        .encode(&mut bytes);
        bytes
    };
    let mut header = Header {
        kind: Kind::Syn,
        connection_id: 700,
        timestamp_us: 1,
        timestamp_diff_us: 0,
        wnd_size: 65536,
        seq_nr: 7,
        ack_nr: 0,
    };
    socket.send_to(&encode(header, &[]), address).await.unwrap();
    let mut bytes = [0; 1500];
    let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
    let state = Packet::decode(&bytes[..n]).unwrap();
    assert_eq!(state.header.kind, Kind::State);
    header.kind = Kind::Data;
    header.connection_id += 1;
    header.seq_nr += 1;
    header.ack_nr = state.header.seq_nr.wrapping_sub(1);
    let hello = Handshake {
        reserved: [0; 8],
        info_hash: hash,
        peer_id: [96; 20],
    }
    .encode();
    socket
        .send_to(&encode(header, &hello), address)
        .await
        .unwrap();
    wait_transport(&daemon, &id, "utp").await;
    daemon.call(json!({"op":"shutdown"})).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stopped = tokio::spawn(async move {
        assert!(
            tokio::time::timeout(Duration::from_secs(4), daemon.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    });
    header.kind = Kind::State;
    header.seq_nr += 1;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
            let packet = Packet::decode(&bytes[..n]).unwrap();
            assert_ne!(
                packet.header.kind,
                Kind::Reset,
                "shutdown must first allow the FIN handshake"
            );
            if matches!(packet.header.kind, Kind::Data | Kind::Fin) {
                header.ack_nr = packet.header.seq_nr;
                socket.send_to(&encode(header, &[]), address).await.unwrap();
                if packet.header.kind == Kind::Fin {
                    break;
                }
            }
        }
    })
    .await
    .expect("FIN before shutdown deadline");
    stopped.await.unwrap();
    while let Ok((n, _)) = socket.try_recv_from(&mut bytes) {
        assert_ne!(
            Packet::decode(&bytes[..n]).unwrap().header.kind,
            Kind::Reset,
            "an acknowledged FIN needs no RESET"
        );
    }
}

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
async fn platform_default_paths_work_for_daemon_and_cli() {
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
    std::fs::write(&config, "listen=\"127.0.0.1:0\"\nport_mapping=false\ndht_bootstrap=[]\n[defaults]\ndht=false\ntrackers=false\npex=false\nweb_seeds=false\n").unwrap();
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
    let output = tokio::process::Command::new(BIN)
        .args(["ctl", "--json", "get", &id])
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

async fn mapping_daemon(root: &std::path::Path, gateway: &str, enabled: bool) -> Daemon {
    std::fs::write(root.join("config.toml"), format!(
        "state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"127.0.0.1:0\"\nallow_local_network=true\nport_mapping={enabled}\nport_mapping_gateway=\"{gateway}\"\ndht_bootstrap=[]\n[defaults]\ndht=false\npex=false\nweb_seeds=true\n",
        root.join("state"), root.join("downloads"), root.join("control.sock"),
    )).unwrap();
    Daemon::start_config(root).await
}

#[tokio::test]
async fn nat_pmp_maps_renews_advertises_and_deletes_on_shutdown() {
    use spill::protocol::{compact::PeersV4, tracker::udp};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    let gateway = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let gateway_address = gateway.local_addr().unwrap();
    let granted = std::sync::Arc::new(std::sync::atomic::AtomicU16::new(0));
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
    let responder = tokio::spawn({
        let granted = granted.clone();
        let requests = requests.clone();
        async move {
            let mut bytes = [0; 64];
            loop {
                let (n, source) = gateway.recv_from(&mut bytes).await.unwrap();
                let request = &bytes[..n];
                requests.lock().unwrap().push(request.to_vec());
                assert_eq!(request[0], 0);
                let mut response = vec![0, request[1] + 128, 0, 0, 0, 0, 0, 1];
                if request[1] == 0 {
                    assert_eq!(n, 2);
                    response.extend_from_slice(&[203, 0, 113, 7]);
                } else {
                    assert_eq!(n, 12);
                    assert!(matches!(request[1], 1 | 2));
                    let external = granted.load(Ordering::Relaxed);
                    response.extend_from_slice(&request[4..6]);
                    let external = if external == 0 {
                        u16::from_be_bytes([request[6], request[7]])
                    } else {
                        external
                    };
                    response.extend_from_slice(&external.to_be_bytes());
                    let lifetime = u32::from_be_bytes(request[8..12].try_into().unwrap());
                    assert!(matches!(lifetime, 0 | 7200));
                    response.extend_from_slice(
                        &(if lifetime == 0 { 0u32 } else { 2u32 }).to_be_bytes(),
                    );
                }
                gateway.send_to(&response, source).await.unwrap();
            }
        }
    });
    let mut daemon = mapping_daemon(&root.0, &gateway_address.to_string(), true).await;
    let health = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let health = daemon.call(json!({"op":"health"})).await;
            if health["port_mapping"]["protocol"] == "nat-pmp" {
                break health;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let address: std::net::SocketAddr = health["listen"].as_str().unwrap().parse().unwrap();
    assert_eq!(health["advertised_port"], address.port());
    assert_eq!(health["port_mapping"]["external_tcp_port"], address.port());
    assert_eq!(health["port_mapping"]["external_udp_port"], address.port());
    assert_eq!(health["port_mapping"]["external_ip"], "203.0.113.7");
    assert!(health["port_mapping"]["next_renewal"].is_u64());

    let tracker = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", tracker.local_addr().unwrap());
    let (announces, mut received) = tokio::sync::mpsc::unbounded_channel();
    let tracker_task = tokio::spawn(async move {
        let codec = udp::Limits {
            max_packet_len: 65507,
            max_peers: 200,
        };
        let mut bytes = [0; 4096];
        loop {
            let (n, source) = tracker.recv_from(&mut bytes).await.unwrap();
            let request = udp::Request::decode(&bytes[..n], codec).unwrap();
            let kind = match request.kind {
                udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                    connection_id: 17,
                    extensions: &[],
                },
                udp::RequestKind::Announce { announce, .. } => {
                    announces.send((announce.port, announce.event)).unwrap();
                    udp::ResponseKind::Announce {
                        interval: 30,
                        seeders: 1,
                        leechers: 0,
                        peers: udp::Peers::V4(PeersV4::parse(&[], 0).unwrap()),
                    }
                }
                _ => panic!("unexpected tracker request"),
            };
            tracker
                .send_to(
                    &udp::Response {
                        transaction_id: request.transaction_id,
                        kind,
                    }
                    .encode(codec)
                    .unwrap(),
                    source,
                )
                .await
                .unwrap();
        }
    });
    let bytes = seed_metainfo("x", b"abc", Some(&url));
    let hash = *spill::protocol::metainfo::Metainfo::parse(&bytes, Default::default())
        .unwrap()
        .info_hash()
        .as_bytes();
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    let started = std::time::Instant::now();
    let first = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, (address.port(), udp::EVENT_STARTED));
    let external = if address.port() == 54321 {
        54322
    } else {
        54321
    };
    granted.store(external, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let health = daemon.call(json!({"op":"health"})).await;
            if health["advertised_port"] == external {
                assert_eq!(health["port_mapping"]["external_tcp_port"], external);
                assert_eq!(health["port_mapping"]["external_udp_port"], external);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut reserved = [0; 8];
    reserved[5] = 0x10;
    peer.write_all(
        &spill::protocol::peer::Handshake {
            reserved,
            info_hash: hash,
            peer_id: [42; 20],
        }
        .encode(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut handshake = [0; 68];
        peer.read_exact(&mut handshake).await.unwrap();
        loop {
            let frame = peer_frame(&mut peer).await;
            if frame[4..].starts_with(&[20, 0]) {
                let hello = spill::protocol::bencode::decode(&frame[6..], Default::default()).unwrap();
                assert!(matches!(hello.get(b"p").unwrap().value(), spill::protocol::bencode::Value::Integer(p) if *p == i64::from(external)));
                break;
            }
        }
    }).await.unwrap();
    let next = tokio::time::timeout(Duration::from_secs(35), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next, (external, udp::EVENT_NONE));
    assert!(started.elapsed() >= Duration::from_secs(29));
    drop(peer);
    daemon.stop().await;
    let requests = requests.lock().unwrap();
    for opcode in [1, 2] {
        assert!(
            requests
                .iter()
                .any(|r| r[1] == opcode && r[8..12] == 7200u32.to_be_bytes())
        );
        assert!(
            requests
                .iter()
                .any(|r| r[1] == opcode && r[8..12] == [0; 4] && r[6..8] == [0; 2])
        );
    }
    for request in requests.iter().filter(|r| r[1] != 0) {
        assert_eq!(request[4..6], address.port().to_be_bytes());
        if request[8..12] != [0; 4] {
            assert_eq!(request[6..8], address.port().to_be_bytes());
        }
    }
    responder.abort();
    tracker_task.abort();
}

#[tokio::test]
async fn upnp_maps_leases_falls_back_to_permanent_and_deletes_on_shutdown() {
    use axum::{
        Router,
        http::{HeaderMap, StatusCode},
        routing::{get, post},
    };
    use std::sync::{Arc, Mutex, atomic::AtomicBool};
    for fault in [None, Some(725), Some(718)] {
        let root = Temp::new();
        let requests = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
        let first_add = Arc::new(AtomicBool::new(true));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let gateway = format!("{origin}/rootDesc.xml");
        let (base, control) = match fault {
            None => (
                format!("<URLBase>{origin}/descriptions/</URLBase>"),
                "../ctl".to_owned(),
            ),
            Some(725) => (String::new(), "/ctl".to_owned()),
            _ => (String::new(), format!("{origin}/ctl")),
        };
        let description = format!(
            "<?xml version=\"1.0\"?><root>{base}<device><serviceList><service><serviceType>urn:schemas-upnp-org:service:WANIPConnection:2</serviceType><controlURL>{control}</controlURL></service></serviceList></device></root>"
        );
        let router = Router::new().route("/rootDesc.xml", get(move || {
            let description = description.clone();
            async move { description }
        })).route("/ctl", post({
            let requests = requests.clone();
            move |headers: HeaderMap, body: String| {
                let requests = requests.clone();
                let first_add = first_add.clone();
                async move {
                    assert!(headers["content-type"].to_str().unwrap().starts_with("text/xml"));
                    let header = headers["soapaction"].to_str().unwrap();
                    let action = header.strip_prefix('"').unwrap().strip_suffix('"').unwrap()
                        .strip_prefix("urn:schemas-upnp-org:service:WANIPConnection:2#").unwrap();
                    assert!(body.contains(&format!("<u:{action} xmlns:u=\"urn:schemas-upnp-org:service:WANIPConnection:2\">")));
                    requests.lock().unwrap().push((action.to_owned(), body));
                    let error = if action == "AddPortMapping" {
                        match fault {
                            Some(725) if first_add.swap(false, Ordering::Relaxed) => Some(725),
                            Some(718) => Some(718),
                            _ => None,
                        }
                    } else { None };
                    let (status, reply) = if let Some(code) = error {
                        (StatusCode::INTERNAL_SERVER_ERROR, format!("<s:Fault><faultcode>s:Client</faultcode><detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{code}</errorCode></UPnPError></detail></s:Fault>"))
                    } else {
                        let fields = match action {
                            "GetExternalIPAddress" => "<NewExternalIPAddress>203.0.113.8</NewExternalIPAddress>",
                            "AddPortMapping" | "DeletePortMapping" => "",
                            _ => panic!("unexpected action {action}"),
                        };
                        (StatusCode::OK, format!("<u:{action}Response xmlns:u=\"urn:schemas-upnp-org:service:WANIPConnection:2\">{fields}</u:{action}Response>"))
                    };
                    (status, [("content-type", "text/xml")], format!("<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body>{reply}</s:Body></s:Envelope>"))
                }
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let source = WebSeed::new().await;
        let mut daemon = mapping_daemon(&root.0, &gateway, true).await;
        let id = daemon.add(&metainfo(Some(&source.url)), None).await;
        daemon.state(&id, "seeding").await;
        assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
        let health = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let health = daemon.call(json!({"op":"health"})).await;
                if health["port_mapping"]["protocol"] == "upnp"
                    || health["port_mapping"]["last_error"].is_string()
                {
                    break health;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let address: std::net::SocketAddr = health["listen"].as_str().unwrap().parse().unwrap();
        let mapping = &health["port_mapping"];
        assert_eq!(health["advertised_port"], address.port());
        if fault == Some(718) {
            assert!(
                mapping["last_error"]
                    .as_str()
                    .unwrap()
                    .contains("UPnP error 718"),
                "{health}"
            );
            assert!(mapping["protocol"].is_null());
        } else {
            assert_eq!(mapping["protocol"], "upnp");
            assert_eq!(mapping["external_ip"], "203.0.113.8");
            assert_eq!(mapping["external_tcp_port"], address.port());
            assert_eq!(mapping["external_udp_port"], address.port());
            assert!(mapping["last_error"].is_null());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            assert!((now + 3590..=now + 3600).contains(&mapping["next_renewal"].as_u64().unwrap()));
        }
        daemon.stop().await;
        let requests = requests.lock().unwrap();
        let adds: Vec<_> = requests
            .iter()
            .filter(|(action, _)| action == "AddPortMapping")
            .collect();
        assert_eq!(
            adds.len(),
            if fault == Some(718) {
                1
            } else if fault == Some(725) {
                3
            } else {
                2
            }
        );
        for (n, (_, body)) in adds.iter().enumerate() {
            for (key, value) in [
                ("NewRemoteHost", "".to_owned()),
                ("NewExternalPort", address.port().to_string()),
                ("NewInternalPort", address.port().to_string()),
                ("NewInternalClient", "127.0.0.1".into()),
                ("NewEnabled", "1".into()),
                ("NewPortMappingDescription", "spill".into()),
                (
                    "NewLeaseDuration",
                    if fault == Some(725) && n != 0 {
                        "0"
                    } else {
                        "7200"
                    }
                    .into(),
                ),
            ] {
                assert!(body.contains(&format!("<{key}>{value}</{key}>")), "{body}");
            }
        }
        let deletes: Vec<_> = requests
            .iter()
            .filter(|(action, _)| action == "DeletePortMapping")
            .collect();
        if fault == Some(718) {
            assert!(deletes.is_empty());
        } else {
            assert_eq!(deletes.len(), 2);
            for protocol in ["TCP", "UDP"] {
                assert!(adds.iter().any(|(_, body)| {
                    body.contains(&format!("<NewProtocol>{protocol}</NewProtocol>"))
                }));
                assert!(deletes.iter().any(|(_, body)| {
                    body.contains(&format!("<NewProtocol>{protocol}</NewProtocol>"))
                        && body.contains(&format!(
                            "<NewExternalPort>{}</NewExternalPort>",
                            address.port()
                        ))
                        && body.contains("<NewRemoteHost></NewRemoteHost>")
                }));
            }
        }
        server.abort();
    }
}

#[tokio::test]
async fn port_mapping_failure_is_nonfatal_and_disabled_sends_nothing() {
    for enabled in [true, false] {
        let root = Temp::new();
        let gateway = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let source = WebSeed::new().await;
        let mut daemon =
            mapping_daemon(&root.0, &gateway.local_addr().unwrap().to_string(), enabled).await;
        let id = daemon.add(&metainfo(Some(&source.url)), None).await;
        daemon.state(&id, "seeding").await;
        assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
        let health = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let health = daemon.call(json!({"op":"health"})).await;
                if !enabled || health["port_mapping"]["last_error"].is_string() {
                    break health;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let address: std::net::SocketAddr = health["listen"].as_str().unwrap().parse().unwrap();
        assert_eq!(health["advertised_port"], address.port());
        assert_eq!(health["port_mapping"]["enabled"], enabled);
        assert!(health["port_mapping"]["protocol"].is_null());
        let mut bytes = [0; 64];
        let received =
            tokio::time::timeout(Duration::from_millis(500), gateway.recv_from(&mut bytes)).await;
        assert_eq!(received.is_ok(), enabled);
        let hash = *spill::protocol::metainfo::Metainfo::parse(
            &metainfo(Some(&source.url)),
            Default::default(),
        )
        .unwrap()
        .info_hash()
        .as_bytes();
        let mut peer = raw_peer(&address.to_string(), hash, 43).await;
        use tokio::io::AsyncWriteExt;
        peer.write_all(&[0, 0, 0, 1, 2]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if peer_frame(&mut peer).await == [0, 0, 0, 1, 1] {
                    break;
                }
            }
            peer.write_all(&[0, 0, 0, 13, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3])
                .await
                .unwrap();
            loop {
                let frame = peer_frame(&mut peer).await;
                if frame.get(4) == Some(&7) {
                    assert_eq!(&frame[13..], b"abc");
                    break;
                }
            }
        })
        .await
        .unwrap();
        drop(peer);
        daemon.stop().await;
    }
}

#[tokio::test]
async fn add_download_seed() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&metainfo(Some(&source.url)), None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 3);
    assert!(row["transfer"]["trackers"].is_array());
    assert!(row["transfer"]["download_bytes_per_second"].is_number());
    assert!(row["transfer"]["upload_bytes_per_second"].is_number());
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
    assert_eq!(source.hits.load(Ordering::Relaxed), 1);
    daemon.stop().await;
}

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

#[tokio::test]
async fn overlapping_uploads_in_opposite_orders_do_not_deadlock() {
    use spill::protocol::peer::message::{BlockRequest, Message};
    use tokio::io::AsyncWriteExt;
    const BLOCK: usize = 16384;
    const BLOCKS: usize = 128;
    let payload: Vec<_> = (0..BLOCK * BLOCKS).map(|i| (i / BLOCK) as u8).collect();
    let bytes = seed_metainfo("x", &payload, None);
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&bytes)
        .unwrap()
        .info_hash()
        .as_bytes();
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), &payload).unwrap();
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut a = raw_peer(&address, hash, 11).await;
    let mut b = raw_peer(&address, hash, 12).await;
    async fn unchoke(peer: &mut tokio::net::TcpStream) {
        peer.write_all(&[0, 0, 0, 1, 2]).await.unwrap();
        loop {
            if matches!(
                Message::decode(&peer_frame(peer).await, 4 * 1024 * 1024)
                    .unwrap()
                    .0,
                Message::Unchoke
            ) {
                break;
            }
        }
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(unchoke(&mut a), unchoke(&mut b));
    })
    .await
    .unwrap();
    let requests = |reverse: bool| {
        let mut bytes = Vec::new();
        for n in 0..BLOCKS {
            let block = if reverse { BLOCKS - 1 - n } else { n };
            let message = Message::Request(BlockRequest {
                index: 0,
                begin: (block * BLOCK) as u32,
                length: BLOCK as u32,
            });
            let start = bytes.len();
            bytes.resize(start + message.encoded_len(4 * 1024 * 1024).unwrap(), 0);
            message
                .encode(&mut bytes[start..], 4 * 1024 * 1024)
                .unwrap();
        }
        bytes
    };
    let forward = requests(false);
    let reverse = requests(true);
    async fn receive(peer: &mut tokio::net::TcpStream, payload: &[u8]) {
        let mut seen = [false; BLOCKS];
        for _ in 0..BLOCKS {
            loop {
                let frame = peer_frame(peer).await;
                if let Message::Piece {
                    index,
                    begin,
                    block,
                } = Message::decode(&frame, 4 * 1024 * 1024).unwrap().0
                {
                    assert_eq!(index, 0);
                    assert_eq!(block, &payload[begin as usize..begin as usize + BLOCK]);
                    assert!(!seen[begin as usize / BLOCK]);
                    seen[begin as usize / BLOCK] = true;
                    break;
                }
            }
        }
    }
    let completed = tokio::time::timeout(Duration::from_secs(5), async {
        let (a_sent, b_sent) = tokio::join!(a.write_all(&forward), b.write_all(&reverse));
        a_sent.unwrap();
        b_sent.unwrap();
        tokio::join!(receive(&mut a, &payload), receive(&mut b, &payload));
    })
    .await;
    if completed.is_err() {
        let row = daemon.call(json!({"op":"get", "id":id})).await;
        panic!("overlapping upload queues must drain before request timeout: {row}");
    }
    daemon.stop().await;
}

#[tokio::test]
async fn endgame_requests_progress_while_other_duplicates_are_stalled() {
    use spill::protocol::peer::{
        Handshake,
        message::{BlockRequest, Message},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    async fn connect(address: &str, hash: [u8; 20], id: u8) -> TcpStream {
        let mut peer = TcpStream::connect(address).await.unwrap();
        let mut reserved = [0; 8];
        reserved[7] = 4;
        peer.write_all(
            &Handshake {
                reserved,
                info_hash: hash,
                peer_id: [id; 20],
            }
            .encode(),
        )
        .await
        .unwrap();
        let mut hello = [0; 68];
        peer.read_exact(&mut hello).await.unwrap();
        assert_ne!(hello[27] & 4, 0);
        peer.write_all(&[0, 0, 0, 1, 14, 0, 0, 0, 1, 1])
            .await
            .unwrap();
        peer
    }
    async fn frame(peer: &mut TcpStream) -> Vec<u8> {
        let length = peer.read_u32().await.unwrap();
        let mut bytes = length.to_be_bytes().to_vec();
        bytes.resize(4 + length as usize, 0);
        peer.read_exact(&mut bytes[4..]).await.unwrap();
        bytes
    }
    async fn request(peer: &mut TcpStream) -> BlockRequest {
        loop {
            if let Message::Request(request) =
                Message::decode(&frame(peer).await, 1024 * 1024).unwrap().0
            {
                return request;
            }
        }
    }
    async fn respond(peer: &mut TcpStream, request: BlockRequest, payload: &[u8]) {
        let start = request.begin as usize;
        let message = Message::Piece {
            index: request.index,
            begin: request.begin,
            block: &payload[start..start + request.length as usize],
        };
        let mut bytes = vec![0; message.encoded_len(1024 * 1024).unwrap()];
        message.encode(&mut bytes, 1024 * 1024).unwrap();
        peer.write_all(&bytes).await.unwrap();
    }

    const BLOCK: usize = 16384;
    let payload: Vec<_> = (0..16 * BLOCK).map(|i| ((i / BLOCK) ^ i) as u8).collect();
    let mut bytes = format!(
        "d4:infod6:lengthi{}e4:name1:x12:piece lengthi{}e6:pieces20:",
        payload.len(),
        payload.len()
    )
    .into_bytes();
    bytes.extend_from_slice(&sha1::Sha1::digest(&payload));
    bytes.extend_from_slice(b"ee");
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&bytes)
        .unwrap()
        .info_hash()
        .as_bytes();
    let root = Temp::new();
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "downloading").await;
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut primary = connect(&address, hash, 1).await;
    let mut held = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        let first = request(&mut primary).await;
        // Grow the normal peer window past four before withholding the tail.
        tokio::time::sleep(Duration::from_millis(300)).await;
        respond(&mut primary, first, &payload).await;
        while held.len() < 8 {
            let request = request(&mut primary).await;
            if request.begin < (8 * BLOCK) as u32 {
                respond(&mut primary, request, &payload).await;
            } else {
                held.push(request);
            }
        }
    })
    .await
    .expect("primary must acquire the eight tail blocks");
    primary.write_all(&[0, 0, 0, 1, 0]).await.unwrap();

    let mut stalled = connect(&address, hash, 2).await;
    let mut duplicates = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..4 {
            let request = request(&mut stalled).await;
            assert!(held.contains(&request));
            duplicates.push(request);
        }
    })
    .await
    .expect("second peer must acquire four duplicate requests");
    // Fast choke retains these requests: the peer may still fulfill them later.
    stalled.write_all(&[0, 0, 0, 1, 0]).await.unwrap();

    let mut healthy = connect(&address, hash, 3).await;
    let mut rescued = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..4 {
            let request = request(&mut healthy).await;
            assert!(held.contains(&request));
            assert!(!duplicates.contains(&request));
            assert!(!rescued.contains(&request));
            rescued.push(request);
        }
    })
    .await
    .expect("healthy peer must receive the other tail blocks without a 30-second timeout");

    let mut fourth = connect(&address, hash, 4).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), request(&mut fourth))
            .await
            .is_err(),
        "each outstanding block already has two live copies"
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            let peers = row["transfer"]["peers"].as_array().unwrap();
            if peers.len() == 4 {
                let free = row["transfer"]["writer_free_slots"].as_u64().unwrap();
                let slots = spill::engine::profile::Profile::DEFAULT.write_queue_bytes
                    / spill::engine::profile::BLOCK_LEN as usize;
                assert!(free <= slots as u64, "{row}");
                assert_eq!(row["transfer"]["upload_reads_in_flight"], 0);
                assert!(
                    peers
                        .iter()
                        .all(|peer| peer["detail"]["fast_extension"] == true)
                );
                assert!(
                    peers
                        .iter()
                        .any(|peer| peer["detail"]["remote_choking"] == true
                            && peer["detail"]["assigned_requests"] == 8)
                );
                assert_eq!(
                    peers
                        .iter()
                        .map(|peer| peer["detail"]["authorized_requests"].as_u64().unwrap())
                        .sum::<u64>(),
                    16
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("JSON must expose the stalled Fast peers and outstanding requests");
    for request in duplicates {
        respond(&mut stalled, request, &payload).await;
    }
    for request in rescued {
        respond(&mut healthy, request, &payload).await;
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut canceled = Vec::new();
        while canceled.len() < held.len() {
            if let Message::Cancel(request) =
                Message::decode(&frame(&mut primary).await, 1024 * 1024)
                    .unwrap()
                    .0
            {
                assert!(held.contains(&request));
                assert!(!canceled.contains(&request));
                canceled.push(request);
            }
        }
    })
    .await
    .expect("fulfilled duplicates must cancel all original requests");
    daemon.state(&id, "seeding").await;
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), payload);
    daemon.stop().await;
}

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
        assert_eq!(&hello[48..56], b"-sP0500-");
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

#[tokio::test]
async fn multi_block_download_and_parallel_recheck_preserve_piece_membership() {
    use sha1::{Digest, Sha1};
    let root = Temp::new();
    // Multiple pieces, each spanning several peer blocks, exercise queued write
    // batches and their hash barriers rather than just single-block transfers.
    const PIECE_LEN: usize = 128 * 1024;
    // Nine pieces exceed the four-job limit; full pieces span multiple hash
    // chunks and the final piece is short.
    let payload: Vec<u8> = (0..8 * PIECE_LEN + 37)
        .map(|i| ((i / 16384) ^ i) as u8)
        .collect();
    let mut source = WebSeed::files(&[("/payload", &payload)], false).await;
    source.url.push_str("payload");
    let mut bytes = format!(
        "d8:url-list{}:{}4:infod6:lengthi{}e4:name1:x12:piece lengthi{PIECE_LEN}e6:pieces{}:",
        source.url.len(),
        source.url,
        payload.len(),
        payload.len().div_ceil(PIECE_LEN) * 20,
    )
    .into_bytes();
    for piece in payload.chunks(PIECE_LEN) {
        bytes.extend_from_slice(&Sha1::digest(piece));
    }
    bytes.extend_from_slice(b"ee");
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], payload.len());
    // The merged bandwidth module must throttle both core request admission and
    // transport writes, including wakeups after the initial burst is exhausted.
    daemon
        .call(json!({"op":"set","id":id,"values":{"upload_limit":1048576}}))
        .await;
    daemon.state(&id, "seeding").await;
    // Exercise both the peer and writer readiness paths with simultaneous TCP
    // leechers and more blocks than one outbound queue can hold.
    let profile = spill::engine::profile::Profile {
        peers: 1,
        cache_bytes: spill::engine::profile::BLOCK_LEN as usize,
        write_queue_bytes: 2 * spill::engine::profile::BLOCK_LEN as usize,
    };
    let meta = profile.parse_owned(&bytes).unwrap();
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    let started = tokio::time::Instant::now();
    let mut downloads = tokio::task::JoinSet::new();
    for diagnostics in [false, true] {
        let meta = meta.clone();
        downloads.spawn(async move {
            let output = Temp::new();
            let storage = spill::engine::provisioning::NamedDestination::open_cancellable(
                std::fs::File::open(&output.0).unwrap(),
                &meta,
                |_| Ok(()),
                || false,
            )
            .unwrap();
            let mut session = spill::engine::session::Session::bind(
                "127.0.0.1:0".parse().unwrap(),
                spill::engine::session::Limits {
                    allow_local_network: true,
                    torrents: 1,
                    peers: 1,
                    payload_bytes: profile.cache_bytes + profile.write_queue_bytes,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
            let hash = session
                .admit(
                    spill::engine::torrent::TorrentOptions::new(meta, profile)
                        .storage(storage)
                        .settings(spill::engine::settings::TorrentSettings {
                            trackers: false,
                            dht: false,
                            pex: false,
                            web_seeds: false,
                            download_limit: 512 * 1024,
                            ..Default::default()
                        })
                        .peers(vec![address])
                        .diagnostics(diagnostics),
                )
                .unwrap();
            let mut observer = session.torrent(hash).unwrap();
            assert_eq!(observer.snapshot().diagnostics.is_some(), diagnostics);
            tokio::time::timeout(Duration::from_secs(5), async {
                while !observer.snapshot().progress.completed {
                    assert!(
                        observer.changed().await,
                        "TCP torrent stopped before completion"
                    );
                }
            })
            .await
            .expect("TCP download progress");
            session.shutdown().await.unwrap();
            let snapshot = observer.snapshot();
            assert!(snapshot.progress.joined && snapshot.progress.completed);
            assert_eq!(
                snapshot.progress.verified_bytes,
                snapshot.progress.total_bytes
            );
            assert!(snapshot.progress.download_bytes_per_second.is_finite());
            assert!(snapshot.progress.upload_bytes_per_second.is_finite());
            let status = serde_json::to_value(snapshot).unwrap();
            assert_eq!(status.get("diagnostics").is_some(), diagnostics);
            assert_eq!(
                status["progress"]["verified_piece_bitfield"],
                json!([255, 128])
            );
            for removed in [
                "admitted_connections",
                "tracker_exhausted",
                "queued_payload_bytes",
            ] {
                assert!(status.get(removed).is_none());
                assert!(status["progress"].get(removed).is_none());
            }
            std::fs::read(output.0.join("x")).unwrap()
        });
    }
    while let Some(download) = downloads.join_next().await {
        assert_eq!(download.unwrap(), payload);
    }
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "payload budgets must outlive the initial burst"
    );
    daemon.stop().await;
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), payload);
    let hits = source.hits.load(Ordering::Relaxed);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    daemon.state(&id, "seeding").await;
    assert_eq!(source.hits.load(Ordering::Relaxed), hits);
    daemon.call(json!({"op":"pause","id":id})).await;
    daemon.state(&id, "paused").await;
    let mut damaged = payload.clone();
    damaged[PIECE_LEN + 17] ^= 0xff;
    *damaged.last_mut().unwrap() ^= 0xff;
    std::fs::write(root.0.join("downloads/x"), damaged).unwrap();
    daemon.call(json!({"op":"recheck","id":id})).await;
    let row = daemon.state(&id, "paused").await;
    assert_eq!(row["verified_bytes"], 7 * PIECE_LEN);
    assert_eq!(
        source.hits.load(Ordering::Relaxed),
        hits,
        "recheck must not download"
    );
    let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
    let resume: Vec<u8> = db
        .query_row("SELECT resume FROM torrents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        spill::protocol::resume::resume_piece_map(&meta, &resume).unwrap(),
        [true, false, true, true, true, true, true, true, false]
    );
    daemon.call(json!({"op":"resume","id":id})).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], payload.len());
    daemon.stop().await;
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), payload);
}

#[tokio::test]
async fn recheck_completion_controls_download_and_seed_admission() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&metainfo(Some(&source.url)), None).await;
    daemon.state(&id, "seeding").await;
    daemon
        .call(json!({"op":"set","id":id,"values":{"web_seeds":false}}))
        .await;
    daemon.call(json!({"op":"pause","id":id})).await;
    std::fs::write(root.0.join("downloads/x"), b"xbc").unwrap();
    daemon.call(json!({"op":"recheck","id":id})).await;
    let row = daemon.state(&id, "paused").await;
    assert_eq!(row["verified_bytes"], 0);
    let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
    let complete: bool = db
        .query_row("SELECT complete FROM torrents", [], |r| r.get(0))
        .unwrap();
    assert!(!complete);
    daemon.call(json!({"op":"resume","id":id})).await;
    daemon.state(&id, "downloading").await;
    daemon.call(json!({"op":"pause","id":id})).await;
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    daemon.call(json!({"op":"recheck","id":id})).await;
    let row = daemon.state(&id, "paused").await;
    assert_eq!(row["verified_bytes"], 3);
    let complete: bool = db
        .query_row("SELECT complete FROM torrents", [], |r| r.get(0))
        .unwrap();
    assert!(complete);
    daemon.stop().await;

    let mut daemon = Daemon::start_with_caps(&root.0, "web_seeds=false", None, true, 0, 1).await;
    daemon.call(json!({"op":"resume","id":id})).await;
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
}

#[tokio::test]
async fn resuming_a_queued_torrent_does_not_preempt_a_running_download() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = Temp::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/payload", listener.local_addr().unwrap());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let gate = release.clone();
    let seed = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await.unwrap());
        }
        gate.notified().await;
        stream.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 0-2/3\r\nConnection: close\r\n\r\nabc").await.unwrap();
    });
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let a = daemon.add(&seed_metainfo("a", b"abc", None), None).await;
    daemon.state(&a, "downloading").await;
    daemon.call(json!({"op":"pause", "id":a})).await;
    let b = daemon.add(&metainfo(Some(&url)), None).await;
    daemon.state(&b, "downloading").await;
    daemon.call(json!({"op":"resume", "id":a})).await;
    for _ in 0..5 {
        assert_eq!(
            daemon.call(json!({"op":"get", "id":a})).await["state"],
            "queued"
        );
        assert_eq!(
            daemon.call(json!({"op":"get", "id":b})).await["state"],
            "downloading"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    release.notify_one();
    daemon.state(&b, "seeding").await;
    daemon.state(&a, "downloading").await;
    daemon.stop().await;
    seed.await.unwrap();
}

#[tokio::test]
async fn ipv6_restart_keeps_torrents_running_when_lazy_dht_fails() {
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    let mut daemon = Daemon::start(&root.0, "dht=true\nweb_seeds=false", None).await;
    let id = daemon.add(&seed_metainfo("x", b"abc", None), None).await;
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
    let config = root.0.join("config.toml");
    let contents = std::fs::read_to_string(&config)
        .unwrap()
        .replace("127.0.0.1:0", "[::1]:0")
        .replace("dht=true", "dht=false");
    std::fs::write(&config, contents).unwrap();
    let log = root.0.join("stderr");
    daemon.child = tokio::process::Command::new(BIN)
        .arg("serve")
        .arg("--config")
        .arg(config)
        .stderr(std::fs::File::create(&log).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while tokio::net::UnixStream::connect(&daemon.socket)
            .await
            .is_err()
        {
            assert!(daemon.child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["settings"]["dht"], true);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(daemon.child.try_wait().unwrap().is_none());
    assert!(
        daemon.call(json!({"op":"health"})).await["listen"]
            .as_str()
            .unwrap()
            .starts_with("[::1]:")
    );
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
    assert_eq!(
        std::fs::read_to_string(log)
            .unwrap()
            .matches("DHT disabled:")
            .count(),
        1
    );
}

#[tokio::test]
async fn restart_resumes_from_verified_checkpoint_and_rejects_old_catalogs() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let bytes = metainfo(Some(&source.url));
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    daemon.stop().await;
    let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        3
    );
    let resume: Vec<u8> = db
        .query_row("SELECT resume FROM torrents", [], |r| r.get(0))
        .unwrap();
    assert!(!resume.is_empty());
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    daemon.state(&id, "seeding").await;
    assert_eq!(
        source.hits.load(Ordering::Relaxed),
        1,
        "resume should not download again"
    );
    daemon.stop().await;
    // A supplied resume must still be checked against the reopened files.
    // Same-length damage must invalidate the saved verified bit and be repaired.
    let payload = root.0.join("downloads/x");
    let modified = std::fs::metadata(&payload).unwrap().modified().unwrap();
    std::fs::write(&payload, b"bad").unwrap();
    let modified = modified + Duration::from_nanos(1);
    std::fs::File::options()
        .write(true)
        .open(&payload)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    assert_eq!(
        std::fs::metadata(&payload).unwrap().modified().unwrap(),
        modified
    );
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    daemon.state(&id, "seeding").await;
    assert_eq!(source.hits.load(Ordering::Relaxed), 2);
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
    daemon.call(json!({"op":"recheck","id":id})).await;
    daemon.state(&id, "paused").await;
    daemon.stop().await;
    db.execute_batch("PRAGMA user_version=9").unwrap();
    drop(db);
    let status = tokio::process::Command::new(BIN)
        .arg("serve")
        .arg("--config")
        .arg(root.0.join("config.toml"))
        .output()
        .await
        .unwrap();
    assert_eq!(status.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&status.stderr).contains("fresh state directory"));
}

#[tokio::test]
async fn directory_conflicts_are_rejected_without_claiming_payload_paths() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/test/a", b"abc"), ("/test/b", b"def")], false).await;
    let bytes = multifile_metainfo(&source.url, &[("a", b"abc", false), ("b", b"def", false)]);
    for retry in [false, true] {
        let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
        let conflict = root.0.join("downloads/test/b.part");
        std::fs::create_dir_all(&conflict).unwrap();
        let id = daemon.add(&bytes, None).await;
        let row = daemon.failed(&id).await;
        let error = row["last_error"].as_str().unwrap();
        assert!(error.contains("payload test/b"), "{error}");
        assert!(error.contains("file already exists"), "{error}");
        assert!(!root.0.join("downloads/test/a.part").exists());
        daemon.stop().await;

        let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
        if retry {
            std::fs::remove_dir(&conflict).unwrap();
            daemon.call(json!({"op":"resume","id":id})).await;
            daemon.state(&id, "seeding").await;
            assert_eq!(
                std::fs::read(root.0.join("downloads/test/a")).unwrap(),
                b"abc"
            );
            assert_eq!(
                std::fs::read(root.0.join("downloads/test/b")).unwrap(),
                b"def"
            );
        }
        daemon
            .call(json!({"op":"remove","id":id,"delete_payload":true}))
            .await;
        if retry {
            assert!(!root.0.join("downloads/test").exists());
        } else {
            assert!(conflict.is_dir());
            std::fs::remove_dir_all(root.0.join("downloads/test")).unwrap();
        }
        assert_eq!(daemon.raw(json!({"op":"get","id":id})).await["code"], 3);
        daemon.stop().await;
    }
}

#[tokio::test]
async fn failed_add_does_not_claim_foreign_partial_files() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/test/a", b"abc"), ("/test/b", b"def")], false).await;
    let bytes = multifile_metainfo(&source.url, &[("a", b"abc", false), ("b", b"def", false)]);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let foreign = root.0.join("downloads/test/a.part");
    std::fs::create_dir_all(foreign.parent().unwrap()).unwrap();
    std::fs::write(&foreign, b"unrelated").unwrap();
    let id = daemon.add(&bytes, None).await;
    let row = daemon.failed(&id).await;
    assert!(
        row["last_error"]
            .as_str()
            .unwrap()
            .contains("file already exists")
    );
    daemon
        .call(json!({"op":"remove","id":id,"delete_payload":true}))
        .await;
    assert_eq!(std::fs::read(&foreign).unwrap(), b"unrelated");
    assert!(!root.0.join("downloads/test/b.part").exists());
    assert_eq!(daemon.raw(json!({"op":"get","id":id})).await["code"], 3);
    daemon.stop().await;
}

#[tokio::test]
async fn payload_removal_preserves_unrelated_files_and_removes_catalog_entry() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/test/a", b"abc")], false).await;
    let bytes = multifile_metainfo(&source.url, &[("a", b"abc", false)]);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    let stray = root.0.join("downloads/test/keep");
    std::fs::write(&stray, b"unrelated").unwrap();
    daemon
        .call(json!({"op":"remove","id":id,"delete_payload":true}))
        .await;
    assert!(!root.0.join("downloads/test/a").exists());
    assert!(!root.0.join("downloads/test/a.part").exists());
    assert_eq!(std::fs::read(&stray).unwrap(), b"unrelated");
    assert_eq!(daemon.raw(json!({"op":"get","id":id})).await["code"], 3);
    assert!(
        daemon.call(json!({"op":"list"})).await["torrents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    daemon.stop().await;
}

#[tokio::test]
async fn pause_resume_remove_keep_and_delete_payload() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let bytes = metainfo(Some(&source.url));
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    daemon.call(json!({"op":"pause","id":id})).await;
    daemon.state(&id, "paused").await;
    daemon.call(json!({"op":"resume","id":id})).await;
    daemon.state(&id, "seeding").await;
    daemon
        .call(json!({"op":"remove","id":id,"delete_payload":false}))
        .await;
    assert!(root.0.join("downloads/x").exists());
    assert_eq!(daemon.raw(json!({"op":"get","id":id})).await["code"], 3);
    let output = root.0.join("second");
    let id = daemon.add(&bytes, Some(&output)).await;
    daemon.state(&id, "seeding").await;
    daemon
        .call(json!({"op":"remove","id":id,"delete_payload":true}))
        .await;
    assert!(!output.join("x").exists());
    assert!(root.0.join("downloads/x").exists());
    daemon.stop().await;
}

#[tokio::test]
async fn get_set_settings_take_effect_and_cli_uses_the_same_schema() {
    let root = Temp::new();
    let source = WebSeed::new().await;
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let id = daemon.add(&metainfo(Some(&source.url)), None).await;
    daemon.state(&id, "downloading").await;
    assert_eq!(source.hits.load(Ordering::Relaxed), 0);
    assert_eq!(
        daemon
            .raw(json!({"op":"set","id":id,"values":{"seed_ratio":-1.0}}))
            .await["code"],
        2
    );
    assert_eq!(
        daemon.call(json!({"op":"get","id":id})).await["settings"]["seed_ratio"],
        0.0
    );
    let result = tokio::process::Command::new(BIN)
        .args(["ctl", "--socket"])
        .arg(&daemon.socket)
        .args([
            "set",
            &id,
            "download_limit=100",
            "upload_limit=200",
            "seed_ratio=0",
            "seed_time=1",
            "file_priorities=[\"high\"]",
            "trackers=false",
            "dht=false",
            "pex=false",
            "web_seeds=true",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let row = daemon.call(json!({"op":"get","id":id})).await;
    assert_eq!(row["settings"]["download_limit"], 100);
    assert_eq!(row["settings"]["file_priorities"], json!(["high"]));
    daemon.state(&id, "seeding").await;
    daemon.state(&id, "paused").await;
    let files = daemon.call(json!({"op":"files","id":id})).await;
    assert_eq!(files["files"][0]["priority"], "high");
    assert_eq!(
        daemon
            .raw(json!({"op":"set","id":id,"values":{"unknown_key":true}}))
            .await["code"],
        2
    );
    let output = tokio::process::Command::new(BIN)
        .args(["ctl", "--socket"])
        .arg(&daemon.socket)
        .args(["list", "--json"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["torrents"][0]["id"],
        id
    );
    daemon.stop().await;
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
    assert!(index.contains("Torrent settings"));
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
async fn padding_is_synthesized_across_pieces_and_never_materialized() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/test/a", b"abc"), ("/test/b", b"WXYZ")], false).await;
    let bytes = multifile_metainfo(
        &source.url,
        &[
            ("a", b"abc", false),
            ("pad", b"\0\0", true),
            ("empty", b"", false),
            ("b", b"WXYZ", false),
        ],
    );
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 9);
    let output = root.0.join("downloads/test");
    assert_eq!(std::fs::read(output.join("a")).unwrap(), b"abc");
    assert_eq!(std::fs::read(output.join("b")).unwrap(), b"WXYZ");
    assert_eq!(std::fs::metadata(output.join("empty")).unwrap().len(), 0);
    assert!(!output.join("pad").exists());
    let files = daemon.call(json!({"op":"files","id":id})).await;
    assert_eq!(files["files"][1]["priority"], "skip");
    assert!(
        source
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(p, _, _)| p == "/test/a" || p == "/test/b")
    );
    daemon.stop().await;
}

#[tokio::test]
async fn file_selection_fetches_boundary_bytes_but_not_skipped_pieces() {
    let root = Temp::new();
    let source = WebSeed::files(
        &[
            ("/test/a", b"abc"),
            ("/test/b", b"DEFGH"),
            ("/test/c", b"ijkl"),
        ],
        false,
    )
    .await;
    let bytes = multifile_metainfo(
        &source.url,
        &[
            ("a", b"abc", false),
            ("b", b"DEFGH", false),
            ("c", b"ijkl", false),
        ],
    );
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "downloading").await;
    daemon
        .call(json!({"op":"set","id":id,"values":{
            "file_priorities":["high","skip","skip"],"web_seeds":true
        }}))
        .await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(
        row["verified_bytes"], 4,
        "only the shared first piece is needed"
    );
    assert_eq!(
        std::fs::read(root.0.join("downloads/test/a")).unwrap(),
        b"abc"
    );
    {
        let requests = source.requests.lock().unwrap();
        assert!(requests.iter().any(|r| r == &("/test/a".into(), 0, 2)));
        assert!(requests.iter().any(|r| r == &("/test/b".into(), 0, 0)));
        assert_eq!(
            requests.len(),
            2,
            "no wholly skipped pieces should be fetched"
        );
    }
    // Expanding the selection must leave seeding and fetch the missing pieces.
    daemon
        .call(json!({"op":"set","id":id,"values":{
            "file_priorities":["normal","normal","normal"]
        }}))
        .await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 12);
    assert_eq!(
        std::fs::read(root.0.join("downloads/test/b")).unwrap(),
        b"DEFGH"
    );
    assert_eq!(
        std::fs::read(root.0.join("downloads/test/c")).unwrap(),
        b"ijkl"
    );
    daemon.stop().await;
}

#[tokio::test]
async fn corrupt_piece_is_retried_from_another_web_seed() {
    let root = Temp::new();
    // Both mirrors return valid ranges, but whichever is used first corrupts
    // its body. A single source cannot reconnect and conceal a missing retry.
    let source = WebSeed::files(&[("/bad", b"abc"), ("/good", b"abc")], true).await;
    let urls = [format!("{}bad", source.url), format!("{}good", source.url)];
    let mut bytes = b"d8:url-listl".to_vec();
    for url in &urls {
        bytes.extend_from_slice(format!("{}:{url}", url.len()).as_bytes());
    }
    bytes.push(b'e');
    bytes.extend_from_slice(&metainfo(None)[1..]);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 3);
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
    assert_eq!(source.hits.load(Ordering::Relaxed), 2);
    {
        let requests = source.requests.lock().unwrap();
        assert_ne!(
            requests[0].0, requests[1].0,
            "retry must use the other mirror"
        );
        assert!(
            requests
                .iter()
                .all(|(_, start, end)| (*start, *end) == (0, 2))
        );
    }
    daemon.stop().await;
}
