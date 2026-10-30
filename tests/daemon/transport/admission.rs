use super::{transport_daemon, utp_tracker_gated, wait_transport};
use crate::common::{Daemon, raw_peer};
use crate::payload::{Temp, seed_metainfo};
use serde_json::json;
use std::time::Duration;

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
