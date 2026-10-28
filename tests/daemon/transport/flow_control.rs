use super::{transport_daemon, wait_transport};
use crate::common::{Temp, seed_metainfo};
use serde_json::json;
use std::time::Duration;

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
