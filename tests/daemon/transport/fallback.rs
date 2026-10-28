use super::{transport_daemon, utp_tracker, wait_transport};
use crate::common::{Temp, seed_metainfo};
use serde_json::json;
use std::time::Duration;

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
