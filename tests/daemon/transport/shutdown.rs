use super::{transport_daemon, wait_transport};
use crate::common::{Temp, seed_metainfo};
use serde_json::json;
use std::time::Duration;

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
