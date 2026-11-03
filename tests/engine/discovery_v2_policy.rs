use crate::payload::Temp;
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, Session},
        settings::TorrentSettings,
        torrent::{Destination, TorrentOptions, TorrentSource},
    },
    protocol::bencode::{self, Value},
};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const HYBRID: &[u8] = include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent");

async fn peer(address: SocketAddr, key: [u8; 20], id: u8, port: u16) -> TcpStream {
    let mut socket = TcpStream::connect(address).await.unwrap();
    let mut hello = b"\x13BitTorrent protocol".to_vec();
    hello.extend_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
    hello.extend_from_slice(&key);
    hello.extend_from_slice(&[id; 20]);
    socket.write_all(&hello).await.unwrap();
    let mut reply = [0; 68];
    socket.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[28..48], &key);
    crate::v2_peer::send(
        &mut socket,
        20,
        format!("\0d1:md6:ut_pexi2ee1:pi{port}ee").as_bytes(),
    )
    .await;
    socket
}
async fn frame(socket: &mut TcpStream) -> Vec<u8> {
    let length = socket.read_u32().await.unwrap();
    assert!(length < 1024 * 1024);
    let mut bytes = vec![0; length as usize];
    socket.read_exact(&mut bytes).await.unwrap();
    bytes
}

#[tokio::test]
async fn hybrid_pex_shares_legacy_and_v2_contacts_with_recipients_in_either_mode() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let meta = Profile::default().parse_owned(HYBRID).unwrap();
        let v1 = *meta.info_hash().v1().unwrap().as_bytes();
        let v2 = meta.info_hash().v2().unwrap().wire_key();
        for recipient_key in [v1, v2] {
            let session = Session::bind(
                NetworkOptions {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    allow_local_network: true,
                    utp: false,
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            let root = Temp::new();
            let mut options = TorrentOptions::new(
                TorrentSource::Metainfo(meta.clone()),
                Destination::Open(root.0.clone()),
            );
            options.settings = TorrentSettings {
                trackers: false,
                dht: false,
                web_seeds: false,
                ..Default::default()
            };
            options.diagnostics = true;
            let mut handle = session.admit(options).unwrap();
            while !handle.snapshot().progress.ready {
                assert!(handle.changed().await);
            }
            let mut first = peer(session.local_addr(), v1, 91, 34001).await;
            let mut second = peer(session.local_addr(), v2, 92, 34002).await;
            // Consume their extension handshakes before attaching the recipient.
            for socket in [&mut first, &mut second] {
                loop {
                    if frame(socket).await.starts_with(&[20, 0]) {
                        break;
                    }
                }
            }
            let mut recipient = peer(session.local_addr(), recipient_key, 93, 34003).await;
            loop {
                let bytes = frame(&mut recipient).await;
                if !bytes.starts_with(&[20, 2]) {
                    continue;
                }
                let doc = bencode::decode(&bytes[2..], Default::default()).unwrap();
                let Value::Bytes(added) = doc.get(b"added").unwrap().value() else {
                    unreachable!()
                };
                let mut ports: Vec<_> = added
                    .as_chunks::<6>()
                    .0
                    .iter()
                    .map(|p| u16::from_be_bytes(p[4..6].try_into().unwrap()))
                    .collect();
                ports.sort_unstable();
                assert_eq!(ports, [34001, 34002]);
                break;
            }
            handle.stop().await.unwrap();
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("hybrid PEX shares contacts across negotiated modes");
}
