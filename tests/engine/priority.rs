use crate::{
    metadata_peer,
    payload::{Temp, seed_metainfo},
    v2_peer as raw,
};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, PeerEncryption, Session},
        settings::TorrentSettings,
        torrent::{Destination, TorrentOptions, TorrentPhase, TorrentSource},
    },
    protocol::{magnet::Magnet, peer::priority::priority},
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
};

async fn session() -> Session {
    Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            allow_local_network: true,
            utp: false,
            peer_encryption: PeerEncryption::Allowed,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap()
}

fn options(source: TorrentSource, root: &Temp) -> TorrentOptions {
    let mut options = TorrentOptions::new(source, Destination::Open(root.0.clone()));
    options.profile.peers = 1;
    options.settings = TorrentSettings {
        trackers: false,
        dht: false,
        pex: false,
        web_seeds: false,
        ..Default::default()
    };
    options
}

async fn closed(peer: &mut TcpStream) {
    let mut bytes = Vec::new();
    if let Err(error) = peer.read_to_end(&mut bytes).await {
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
        ));
    }
}

async fn introduce(peer: &mut TcpStream, key: [u8; 20], identity: u8) {
    peer.write_all(
        &spill::protocol::peer::Handshake {
            reserved: [0, 0, 0, 0, 0, 0x10, 0, 0],
            info_hash: key,
            peer_id: [identity; 20],
        }
        .encode(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn canonical_admission_replaces_payload_peers_but_preserves_metadata_sources() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for magnet in [false, true] {
            let root = Temp::new();
            let (url, tracker) = metadata_peer::tracker(&[]).await;
            let meta = Profile::default()
                .parse_owned(&seed_metainfo("x", b"abc", Some(&url)))
                .unwrap();
            let key = meta.info_hash().wire_key();
            let info = meta.info_bytes().to_vec();
            let source = if magnet {
                TorrentSource::Magnet(
                    Magnet::parse(
                        &format!("magnet:?xt=urn:btih:{}&tr={url}", meta.info_hash()),
                        Default::default(),
                    )
                    .unwrap(),
                )
            } else {
                TorrentSource::Metainfo(meta)
            };
            let session = session().await;
            let mut options = options(source, &root);
            options.settings.trackers = magnet;
            let mut handle = session.admit(options).unwrap();
            while if magnet {
                handle.snapshot().progress.phase != TorrentPhase::ResolvingMetadata
            } else {
                !handle.snapshot().progress.ready
            } {
                assert!(handle.changed().await);
            }
            let address = session.local_addr();
            let mut sockets: Vec<_> = (0..3)
                .map(|_| {
                    let socket = TcpSocket::new_v4().unwrap();
                    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
                    socket
                })
                .collect();
            sockets.sort_by_key(|socket| priority(address, socket.local_addr().unwrap()));
            let high = sockets.pop().unwrap();
            let middle = sockets.pop().unwrap();
            let low = sockets.pop().unwrap();
            let mut low = low.connect(address).await.unwrap();
            raw::initiate(&mut low, key, 1, false).await;
            assert!(!raw::frame(&mut low).await.unwrap().is_empty());
            if !magnet {
                raw::send(&mut low, 5, &[0x80]).await;
                raw::send(&mut low, 1, &[]).await;
                assert_eq!(&raw::next(&mut low, 6).await[1..], raw::request(0, 0, 3));
            } else {
                raw::send(
                    &mut low,
                    20,
                    format!("\0d1:md11:ut_metadatai1ee13:metadata_sizei{}ee", info.len())
                        .as_bytes(),
                )
                .await;
                while !raw::frame(&mut low).await.unwrap().starts_with(&[20, 1]) {}
            }
            let mut high = high.connect(address).await.unwrap();
            if magnet {
                introduce(&mut high, key, 2).await;
                closed(&mut high).await;
            } else {
                raw::initiate(&mut high, key, 2, false).await;
                assert!(!raw::frame(&mut high).await.unwrap().is_empty());
                closed(&mut low).await;
            }
            let mut middle = middle.connect(address).await.unwrap();
            introduce(&mut middle, key, 3).await;
            closed(&mut middle).await;
            if magnet {
                let mut data =
                    format!("\x01d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len())
                        .into_bytes();
                data.extend_from_slice(&info);
                raw::send(&mut low, 20, &data).await;
                while !handle.snapshot().progress.ready {
                    assert!(handle.changed().await);
                }
            }
            let supplier = if magnet { &mut low } else { &mut high };
            raw::send(supplier, 5, &[0x80]).await;
            raw::send(supplier, 1, &[]).await;
            let request = raw::next(supplier, 6).await;
            assert_eq!(&request[1..], raw::request(0, 0, 3));
            let mut block = request[1..9].to_vec();
            block.extend_from_slice(b"abc");
            raw::send(supplier, 7, &block).await;
            handle.wait_complete().await.unwrap();
            assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
            handle.stop().await.unwrap();
            closed(&mut high).await;
            session.shutdown().await.unwrap();
            tracker.abort();
        }
    })
    .await
    .expect("bounded canonical peer replacement");
}

#[tokio::test]
async fn outgoing_candidates_prefer_higher_canonical_rank() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let session = session().await;
        let mut listeners = vec![
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ];
        listeners
            .sort_by_key(|listener| priority(session.local_addr(), listener.local_addr().unwrap()));
        let low = listeners.remove(0);
        let high = listeners.remove(0);
        let root = Temp::new();
        let meta = Profile::default()
            .parse_owned(&seed_metainfo("x", b"abc", None))
            .unwrap();
        let key = meta.info_hash().wire_key();
        let mut options = options(TorrentSource::Metainfo(meta), &root);
        options.peers = vec![low.local_addr().unwrap(), high.local_addr().unwrap()];
        let handle = session.admit(options).unwrap();
        let (mut peer, _) = high.accept().await.unwrap();
        raw::accept(&mut peer, key, 4).await;
        assert!(!raw::frame(&mut peer).await.unwrap().is_empty());
        handle.stop().await.unwrap();
        session.shutdown().await.unwrap();
    })
    .await
    .expect("canonical outgoing candidate order");
}
