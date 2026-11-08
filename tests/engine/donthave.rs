use crate::{
    payload::{Temp, seed_metainfo},
    v2_peer as raw,
};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, Session},
        torrent::{Destination, TorrentOptions, TorrentSource},
    },
    protocol::peer::extension,
};
use std::time::Duration;
use tokio::net::TcpStream;

async fn peer(session: &Session, key: [u8; 20], identity: u8, fast: bool) -> (TcpStream, u8) {
    let mut socket = TcpStream::connect(session.local_addr()).await.unwrap();
    raw::initiate(&mut socket, key, identity, fast).await;
    let hello = raw::next(&mut socket, 20).await;
    assert_eq!(hello[1], 0);
    let hello = extension::Handshake::parse(&hello[2..], Default::default()).unwrap();
    let id = hello.extension_id(b"lt_donthave").unwrap();
    assert_ne!(id, 0);
    (socket, id)
}

async fn setup() -> (
    Temp,
    Session,
    spill::engine::torrent::TorrentHandle,
    [u8; 20],
) {
    let root = Temp::new();
    let meta = Profile::default()
        .parse_owned(&seed_metainfo("x", b"abc", None))
        .unwrap();
    let key = meta.info_hash().wire_key();
    let session = Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            utp: false,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap();
    let mut options = TorrentOptions::new(
        TorrentSource::Metainfo(meta),
        Destination::Open(root.0.clone()),
    );
    options.settings.trackers = false;
    options.settings.dht = false;
    options.settings.pex = false;
    options.settings.web_seeds = false;
    options.diagnostics = true;
    let mut handle = session.admit(options).unwrap();
    while !handle.snapshot().progress.ready {
        assert!(handle.changed().await);
    }
    (root, session, handle, key)
}

#[tokio::test]
async fn withdrawal_releases_legacy_requests_but_fast_waits_for_reject() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for fast in [false, true] {
            // The remote mapping differs from ours, then disables its receive ID.
            // Neither update disables its ability to send to our receive ID.
            for negotiation in [b"\0d1:mdee".as_slice(), b"\0d1:md11:lt_donthavei9eee"] {
                let (root, session, mut handle, key) = setup().await;
                let (mut a, id) = peer(&session, key, 81, fast).await;
                raw::send(&mut a, 20, negotiation).await;
                raw::send(&mut a, 5, &[0x80]).await;
                raw::send(&mut a, 1, &[]).await;
                let request = raw::next(&mut a, 6).await;
                assert_eq!(&request[1..], raw::request(0, 0, 3));
                raw::send(&mut a, 20, b"\0d1:md11:lt_donthavei0eee").await;
                let mut withdrawal = vec![id];
                withdrawal.extend_from_slice(&0u32.to_be_bytes());
                raw::send(&mut a, 20, &withdrawal).await;
                raw::send(&mut a, 20, &withdrawal).await; // Duplicate withdrawal is harmless.
                loop {
                    let frame = raw::frame(&mut a).await.unwrap();
                    assert_ne!(frame.first(), Some(&8), "withdrawal must not send a cancel");
                    if frame == [3] {
                        break;
                    }
                }
                loop {
                    let snapshot = handle.snapshot();
                    if let Some(peer) = snapshot.diagnostics.as_ref().unwrap().peers.first()
                        && !peer.detail.has_needed_pieces
                        && peer.detail.authorized_requests == usize::from(fast)
                    {
                        assert_eq!(peer.detail.assigned_requests, usize::from(fast));
                        break;
                    }
                    assert!(handle.changed().await);
                }
                let (mut b, _) = peer(&session, key, 82, false).await;
                raw::send(&mut b, 5, &[0x80]).await;
                if fast {
                    raw::send(&mut a, 16, &request[1..]).await;
                    loop {
                        let snapshot = handle.snapshot();
                        if snapshot
                            .diagnostics
                            .as_ref()
                            .unwrap()
                            .peers
                            .iter()
                            .any(|peer| {
                                peer.endpoint == Some(a.local_addr().unwrap())
                                    && peer.detail.authorized_requests == 0
                            })
                        {
                            break;
                        }
                        assert!(handle.changed().await);
                    }
                }
                raw::send(&mut b, 1, &[]).await;
                let reassigned = raw::next(&mut b, 6).await;
                assert_eq!(reassigned, request);
                let mut block = request[1..9].to_vec();
                block.extend_from_slice(b"abc");
                raw::send(&mut b, 7, &block).await;
                handle.wait_complete().await.unwrap();
                assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
                handle.stop().await.unwrap();
                session.shutdown().await.unwrap();
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_withdrawals_disconnect_only_the_sender() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (_root, session, handle, key) = setup().await;
        for (identity, payload) in [
            vec![],
            vec![0; 3],
            vec![0; 5],
            1u32.to_be_bytes().to_vec(),
            u32::MAX.to_be_bytes().to_vec(),
        ]
        .into_iter()
        .enumerate()
        {
            let (mut socket, id) = peer(&session, key, 90 + identity as u8, false).await;
            let mut bytes = vec![id];
            bytes.extend_from_slice(&payload);
            raw::send(&mut socket, 20, &bytes).await;
            while raw::frame(&mut socket).await.is_ok() {}
            assert!(handle.snapshot().progress.exit.is_none());
        }
        handle.stop().await.unwrap();
        session.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
