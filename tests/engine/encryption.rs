use crate::payload::{Temp, seed_metainfo};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, PeerEncryption, Session},
        torrent::{Destination, TorrentHandle, TorrentOptions, TorrentSource},
    },
    protocol::{
        magnet::Magnet,
        peer::{
            Handshake,
            mse::{Methods, Negotiation, torrent_key},
        },
    },
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn session(utp: bool, peer_encryption: PeerEncryption) -> Session {
    Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            allow_local_network: true,
            utp,
            peer_encryption,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap()
}

fn options(
    meta: spill::protocol::metainfo::OwnedMetainfo,
    destination: Destination,
) -> TorrentOptions {
    let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta), destination);
    options.diagnostics = true;
    options.settings.dht = false;
    options.settings.pex = false;
    options.settings.trackers = false;
    options.settings.web_seeds = false;
    options
}

async fn established(handle: &mut TorrentHandle, encryption: &str, transport: &str) {
    loop {
        if handle.snapshot().diagnostics.as_ref().is_some_and(|d| {
            d.peers.iter().any(|p| {
                p.detail.established && p.encryption == Some(encryption) && p.transport == transport
            })
        }) {
            return;
        }
        assert!(
            handle.changed().await,
            "torrent ended before the expected peer connected"
        );
    }
}

#[tokio::test]
async fn encrypted_transfers_cover_policies_transports_and_plaintext_to_mse_retry() {
    use PeerEncryption::{Allowed, Preferred, Required};
    tokio::time::timeout(Duration::from_secs(60), async {
        for (utp, seed_policy, leech_policy) in [
            (false, Required, Allowed),
            (true, Required, Preferred),
            (false, Preferred, Required),
        ] {
            let source = Temp::new();
            let destination = Temp::new();
            let payload = vec![71; 256 * 1024];
            std::fs::write(source.0.join("x"), &payload).unwrap();
            let meta = Profile::default()
                .parse_owned(&seed_metainfo("x", &payload, None))
                .unwrap();
            let seed = session(utp, seed_policy).await;
            let leech = session(utp, leech_policy).await;
            let seed_options = options(meta.clone(), Destination::Adopt(source.0.clone()));
            let mut leech_options = options(meta, Destination::Open(destination.0.clone()));
            // Keep the connection alive across the two-second diagnostic interval.
            leech_options.settings.download_limit = 32 * 1024;
            let mut seed_handle = seed.admit(seed_options).unwrap();
            seed_handle.wait_complete().await.unwrap();
            leech_options.peers.push(seed.local_addr());
            let mut leech_handle = leech.admit(leech_options).unwrap();
            let transport = if utp { "utp" } else { "tcp" };
            tokio::join!(
                established(&mut seed_handle, "rc4", transport),
                established(&mut leech_handle, "rc4", transport),
            );
            leech_handle.wait_complete().await.unwrap();
            assert_eq!(std::fs::read(destination.0.join("x")).unwrap(), payload);
            seed.shutdown().await.unwrap();
            leech.shutdown().await.unwrap();
        }
    })
    .await
    .expect("encrypted transfers and plaintext retry");
}

#[tokio::test]
async fn preferred_retries_only_before_mse_completes_and_required_rejects_plaintext() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let meta = Profile::default()
            .parse_owned(&seed_metainfo("x", b"abc", None))
            .unwrap();
        let wire_key = meta.info_hash().wire_key();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut mse, _) = listener.accept().await.unwrap();
            let mut public = [0; 96];
            mse.read_exact(&mut public).await.unwrap();
            assert_ne!(&public[..20], b"\x13BitTorrent protocol");
            drop(mse);
            let (mut plain, _) = listener.accept().await.unwrap();
            let mut hello = [0; 68];
            plain.read_exact(&mut hello).await.unwrap();
            assert_eq!(Handshake::decode(&hello).unwrap().0.info_hash, wire_key);
            plain
                .write_all(
                    &Handshake {
                        reserved: [0; 8],
                        info_hash: wire_key,
                        peer_id: [91; 20],
                    }
                    .encode(),
                )
                .await
                .unwrap();
            let mut bytes = [0; 4096];
            while plain.read(&mut bytes).await.unwrap_or(0) != 0 {}
        });
        let root = Temp::new();
        let preferred = session(false, NetworkOptions::default().peer_encryption).await;
        let mut request = options(meta.clone(), Destination::Open(root.0.clone()));
        request.peers.push(address);
        let mut handle = preferred.admit(request).unwrap();
        established(&mut handle, "plaintext", "tcp").await;
        preferred.shutdown().await.unwrap();
        peer.await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut codec = Negotiation::responder([93; 20], Methods::Rc4, &[]);
            let mut bytes = [0; 4096];
            loop {
                let count = socket.read(&mut bytes).await.unwrap();
                assert_ne!(count, 0);
                let progress = codec
                    .receive(&bytes[..count], |key| {
                        (key == torrent_key(wire_key)).then_some(wire_key)
                    })
                    .unwrap();
                socket.write_all(&progress.output).await.unwrap();
                if let Some(negotiated) = progress.negotiated {
                    assert_eq!(
                        Handshake::decode(&negotiated.initial).unwrap().0.info_hash,
                        wire_key
                    );
                    break;
                }
            }
            drop(socket);
            assert!(
                tokio::time::timeout(Duration::from_secs(2), listener.accept())
                    .await
                    .is_err(),
                "successful MSE must not be retried in plaintext"
            );
        });
        let root = Temp::new();
        let preferred = session(false, PeerEncryption::Preferred).await;
        let mut request = options(meta.clone(), Destination::Open(root.0.clone()));
        request.peers.push(address);
        let _handle = preferred.admit(request).unwrap();
        peer.await.unwrap();
        preferred.shutdown().await.unwrap();

        let required = session(false, PeerEncryption::Required).await;
        let source = Temp::new();
        std::fs::write(source.0.join("x"), b"abc").unwrap();
        let seed = required
            .admit(options(meta, Destination::Adopt(source.0.clone())))
            .unwrap();
        seed.wait_complete().await.unwrap();
        let mut socket = TcpStream::connect(required.local_addr()).await.unwrap();
        socket
            .write_all(
                &Handshake {
                    reserved: [0; 8],
                    info_hash: wire_key,
                    peer_id: [92; 20],
                }
                .encode(),
            )
            .await
            .unwrap();
        let mut byte = [0];
        match socket.read(&mut byte).await {
            Ok(count) => assert_eq!(count, 0),
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
        }
        required.shutdown().await.unwrap();
    })
    .await
    .expect("plaintext fallback and downgrade rejection");
}

#[tokio::test]
async fn encrypted_v2_magnet_routes_to_a_hybrid_seed() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for utp in [false, true] {
            let meta = Profile::default()
                .parse_owned(include_bytes!(
                    "../protocol/fixtures/libtorrent-hybrid.torrent"
                ))
                .unwrap();
            let source = Temp::new();
            let destination = Temp::new();
            std::fs::create_dir(source.0.join("vectors")).unwrap();
            for (name, length) in ["block", "empty", "odd", "one", "piece", "tail"]
                .into_iter()
                .zip([16384, 0, 65659, 1, 32768, 123])
            {
                std::fs::write(
                    source.0.join("vectors").join(name),
                    crate::v2_peer::payload(0, length),
                )
                .unwrap();
            }
            let seed = session(utp, PeerEncryption::Required).await;
            let seed_handle = seed
                .admit(options(meta.clone(), Destination::Adopt(source.0.clone())))
                .unwrap();
            seed_handle.wait_complete().await.unwrap();
            let (url, tracker) = crate::metadata_peer::tracker(&[seed.local_addr()]).await;
            let magnet = Magnet::parse(
                &format!(
                    "magnet:?xt=urn:btmh:1220{}&tr={url}",
                    meta.info_hash().v2().unwrap()
                ),
                Default::default(),
            )
            .unwrap();
            let leech = session(utp, PeerEncryption::Required).await;
            let mut request = options(meta.clone(), Destination::Open(destination.0.clone()));
            request.source = TorrentSource::Magnet(magnet);
            request.settings.trackers = true;
            request.settings.download_limit = 16 * 1024;
            let mut handle = leech.admit(request).unwrap();
            let resolved = handle.wait_metadata().await.unwrap();
            assert_eq!(resolved.info_hash(), meta.info_hash());
            established(&mut handle, "rc4", if utp { "utp" } else { "tcp" }).await;
            handle.wait_complete().await.unwrap();
            for name in ["block", "empty", "odd", "one", "piece", "tail"] {
                assert_eq!(
                    std::fs::read(destination.0.join("vectors").join(name)).unwrap(),
                    std::fs::read(source.0.join("vectors").join(name)).unwrap()
                );
            }
            leech.shutdown().await.unwrap();
            seed.shutdown().await.unwrap();
            tracker.abort();
            assert!(tracker.await.unwrap_err().is_cancelled());
        }
    })
    .await
    .expect("encrypted metadata acquisition and hybrid v2 routing");
}
