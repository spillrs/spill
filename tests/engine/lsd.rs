use crate::payload::{Temp, seed_metainfo};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, PeerEncryption, Session},
        settings::TorrentSettings,
        torrent::{Destination, TorrentOptions, TorrentSource},
    },
    protocol::magnet::Magnet,
};
use std::{io, time::Duration};

async fn session() -> io::Result<Session> {
    Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            allow_local_network: true,
            lsd: true,
            utp: false,
            peer_encryption: PeerEncryption::Allowed,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
}

#[tokio::test]
async fn multicast_bootstraps_a_trackerless_magnet_and_transfers_payload() {
    let mut recovered = None;
    // A non-sharing owner of 6771 must not prevent ordinary session use or cleanup.
    if let Ok(blocker) = std::net::UdpSocket::bind("0.0.0.0:6771") {
        let degraded = session().await.unwrap();
        let status = degraded.network_status().await;
        assert!(status.tcp_listen.is_some() && status.udp_listen.is_some());
        assert!(status.lsd.enabled && !status.lsd.active && status.lsd.last_error.is_some());
        drop(blocker);
        tokio::task::yield_now().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
        recovered = Some(
            tokio::time::timeout(Duration::from_secs(2), async {
                while !degraded.network_status().await.lsd.active {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .is_ok(),
        );
        degraded.shutdown().await.unwrap();
    } else {
        eprintln!("skipping LSD recovery check: port 6771 already in use");
    }
    let download = session().await.expect("peer listeners");
    let status = download.network_status().await.lsd;
    if !status.active {
        eprintln!(
            "skipping LSD: loopback multicast unavailable: {:?}",
            status.last_error
        );
        download.shutdown().await.unwrap();
        return;
    }
    assert_ne!(
        recovered,
        Some(false),
        "LSD retries after the port becomes available"
    );
    // A second session must share 6771 while the first already owns it.
    let seed = session().await.expect("peer listeners");
    assert!(seed.network_status().await.lsd.active, "shared LSD port");
    tokio::time::timeout(Duration::from_secs(20), async {
        let source = Temp::new();
        let destination = Temp::new();
        let payload = vec![42; 128 * 1024];
        std::fs::write(source.0.join("x"), &payload).unwrap();
        let meta = Profile::default()
            .parse_owned(&seed_metainfo("x", &payload, None))
            .unwrap();
        let settings = TorrentSettings {
            trackers: false,
            dht: true,
            pex: false,
            web_seeds: false,
            ..Default::default()
        };
        // Consume the session's regular announce budget before the real magnet.
        let warmup = download
            .inspect(
                Magnet::parse(
                    "magnet:?xt=urn:btih:1111111111111111111111111111111111111111",
                    Default::default(),
                )
                .unwrap(),
                Profile::default(),
                settings.clone(),
            )
            .unwrap();
        while download.network_status().await.lsd.announces == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        warmup.stop().await.unwrap();
        let magnet = Magnet::parse(
            &format!("magnet:?xt=urn:btih:{}", meta.info_hash()),
            Default::default(),
        )
        .unwrap();
        let mut options = TorrentOptions::new(
            TorrentSource::Magnet(magnet),
            Destination::Create(destination.0.clone()),
        );
        options.settings = settings.clone();
        options.diagnostics = true;
        let handle = download.admit(options).unwrap();
        // A sent announce proves registration, unlike the earlier phase transition.
        while download.network_status().await.lsd.announces < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let mut options = TorrentOptions::new(
            TorrentSource::Metainfo(meta),
            Destination::Open(source.0.clone()),
        );
        options.settings = settings;
        let seeding = seed.admit(options).unwrap();
        seeding.wait_complete().await.unwrap();
        handle.wait_complete().await.unwrap();
        assert_eq!(std::fs::read(destination.0.join("x")).unwrap(), payload);
        let diagnostics = handle.snapshot().diagnostics.unwrap();
        assert!(diagnostics.peers.iter().any(|peer| peer.source == "lsd"));
        assert!(
            diagnostics
                .peers
                .iter()
                .all(|peer| peer.endpoint != Some(download.local_addr()))
        );
        download.shutdown().await.unwrap();
        seed.shutdown().await.unwrap();
    })
    .await
    .expect("LSD magnet bootstrap and transfer");
}
