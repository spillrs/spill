use crate::payload::{Temp, seed_metainfo};
use spill::{
    engine::{
        profile::Profile,
        session::{DhtOptions, Limits, NetworkOptions, Session, dht::SavedState},
        settings::TorrentSettings,
        torrent::{Destination, RunPolicy, TorrentOptions, TorrentPhase, TorrentSource},
    },
    protocol::{
        dht::{NodeId, krpc::NodeV4},
        magnet::Magnet,
    },
};
use std::{io, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dht_is_lazy_respects_execution_policy_and_retains_final_state() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for capable in [false, true] {
            let id = NodeId([7; 20]);
            let session = Session::bind(
                NetworkOptions {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    dht: capable.then_some(DhtOptions {
                        saved: Some(SavedState {
                            id,
                            nodes: Vec::new(),
                        }),
                        routers: Vec::new(),
                    }),
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            assert!(session.dht_state().await.unwrap().is_none());
            let root = Temp::new();
            std::fs::write(root.0.join("x"), b"abc").unwrap();
            let profile = Profile::default();
            for (private, run, enabled) in [
                (false, RunPolicy::Recheck, true),
                (true, RunPolicy::Persistent, true),
                (false, RunPolicy::Persistent, false),
                (false, RunPolicy::Persistent, true),
            ] {
                let mut bytes = seed_metainfo("x", b"abc", None);
                if private {
                    bytes.truncate(bytes.len() - 2);
                    bytes.extend_from_slice(b"7:privatei1eee");
                }
                let meta = profile.parse_owned(&bytes).unwrap();
                let mut options = TorrentOptions::new(
                    TorrentSource::Metainfo(meta),
                    Destination::Open(root.0.clone()),
                );
                options.profile = profile;
                options.settings = TorrentSettings {
                    dht: enabled,
                    trackers: false,
                    pex: false,
                    web_seeds: false,
                    ..Default::default()
                };
                options.run = run;
                let handle = session.admit(options).unwrap();
                handle.wait_complete().await.unwrap();
                let state = session.dht_state().await.unwrap();
                assert_eq!(
                    state.is_some(),
                    capable && enabled && !private && run != RunPolicy::Recheck
                );
                if let Some(state) = state {
                    assert_eq!(state.id, id);
                    assert!(state.nodes.is_empty());
                }
                handle.stop().await.unwrap();
                assert!(session.torrent(handle.info_hash()).is_none());
            }
            let (first, second) = tokio::join!(session.shutdown(), session.shutdown());
            first.unwrap();
            second.unwrap();
            let state = session.dht_state().await.unwrap();
            assert_eq!(state.is_some(), capable);
            if let Some(state) = state {
                assert_eq!(state.id, id);
            }
        }
    })
    .await
    .expect("lazy session DHT");
}

#[tokio::test]
async fn cancelled_dht_initialization_waits_remain_owned_through_shutdown() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let session = Session::bind(
            NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(),
                dht: Some(DhtOptions {
                    saved: None,
                    routers: Vec::new(),
                }),
                ..Default::default()
            },
            Limits::default(),
        )
        .await
        .unwrap();
        let magnet = Magnet::parse(
            "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
            Default::default(),
        )
        .unwrap();
        let mut handle = session
            .inspect(
                magnet.clone(),
                Profile::default(),
                TorrentSettings {
                    trackers: false,
                    ..Default::default()
                },
            )
            .unwrap();
        while handle.snapshot().progress.phase != TorrentPhase::ResolvingMetadata {
            assert!(handle.changed().await);
        }
        let mut resolve = Box::pin(handle.wait_metadata());
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(resolve.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(resolve);
        let mut snapshot = Box::pin(session.dht_state());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(snapshot.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        drop(snapshot);
        session.shutdown().await.unwrap();
        assert!(handle.outcome().await.cleanup_complete);
        assert_eq!(
            handle.wait_metadata().await.unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert!(
            session
                .inspect(magnet, Profile::default(), Default::default())
                .is_err()
        );
        if let Ok(Some(state)) = session.dht_state().await {
            assert!(state.nodes.is_empty());
        }
        session.shutdown().await.unwrap();
    })
    .await
    .expect("cancelled DHT waits and joined shutdown");
}

#[tokio::test]
async fn invalid_dht_capabilities_are_rejected_before_binding() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    for (listen, routers, saved) in [
        (
            listener.local_addr().unwrap(),
            vec!["invalid".to_owned()],
            None,
        ),
        ("[::1]:0".parse().unwrap(), Vec::new(), None),
        (
            listener.local_addr().unwrap(),
            Vec::new(),
            Some(SavedState {
                id: NodeId([0; 20]),
                nodes: vec![NodeV4 {
                    id: NodeId([1; 20]),
                    address: "224.0.0.1:6881".parse().unwrap(),
                }],
            }),
        ),
    ] {
        let error = Session::bind(
            NetworkOptions {
                listen,
                dht: Some(DhtOptions { saved, routers }),
                ..Default::default()
            },
            Limits::default(),
        )
        .await
        .err()
        .expect("invalid DHT capability");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
