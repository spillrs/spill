use crate::payload::{Temp, seed_metainfo};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, Session},
        settings::TorrentSettings,
        torrent::{
            Destination, RunPolicy, TorrentExit, TorrentOptions, TorrentPhase, TorrentSource,
        },
    },
    protocol::magnet::Magnet,
};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn magnets_share_admission_inspection_download_and_network_only_recheck() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let source = Temp::new();
        std::fs::write(source.0.join("x"), b"abc").unwrap();
        let meta = Profile::default()
            .parse_owned(&seed_metainfo("x", b"abc", None))
            .unwrap();
        let seed = Session::bind(
            NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(),
                allow_local_network: true,
                ..Default::default()
            },
            Limits::default(),
        )
        .await
        .unwrap();
        seed.admit(TorrentOptions::new(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Adopt(source.0.clone()),
        ))
        .unwrap()
        .wait_complete()
        .await
        .unwrap();
        let address = seed.local_addr();
        let std::net::SocketAddr::V4(address) = address else {
            unreachable!()
        };
        let mut compact = address.ip().octets().to_vec();
        compact.extend_from_slice(&address.port().to_be_bytes());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/announce", listener.local_addr().unwrap());
        let exchanges = Arc::new(AtomicUsize::new(0));
        let hits = exchanges.clone();
        let (allow_metadata, allowed) = tokio::sync::oneshot::channel();
        let tracker = tokio::spawn(async move {
            let mut allowed = Some(allowed);
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                loop {
                    let mut line = String::new();
                    socket.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                }
                if let Some(allowed) = allowed.take() {
                    allowed.await.unwrap();
                }
                hits.fetch_add(1, Ordering::Relaxed);
                let mut body = b"d8:intervali60e5:peers6:".to_vec();
                body.extend_from_slice(&compact);
                body.push(b'e');
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                socket.write_all(&body).await.unwrap();
            }
        });
        let magnet = Magnet::parse(
            &format!("magnet:?xt=urn:btih:{}&tr={url}", meta.info_hash()),
            Default::default(),
        )
        .unwrap();
        let session = Session::bind(
            NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(),
                allow_local_network: true,
                ..Default::default()
            },
            Limits {
                torrents: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let settings = TorrentSettings {
            dht: false,
            pex: false,
            web_seeds: false,
            ..Default::default()
        };
        let inspector = session
            .inspect(magnet.clone(), Profile::default(), settings.clone())
            .unwrap();
        assert_eq!(session.torrents().len(), 1);
        assert!(
            session
                .inspect(magnet.clone(), Profile::default(), settings.clone())
                .is_err()
        );
        let other = Magnet::parse(
            "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            session
                .inspect(other, Profile::default(), settings.clone())
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            inspector.wait_complete().await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        allow_metadata.send(()).unwrap();
        assert_eq!(
            inspector.wait_metadata().await.unwrap().info_hash(),
            meta.info_hash()
        );
        let outcome = inspector.outcome().await;
        assert_eq!(outcome.termination, TorrentExit::MetadataResolved);
        assert!(outcome.cleanup_complete && !outcome.wanted_complete);
        assert!(inspector.recovery().is_none());
        assert!(session.torrents().is_empty());

        let failed_output = Temp::new();
        let (entered, entering) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, waiting) = std::sync::mpsc::channel();
        let waiting = Mutex::new(waiting);
        let mut options = TorrentOptions::new(
            TorrentSource::Magnet(magnet.clone()),
            Destination::Open(failed_output.0.clone()),
        );
        options.settings = settings.clone();
        options.persistence.resolved = Some(Box::new(move |_| {
            entered.lock().unwrap().take().unwrap().send(()).unwrap();
            waiting
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "resolved metadata denied",
            ))
        }));
        let failed = session.admit(options).unwrap();
        entering.await.unwrap();
        assert_eq!(failed.metainfo().unwrap().info_hash(), meta.info_hash());
        assert_eq!(std::fs::read_dir(&failed_output.0).unwrap().count(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), failed.stop())
                .await
                .is_err()
        );
        assert!(session.torrent(meta.info_hash()).is_some());
        release.send(()).unwrap();
        assert_eq!(
            failed.wait_metadata().await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        let outcome = failed.outcome().await;
        assert_eq!(outcome.termination, TorrentExit::Failed);
        assert!(outcome.cleanup_complete && failed.recovery().is_none());
        assert_eq!(std::fs::read_dir(&failed_output.0).unwrap().count(), 0);

        for recheck in [true, false] {
            let output = Temp::new();
            let mut options = TorrentOptions::new(
                TorrentSource::Magnet(magnet.clone()),
                Destination::Open(output.0.clone()),
            );
            options.settings = settings.clone();
            options.run = if recheck {
                RunPolicy::Recheck
            } else {
                RunPolicy::UntilComplete
            };
            let mut handle = session.admit(options).unwrap();
            inspector.request_stop();
            if recheck {
                let waiter = handle.clone();
                loop {
                    assert!(!matches!(
                        handle.snapshot().progress.phase,
                        TorrentPhase::Downloading | TorrentPhase::Seeding
                    ));
                    // changed() returns false once its final report is consumed;
                    // the retained outcome is ready by then.
                    tokio::select! {
                        biased;
                        outcome = waiter.outcome() => {
                            assert!(outcome.recheck_finished && outcome.cleanup_complete);
                            assert!(!outcome.wanted_complete && !outcome.full_complete);
                            break;
                        }
                        changed = handle.changed() => { assert!(changed); }
                    }
                }
                assert_eq!(handle.snapshot().progress.processed_payload_bytes, 0);
                assert!(!output.0.join("x").exists());
            } else {
                handle.wait_complete().await.unwrap();
                handle.join().await.unwrap();
                assert_eq!(handle.outcome().await.termination, TorrentExit::Complete);
                assert_eq!(std::fs::read(output.0.join("x")).unwrap(), b"abc");
            }
            assert!(handle.recovery().is_some());
            assert!(session.torrents().is_empty());
        }
        assert!(exchanges.load(Ordering::Relaxed) >= 3);
        session.shutdown().await.unwrap();
        seed.shutdown().await.unwrap();
        tracker.abort();
        let _ = tracker.await;
    })
    .await
    .expect("managed magnet lifecycle");
}

#[tokio::test]
async fn preparation_acknowledgements_survive_cancelled_stop_and_preserve_failure() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for mode in 0..3 {
            let fail = mode != 0;
            let output = Temp::new();
            let meta = Profile::default()
                .parse_owned(&seed_metainfo("x", b"abc", None))
                .unwrap();
            let session = Session::offline(Limits {
                torrents: 1,
                ..Default::default()
            });
            let (entered, entering) = tokio::sync::oneshot::channel();
            let entered = Mutex::new(Some(entered));
            let (release, waiting) = std::sync::mpsc::channel();
            let waiting = Mutex::new(waiting);
            let mut options = TorrentOptions::new(
                TorrentSource::Metainfo(meta.clone()),
                Destination::Create(output.0.clone()),
            );
            options.run = RunPolicy::Recheck;
            options.persistence.binding = Some(Box::new(move |_| {
                entered.lock().unwrap().take().unwrap().send(()).unwrap();
                waiting
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .expect("binding callback blocked async runtime");
                match mode {
                    0 => Ok(()),
                    1 => Err(io::Error::new(io::ErrorKind::Interrupted, "binding denied")),
                    _ => panic!("binding panic"),
                }
            }));
            let handle = session.admit(options).unwrap();
            entering.await.unwrap();
            assert_eq!(
                handle.snapshot().progress.phase,
                TorrentPhase::PreparingDestination
            );
            assert_eq!(
                std::fs::read_dir(&output.0).unwrap().count(),
                0,
                "payload creation preceded binding acknowledgement"
            );
            assert!(handle.recovery().is_some());
            assert!(
                tokio::time::timeout(Duration::from_millis(30), handle.stop())
                    .await
                    .is_err()
            );
            let mut duplicate = TorrentOptions::new(
                TorrentSource::Metainfo(meta.clone()),
                Destination::Open(output.0.clone()),
            );
            duplicate.run = RunPolicy::Recheck;
            assert!(session.admit(duplicate).is_err());
            release.send(()).unwrap();
            let outcome = handle.outcome().await;
            assert!(outcome.cleanup_complete && !outcome.wanted_complete);
            assert_eq!(
                outcome.termination,
                if fail {
                    TorrentExit::Failed
                } else {
                    TorrentExit::StopRequested
                }
            );
            assert_eq!(
                outcome.error.as_ref().map(|e| e.kind()),
                if mode == 0 {
                    None
                } else if mode == 1 {
                    Some(io::ErrorKind::Interrupted)
                } else {
                    Some(io::ErrorKind::Other)
                }
            );
            let mut options = TorrentOptions::new(
                TorrentSource::Metainfo(meta),
                Destination::Recover {
                    root: output.0.clone(),
                    state: handle.recovery().unwrap(),
                },
            );
            options.run = RunPolicy::Recheck;
            let replacement = session.admit(options).unwrap();
            replacement.join().await.unwrap();
            assert!(replacement.outcome().await.recheck_finished);
            session.shutdown().await.unwrap();
        }
        let output = Temp::new();
        std::fs::write(output.0.join("x"), b"bad").unwrap();
        let meta = Profile::default()
            .parse_owned(&seed_metainfo("x", b"abc", None))
            .unwrap();
        let session = Session::offline(Limits::default());
        for destination in [
            Destination::Create(output.0.clone()),
            Destination::Adopt(output.0.clone()),
            Destination::Open(output.0.join("missing")),
        ] {
            let mut options =
                TorrentOptions::new(TorrentSource::Metainfo(meta.clone()), destination);
            options.run = RunPolicy::Recheck;
            let handle = session.admit(options).unwrap();
            let outcome = handle.outcome().await;
            assert_eq!(outcome.termination, TorrentExit::Failed);
            assert!(outcome.cleanup_complete && handle.recovery().is_none());
            assert_eq!(std::fs::read(output.0.join("x")).unwrap(), b"bad");
        }
        session.shutdown().await.unwrap();
    })
    .await
    .expect("joined preparation and binding acknowledgement");
}

#[tokio::test]
async fn offline_magnets_and_disabled_discovery_never_prepare_payload() {
    let output = Temp::new();
    let magnet = Magnet::parse(
        "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
        Default::default(),
    )
    .unwrap();
    let offline = Session::offline(Limits::default());
    let mut options = TorrentOptions::new(
        TorrentSource::Magnet(magnet.clone()),
        Destination::Open(output.0.clone()),
    );
    options.run = RunPolicy::Recheck;
    assert_eq!(
        offline.admit(options).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(offline.torrents().is_empty());
    offline.shutdown().await.unwrap();
    let online = Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap();
    let mut options = TorrentOptions::new(
        TorrentSource::Magnet(magnet),
        Destination::Open(output.0.clone()),
    );
    options.settings.trackers = false;
    options.settings.dht = false;
    let handle = online.admit(options).unwrap();
    let outcome = handle.outcome().await;
    assert_eq!(outcome.termination, TorrentExit::Failed);
    assert!(outcome.cleanup_complete && handle.recovery().is_none());
    assert!(handle.wait_metadata().await.is_err());
    assert_eq!(std::fs::read_dir(&output.0).unwrap().count(), 0);
    online.shutdown().await.unwrap();
}
