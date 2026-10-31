use crate::payload::{Temp, seed_metainfo};
use spill::engine::{
    profile::Profile,
    session::{Limits, NetworkOptions, Session},
    settings::TorrentSettings,
    torrent::{Destination, RunPolicy, TorrentOptions, TorrentPhase, TorrentSource},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

#[tokio::test]
async fn retained_torrent_handles_release_session_ports_after_shutdown() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for utp in [false, true] {
            for run in [RunPolicy::Persistent, RunPolicy::UntilComplete] {
                let root = Temp::new();
                std::fs::write(root.0.join("x"), b"abc").unwrap();
                let meta = Profile::default()
                    .parse_owned(&seed_metainfo("x", b"abc", None))
                    .unwrap();
                let session = Session::bind(
                    NetworkOptions {
                        listen: "127.0.0.1:0".parse().unwrap(),
                        utp,
                        ..Default::default()
                    },
                    Limits::default(),
                )
                .await
                .unwrap();
                let address = session.local_addr();
                let mut options = TorrentOptions::new(
                    TorrentSource::Metainfo(meta),
                    Destination::Adopt(root.0.clone()),
                );
                options.run = run;
                let handle = session.admit(options).unwrap();
                handle.wait_complete().await.unwrap();
                if run == RunPolicy::Persistent {
                    handle.stop().await.unwrap();
                } else {
                    handle.join().await.unwrap();
                }
                let outcome = handle.outcome().await;
                assert!(outcome.wanted_complete && outcome.cleanup_complete);
                session.shutdown().await.unwrap();
                drop(session);

                let replacement = Session::bind(
                    NetworkOptions {
                        listen: address,
                        utp,
                        ..Default::default()
                    },
                    Limits::default(),
                )
                .await
                .expect("a retained finished torrent handle must not keep session ports bound");
                assert!(Arc::ptr_eq(&outcome, &handle.outcome().await));
                assert!(handle.recovery().is_some());
                replacement.shutdown().await.unwrap();
            }
        }
    })
    .await
    .expect("session port reuse with retained torrent outcomes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clones_retain_execution_and_cancelled_shutdown_remains_joinable() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let tracker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/announce", tracker.local_addr().unwrap());
        let (stopped, stopping) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let tracker = tokio::spawn(async move {
            let mut stopped = Some(stopped);
            let mut released = Some(released);
            loop {
                let (socket, _) = tracker.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                let stopping = line.contains("event=stopped");
                loop {
                    line.clear();
                    assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                if stopping {
                    stopped.take().unwrap().send(()).unwrap();
                    released.take().unwrap().await.unwrap();
                }
                let body = b"d8:intervali60e5:peers0:e";
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(body).await.unwrap();
                if stopping {
                    break;
                }
            }
        });
        let root = Temp::new();
        std::fs::write(root.0.join("x"), b"abc").unwrap();
        let profile = Profile::default();
        let meta = profile
            .parse_owned(&seed_metainfo("x", b"abc", Some(&url)))
            .unwrap();
        let session = Session::bind(
            NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(),
                allow_local_network: true,
                ..Default::default()
            },
            Limits::default(),
        )
        .await
        .unwrap();
        let sibling = session.clone();
        let mut options = TorrentOptions::new(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Open(root.0.clone()),
        );
        options.profile = profile;
        options.settings = TorrentSettings {
            dht: false,
            pex: false,
            web_seeds: false,
            ..Default::default()
        };
        options.diagnostics = true;
        let mut observer = session.admit(options).unwrap();
        let hash = observer.info_hash();
        assert!(sibling.torrent(hash).is_some());
        drop(session);
        loop {
            let status = observer.snapshot();
            if status
                .diagnostics
                .as_ref()
                .is_some_and(|d| d.trackers.iter().any(|t| t.state == "Success"))
            {
                break;
            }
            assert!(
                observer.changed().await,
                "dropping one session clone stopped the seed"
            );
        }
        let mut unfinished = TcpStream::connect(sibling.local_addr()).await.unwrap();
        unfinished.write_all(b"\x13Bit").await.unwrap();
        let session = sibling.clone();
        let shutdown = tokio::spawn(async move { session.shutdown().await });
        stopping.await.unwrap();
        match unfinished.read(&mut [0; 1]).await {
            Ok(count) => assert_eq!(count, 0, "unfinished handshake survived listener shutdown"),
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
        }
        shutdown.abort();
        assert!(shutdown.await.unwrap_err().is_cancelled());
        assert_eq!(
            sibling
                .admit(TorrentOptions::new(
                    TorrentSource::Metainfo(meta),
                    Destination::Open(root.0.clone())
                ))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_ne!(sibling.local_addr().port(), 0);
        assert_ne!(sibling.advertised_port(), 0);
        assert!(!sibling.port_mapping().enabled);
        release.send(()).unwrap();
        let session = sibling.clone();
        let (first, second) = tokio::join!(sibling.shutdown(), session.shutdown());
        first.unwrap();
        second.unwrap();
        assert!(observer.snapshot().progress.joined);
        assert!(sibling.torrent(hash).is_none());
        assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
        tracker.await.unwrap();
    })
    .await
    .expect("shared session shutdown");
}

#[tokio::test]
async fn stale_handle_cannot_stop_replacement_and_last_clone_stops_it() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let root = Temp::new();
        std::fs::write(root.0.join("x"), b"abc").unwrap();
        let profile = Profile::default();
        let meta = profile
            .parse_owned(&seed_metainfo("x", b"abc", None))
            .unwrap();
        let options = || {
            TorrentOptions::new(
                TorrentSource::Metainfo(meta.clone()),
                Destination::Open(root.0.clone()),
            )
        };
        let session = Session::bind(
            NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(),
                dht: Some(spill::engine::session::DhtOptions {
                    saved: None,
                    routers: Vec::new(),
                }),
                ..Default::default()
            },
            Limits::default(),
        )
        .await
        .unwrap();
        let old_observer = session.admit(options()).unwrap();
        let hash = old_observer.info_hash();
        old_observer.stop().await.unwrap();
        let stopped = old_observer.outcome().await;
        assert_eq!(
            stopped.termination,
            spill::engine::torrent::TorrentExit::StopRequested
        );
        assert!(!stopped.wanted_complete && stopped.cleanup_complete);
        assert_eq!(
            old_observer.wait_complete().await.unwrap_err().kind(),
            std::io::ErrorKind::Interrupted
        );
        assert!(session.torrent(hash).is_none());
        let mut observer = session.admit(options()).unwrap();
        old_observer.stop().await.unwrap();
        assert!(session.torrent(hash).is_some());
        while !observer.snapshot().progress.ready || !observer.snapshot().progress.completed {
            assert!(
                observer.changed().await,
                "stale handle stopped the replacement"
            );
        }
        drop(session);
        while observer.snapshot().progress.phase != TorrentPhase::Stopped {
            assert!(
                observer.changed().await,
                "last-clone drop did not publish stopping"
            );
        }
        assert!(observer.outcome().await.cleanup_complete);
        assert!(observer.snapshot().progress.joined);
        assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
    })
    .await
    .expect("replacement and last-clone stop");
}
