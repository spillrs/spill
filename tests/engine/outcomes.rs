use crate::payload::{Temp, seed_metainfo};
use spill::engine::{
    profile::Profile,
    session::{Limits, NetworkOptions, Session},
    torrent::{Destination, RecoveryState, RunPolicy, TorrentExit, TorrentOptions, TorrentSource},
};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn completion_checkpoint_failures_preserve_the_writer_error() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for panic in [false, true] {
            let root = Temp::new();
            std::fs::write(root.0.join("x"), b"abc").unwrap();
            let meta = Profile::default()
                .parse_owned(&seed_metainfo("x", b"abc", None))
                .unwrap();
            let session = Session::bind(
                NetworkOptions {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            let (entered, entering) = tokio::sync::oneshot::channel();
            let entered = Mutex::new(Some(entered));
            let (release, waiting) = std::sync::mpsc::channel();
            let waiting = Mutex::new(waiting);
            let mut options = TorrentOptions::new(
                TorrentSource::Metainfo(meta.clone()),
                Destination::Adopt(root.0.clone()),
            );
            options.run = RunPolicy::UntilComplete;
            options.persistence.checkpoint = Some(Box::new(move |_| {
                entered.lock().unwrap().take().unwrap().send(()).unwrap();
                waiting
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .expect("checkpoint callback blocked the async runtime");
                if panic {
                    panic!("completion checkpoint panic");
                }
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "completion checkpoint denied",
                ))
            }));
            let handle = session.admit(options).unwrap();
            entering.await.unwrap();
            assert!(!handle.snapshot().progress.completed);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), handle.outcome())
                    .await
                    .is_err()
            );
            assert!(!handle.is_finished());
            release.send(()).unwrap();

            let outcome = handle.outcome().await;
            assert_eq!(outcome.termination, TorrentExit::Failed);
            assert!(!outcome.wanted_complete && outcome.full_complete && outcome.cleanup_complete);
            let expected_kind = if panic {
                io::ErrorKind::Other
            } else {
                io::ErrorKind::PermissionDenied
            };
            let expected_message = if panic {
                "completion checkpoint panic"
            } else {
                "completion checkpoint denied"
            };
            for error in [
                outcome.error.as_ref().unwrap().as_ref(),
                &handle.join().await.unwrap_err(),
                &handle.wait_complete().await.unwrap_err(),
            ] {
                assert_eq!(error.kind(), expected_kind);
                assert!(error.to_string().contains(expected_message), "{error}");
            }
            assert!(session.torrent(handle.info_hash()).is_none());
            let mut replacement = TorrentOptions::new(
                TorrentSource::Metainfo(meta),
                Destination::Recover {
                    root: root.0.clone(),
                    state: handle.recovery().unwrap(),
                },
            );
            replacement.run = RunPolicy::UntilComplete;
            let replacement = session.admit(replacement).unwrap();
            replacement.join().await.unwrap();
            replacement.wait_complete().await.unwrap();
            session.shutdown().await.unwrap();
            assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
        }
    })
    .await
    .expect("completion checkpoint errors survive execution cleanup");
}

#[tokio::test]
async fn completion_survives_later_checkpoint_failure_and_removal() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let root = Temp::new();
        std::fs::write(root.0.join("x"), b"abc").unwrap();
        let profile = Profile::default();
        let meta = profile
            .parse_owned(&seed_metainfo("x", b"abc", None))
            .unwrap();
        let session = Session::bind(
            NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(),
                ..Default::default()
            },
            Limits::default(),
        )
        .await
        .unwrap();
        let checkpoints = Arc::new(AtomicUsize::new(0));
        let saved = checkpoints.clone();
        let mut options = TorrentOptions::new(
            TorrentSource::Metainfo(meta),
            Destination::Open(root.0.clone()),
        );
        options.persistence.checkpoint = Some(Box::new(move |_| {
            if saved.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "checkpoint denied",
                ))
            }
        }));
        let handle = session.admit(options).unwrap();
        let hash = handle.info_hash();
        handle.wait_complete().await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let error = handle.stop().await.unwrap_err();
        let seeding = handle.snapshot().progress.seeding_duration;
        assert!(seeding >= Duration::from_millis(30));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(handle.snapshot().progress.seeding_duration, seeding);
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        let outcome = handle.outcome().await;
        assert_eq!(outcome.termination, TorrentExit::Failed);
        assert!(outcome.wanted_complete && outcome.full_complete && outcome.cleanup_complete);
        assert!(!outcome.recheck_finished);
        assert!(checkpoints.load(Ordering::Relaxed) >= 2);
        handle.wait_complete().await.unwrap();
        assert_eq!(
            handle.join().await.unwrap_err().to_string(),
            "checkpoint denied"
        );
        assert!(session.torrent(hash).is_none());
        assert!(Arc::ptr_eq(&outcome, &handle.outcome().await));
        assert_eq!(
            handle.join().await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        session.shutdown().await.unwrap();
        assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
    })
    .await
    .expect("completion and failed cleanup checkpoint");
}

#[tokio::test]
async fn incomplete_recheck_and_callback_failures_retain_distinct_outcomes() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for mode in 0..3 {
            let root = Temp::new();
            let profile = Profile::default();
            let meta = profile
                .parse_owned(&seed_metainfo("x", b"abc", None))
                .unwrap();
            let session = Session::offline(Limits {
                torrents: 1,
                ..Default::default()
            });
            let mut options = TorrentOptions::new(
                TorrentSource::Metainfo(meta.clone()),
                Destination::Open(root.0.clone()),
            );
            options.run = RunPolicy::Recheck;
            options.persistence.checkpoint = Some(Box::new(move |_| match mode {
                0 => Ok(()),
                1 => Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "checkpoint denied",
                )),
                _ => panic!("checkpoint panic"),
            }));
            let handle = session.admit(options).unwrap();
            let mut completion = Box::pin(handle.wait_complete());
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(completion.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(completion);
            let outcome = handle.outcome().await;
            assert!(!outcome.wanted_complete && !outcome.full_complete);
            assert!(outcome.recheck_finished);
            assert!(outcome.cleanup_complete);
            let error = handle.wait_complete().await.unwrap_err();
            if mode == 0 {
                assert_eq!(outcome.termination, TorrentExit::Rechecked);
                assert!(outcome.error.is_none());
                assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
                handle.join().await.unwrap();
            } else {
                assert_eq!(outcome.termination, TorrentExit::Failed);
                assert_eq!(
                    error.kind(),
                    if mode == 1 {
                        io::ErrorKind::PermissionDenied
                    } else {
                        io::ErrorKind::Other
                    }
                );
                assert!(handle.join().await.is_err());
            }
            assert!(Arc::ptr_eq(&outcome, &handle.outcome().await));
            if mode == 2 {
                assert!(
                    outcome
                        .error
                        .as_ref()
                        .unwrap()
                        .to_string()
                        .contains("checkpoint panic")
                );
            }
            let mut options = TorrentOptions::new(
                TorrentSource::Metainfo(meta),
                Destination::Recover {
                    root: root.0.clone(),
                    state: handle.recovery().unwrap(),
                },
            );
            options.run = RunPolicy::Recheck;
            session.admit(options).unwrap().join().await.unwrap();
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("retained recheck outcomes");
}

#[tokio::test]
async fn tracker_checkpoint_panic_after_cancelled_stop_retains_cleanup_ownership() {
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        let tracker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/announce", tracker.local_addr().unwrap());
        let tracker = tokio::spawn(async move {
            loop {
                let (socket, _) = tracker.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                let completed = line.contains("event=completed");
                loop {
                    line.clear();
                    assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                let body = b"d8:intervali60e5:peers0:e";
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
                socket.write_all(body).await.unwrap();
                if completed {
                    break;
                }
            }
        });
        let root = Temp::new();
        let profile = Profile::default();
        let meta = profile
            .parse_owned(&seed_metainfo("x", b"abc", Some(&url)))
            .unwrap();
        // Record writable authority through an offline execution.
        let mut create = TorrentOptions::new(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Create(root.0.clone()),
        );
        create.run = RunPolicy::Recheck;
        let offline = Session::offline(Limits::default());
        let created = offline.admit(create).unwrap();
        assert!(created.outcome().await.recheck_finished);
        let binding = created.recovery().unwrap().binding;
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
        let (entered, entering) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, waiting) = std::sync::mpsc::channel();
        let waiting = Mutex::new(waiting);
        let mut options = TorrentOptions::new(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Recover {
                root: root.0.clone(),
                state: RecoveryState {
                    binding: binding.clone(),
                    resume: None,
                    completed_announce: Some(false),
                },
            },
        );
        options.persistence.completion_announce = Some(Box::new(move |_| {
            entered.lock().unwrap().take().unwrap().send(()).unwrap();
            waiting
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .expect("tracker callback blocked the async runtime");
            panic!("tracker checkpoint panic");
        }));
        let mut handle = session.admit(options).unwrap();
        let hash = handle.info_hash();
        entering.await.unwrap();
        handle.request_stop();
        while handle.snapshot().progress.phase != spill::engine::torrent::TorrentPhase::Stopping {
            assert!(handle.changed().await);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), handle.stop())
                .await
                .is_err()
        );
        assert!(!handle.is_finished());
        assert!(session.torrent(hash).is_some());
        // Payload leases outlive a stop that is still joining its callback.
        let mut probe = TorrentOptions::new(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Recover {
                root: root.0.clone(),
                state: RecoveryState {
                    binding: binding.clone(),
                    resume: None,
                    completed_announce: None,
                },
            },
        );
        probe.run = RunPolicy::Recheck;
        let probe = offline.admit(probe).unwrap().outcome().await;
        assert!(probe.error.is_some() && !probe.recheck_finished);
        release.send(()).unwrap();
        let outcome = handle.outcome().await;
        assert_eq!(outcome.termination, TorrentExit::Failed);
        assert!(outcome.cleanup_complete);
        assert!(
            outcome
                .error
                .as_ref()
                .unwrap()
                .to_string()
                .contains("tracker checkpoint panic")
        );
        assert!(handle.join().await.is_err());
        assert!(session.torrent(hash).is_none());
        let mut options = TorrentOptions::new(
            TorrentSource::Metainfo(meta),
            Destination::Recover {
                root: root.0.clone(),
                state: handle.recovery().unwrap(),
            },
        );
        options.run = RunPolicy::Recheck;
        session.admit(options).unwrap().join().await.unwrap();
        handle.request_stop();
        assert!(Arc::ptr_eq(&outcome, &handle.outcome().await));
        session.shutdown().await.unwrap();
        offline.shutdown().await.unwrap();
        tracker.await.unwrap();
    })
    .await
    .expect("tracker checkpoint acknowledgement cleanup");
}
