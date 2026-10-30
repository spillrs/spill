use crate::payload::Temp;
use sha1::{Digest, Sha1};
use spill::engine::{
    profile::Profile,
    selection::FilePriority,
    session::{Limits, NetworkOptions, Session},
    settings::TorrentSettings,
    torrent::{Destination, RunPolicy, TorrentExit, TorrentOptions, TorrentSource},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::watch,
    task::JoinSet,
    time::Instant,
};

#[tokio::test]
async fn one_shot_download_joins_success_and_preparation_failure() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let root = Temp::new();
        std::fs::write(root.0.join("x"), b"abc").unwrap();
        let mut bytes = crate::payload::seed_metainfo("x", b"abc", None);
        // Private metainfo keeps this filesystem-only scenario off the public DHT.
        bytes.truncate(bytes.len() - 2);
        bytes.extend_from_slice(b"7:privatei1eee");
        let meta = Profile::default().parse_owned(&bytes).unwrap();
        let error = spill::engine::download(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Create(root.0.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        for destination in [
            Destination::Adopt(root.0.clone()),
            Destination::Open(root.0.clone()),
        ] {
            spill::engine::download(TorrentSource::Metainfo(meta.clone()), destination)
                .await
                .unwrap();
            assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
        }
    })
    .await
    .expect("one-shot cleanup and destination reuse");
}

#[tokio::test]
async fn until_complete_joins_after_delivery_or_grace_without_announcing_partial_content() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for (trackers, partial, stall, fail_ack) in [
            (false, false, false, false),
            (true, false, false, false),
            (true, false, true, false),
            (true, true, false, false),
            (true, false, false, true),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/announce", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = requests.clone();
            let (stop_tracker, mut stopping) = watch::channel(false);
            let tracker = tokio::spawn(async move {
                let mut exchanges = JoinSet::new();
                loop {
                    tokio::select! {
                        biased;
                        _ = stopping.changed() => break,
                        result = exchanges.join_next(), if !exchanges.is_empty() => { result.unwrap().unwrap(); }
                        socket = listener.accept() => {
                            let (socket, _) = socket.unwrap();
                            let recorded = recorded.clone();
                            exchanges.spawn(async move {
                                let mut socket = BufReader::new(socket);
                                let mut line = String::new();
                                assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                                let event = if line.contains("event=completed") { "completed" }
                                    else if line.contains("event=stopped") { "stopped" }
                                    else { "started" };
                                recorded.lock().unwrap().push(event);
                                loop {
                                    line.clear();
                                    assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                                    if line == "\r\n" { break; }
                                }
                                if stall && event == "completed" {
                                    std::future::pending::<()>().await;
                                }
                                let body = b"d8:intervali60e5:peers0:e";
                                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                                socket.write_all(body).await.unwrap();
                            });
                        }
                    }
                }
                exchanges.abort_all();
                while let Some(result) = exchanges.join_next().await {
                    if let Err(error) = result { assert!(error.is_cancelled()); }
                }
            });
            let mut bytes = format!("d8:announce{}:{}4:infod5:filesld6:lengthi3e4:pathl1:aeed6:lengthi3e4:pathl1:beee4:name6:bundle12:piece lengthi3e6:pieces40:", url.len(), url).into_bytes();
            bytes.extend_from_slice(&Sha1::digest(b"abc"));
            bytes.extend_from_slice(&Sha1::digest(b"def"));
            bytes.extend_from_slice(b"ee");
            let profile = Profile::default();
            let meta = profile.parse_owned(&bytes).unwrap();
            let source = Temp::new();
            std::fs::create_dir(source.0.join("bundle")).unwrap();
            std::fs::write(source.0.join("bundle/a"), b"abc").unwrap();
            std::fs::write(source.0.join("bundle/b"), b"def").unwrap();
            let seed = Session::bind(NetworkOptions { listen: "127.0.0.1:0".parse().unwrap(), allow_local_network: true, ..Default::default() }, Limits::default()).await.unwrap();
            let settings = TorrentSettings { trackers: false, dht: false, pex: false, web_seeds: false, ..Default::default() };
            let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta.clone()), Destination::Open(source.0.clone()));
            options.settings = settings.clone();
            seed.admit(options).unwrap().wait_complete().await.unwrap();
            let destination = Temp::new();
            let session = Session::bind(NetworkOptions { listen: "127.0.0.1:0".parse().unwrap(), allow_local_network: true, ..Default::default() }, Limits { torrents: 1, ..Default::default() }).await.unwrap();
            let delivery = Arc::new(Mutex::new(Vec::new()));
            let saved = delivery.clone();
            let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta.clone()), Destination::Open(destination.0.clone()));
            options.settings = TorrentSettings { trackers, file_priorities: if partial { vec![FilePriority::Normal, FilePriority::Skip] } else { Vec::new() }, ..settings };
            options.peers = vec![seed.local_addr()];
            options.persistence.completion_announce = Some(Box::new(move |delivered| {
                saved.lock().unwrap().push(delivered);
                if delivered && fail_ack { Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "delivery checkpoint denied")) } else { Ok(()) }
            }));
            options.run = RunPolicy::UntilComplete;
            let handle = session.admit(options).unwrap();
            handle.wait_complete().await.unwrap();
            let completed = Instant::now();
            let mut observer = handle.clone();
            while observer.changed().await {}
            let outcome = handle.outcome().await;
            assert_eq!(outcome.termination, if fail_ack { TorrentExit::Failed } else { TorrentExit::Complete });
            assert!(outcome.wanted_complete && outcome.cleanup_complete);
            assert_eq!(outcome.full_complete, !partial);
            assert!(!outcome.recheck_finished);
            assert_eq!(outcome.error.as_ref().map(|e| e.kind()), fail_ack.then_some(std::io::ErrorKind::PermissionDenied));
            assert!(handle.snapshot().progress.joined);
            if stall {
                assert!(completed.elapsed() >= Duration::from_millis(4900));
            } else {
                assert!(completed.elapsed() < Duration::from_secs(4), "unnecessary completion grace");
            }
            assert_eq!(*delivery.lock().unwrap(), if !trackers || partial { vec![] } else if stall { vec![false] } else { vec![false, true] });
            assert_eq!(handle.join().await.is_err(), fail_ack);
            assert!(session.torrent(handle.info_hash()).is_none());
            // The recorded authority reopens the payload once cleanup has released its leases.
            let mut recheck = TorrentOptions::new(TorrentSource::Metainfo(meta.clone()), Destination::Recover { root: destination.0.clone(), state: handle.recovery().unwrap() });
            recheck.run = RunPolicy::Recheck;
            let offline = Session::offline(Limits::default());
            let rechecked = offline.admit(recheck).unwrap().outcome().await;
            assert!(rechecked.recheck_finished && rechecked.error.is_none());
            offline.shutdown().await.unwrap();
            session.shutdown().await.unwrap();
            seed.shutdown().await.unwrap();
            stop_tracker.send_replace(true);
            tracker.await.unwrap();
            let requests = requests.lock().unwrap();
            if !trackers {
                assert!(requests.is_empty());
            } else {
                assert_eq!(requests.first(), Some(&"started"));
                assert_eq!(requests.last(), Some(&"stopped"));
                assert_eq!(requests.contains(&"completed"), !partial);
            }
            assert_eq!(std::fs::read(destination.0.join("bundle/a")).unwrap(), b"abc");
            if partial {
                assert!(!destination.0.join("bundle/b").exists());
            } else {
                assert_eq!(std::fs::read(destination.0.join("bundle/b")).unwrap(), b"def");
            }
        }
    }).await.expect("until-complete delivery and joined cleanup");
}
