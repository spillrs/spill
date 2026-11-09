use crate::{payload::Temp, v2_peer as raw};
use sha1::{Digest, Sha1};
use spill::engine::{
    profile::Profile,
    session::{Limits, NetworkOptions, PeerEncryption, Session},
    settings::TorrentSettings,
    torrent::{Destination, TorrentOptions, TorrentSource},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Notify, mpsc},
};

#[tokio::test]
async fn private_failover_discards_old_peers_and_contacts_and_stops_every_registration() {
    tokio::time::timeout(Duration::from_secs(45), async {
        for cancel_switch in [false, true] {
        let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stale = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let replacement = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let urls = [a.local_addr().unwrap(), b.local_addr().unwrap()]
            .map(|address| format!("http://{address}/announce"));
        let info = [
            b"d6:lengthi6e4:name1:x12:piece lengthi3e6:pieces40:".as_slice(),
            &Sha1::digest(b"abc"), &Sha1::digest(b"def"), b"7:privatei1ee",
        ].concat();
        let bytes = [
            b"d13:announce-listll".as_slice(),
            format!("{}:{}el{}:{}ee4:info", urls[0].len(), urls[0], urls[1].len(), urls[1]).as_bytes(),
            &info, b"e",
        ].concat();
        let profile = Profile { peers: 1, ..Profile::default() };
        let meta = profile.parse_owned(&bytes).unwrap();
        let key = meta.info_hash().wire_key();
        let records = Arc::new(Mutex::new(Vec::new()));
        let (sent, mut requests) = mpsc::unbounded_channel();
        let endpoints = [old.local_addr().unwrap(), stale.local_addr().unwrap(), replacement.local_addr().unwrap()];
        let mut trackers = Vec::new();
        for (name, listener) in [('A', a), ('B', b)] {
            let records = records.clone();
            let sent = sent.clone();
            trackers.push(tokio::spawn(async move {
                let mut attempts = 0;
                let mut stops = 0;
                loop {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut socket = BufReader::new(socket);
                    let mut line = String::new();
                    socket.read_line(&mut line).await.unwrap();
                    let query = line.split_whitespace().nth(1).unwrap().split_once('?').unwrap().1;
                    let fields: BTreeMap<String, String> = query.split('&').map(|pair| {
                        let (key, value) = pair.split_once('=').unwrap();
                        (key.to_owned(), value.to_owned())
                    }).collect();
                    let event = fields.get("event").map(String::as_str).unwrap_or("none");
                    records.lock().unwrap().push((name, event.to_owned(), fields.clone()));
                    sent.send((name, event.to_owned())).unwrap();
                    loop {
                        line.clear();
                        socket.read_line(&mut line).await.unwrap();
                        if line == "\r\n" { break; }
                    }
                    if name == 'A' && event == "stopped" {
                        stops += 1;
                        if cancel_switch {
                            std::future::pending::<()>().await;
                        }
                    }
                    attempts += 1;
                    let failed = (name == 'A' && (attempts == 3 || (event == "stopped" && stops == 1)))
                        || (name == 'B' && attempts == 4);
                    let status = if failed { "503 Unavailable" } else { "200 OK" };
                    let body = if failed {
                        b"d14:failure reason11:unavailablee".to_vec()
                    } else {
                        let address = if name == 'B' { endpoints[2] } else if attempts == 1 { endpoints[0] } else { endpoints[1] };
                        let std::net::SocketAddr::V4(address) = address else { unreachable!() };
                        [b"d8:intervali300e12:min intervali0e5:peers6:".as_slice(), &address.ip().octets(), &address.port().to_be_bytes(), b"e"].concat()
                    };
                    socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                    socket.write_all(&body).await.unwrap();
                }
            }));
        }
        let old_closed = Arc::new(AtomicBool::new(false));
        let closed = old_closed.clone();
        let old_ready = Arc::new(Notify::new());
        let ready = old_ready.clone();
        let old_peer = tokio::spawn(async move {
            let (mut socket, _) = old.accept().await.unwrap();
            raw::accept(&mut socket, key, 42).await;
            raw::send(&mut socket, 5, &[0x80]).await;
            raw::send(&mut socket, 1, &[]).await;
            let request = raw::next(&mut socket, 6).await;
            assert_eq!(&request[1..], &raw::request(0, 0, 3));
            raw::send(&mut socket, 7, &[&request[1..9], b"abc"].concat()).await;
            assert_eq!(&raw::next(&mut socket, 4).await[1..], &0u32.to_be_bytes());
            raw::send(&mut socket, 2, &[]).await;
            raw::next(&mut socket, 1).await;
            raw::send(&mut socket, 6, &raw::request(0, 0, 3)).await;
            let uploaded = raw::next(&mut socket, 7).await;
            assert_eq!(&uploaded[9..], b"abc");
            ready.notify_one();
            while socket.read_u8().await.is_ok() {}
            closed.store(true, Ordering::SeqCst);
        });
        let release = Arc::new(Notify::new());
        let gate = release.clone();
        let new_ready = Arc::new(Notify::new());
        let ready = new_ready.clone();
        let new_peer = tokio::spawn(async move {
            let (mut socket, _) = replacement.accept().await.unwrap();
            while !old_closed.load(Ordering::SeqCst) { tokio::task::yield_now().await; }
            raw::accept(&mut socket, key, 43).await;
            ready.notify_one();
            gate.notified().await;
            raw::send(&mut socket, 5, &[0x40]).await;
            raw::send(&mut socket, 1, &[]).await;
            let request = raw::next(&mut socket, 6).await;
            assert_eq!(&request[1..], &raw::request(1, 0, 3));
            raw::send(&mut socket, 7, &[&request[1..9], b"def"].concat()).await;
            while socket.read_u8().await.is_ok() {}
        });
        let session = Session::bind(NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(), allow_local_network: true,
            utp: false, peer_encryption: PeerEncryption::Allowed, ..Default::default()
        }, Limits::default()).await.unwrap();
        let root = Temp::new();
        let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta), Destination::Open(root.0.clone()));
        options.profile = profile;
        options.diagnostics = true;
        options.settings = TorrentSettings { trackers: true, dht: false, pex: false, web_seeds: false, ..Default::default() };
        options.peers.push(endpoints[1]);
        assert_eq!(session.admit(options).err().unwrap().kind(), std::io::ErrorKind::InvalidInput);
        let meta = profile.parse_owned(&bytes).unwrap();
        let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta), Destination::Open(root.0.clone()));
        options.profile = profile;
        options.diagnostics = true;
        options.settings = TorrentSettings { trackers: true, dht: false, pex: false, web_seeds: false, ..Default::default() };
        let handle = session.admit(options).unwrap();
        assert_eq!(requests.recv().await.unwrap(), ('A', "started".into()));
        old_ready.notified().await;
        for exchanges in [2, 3] {
            handle.reannounce();
            assert_eq!(requests.recv().await.unwrap(), ('A', "none".into()));
            if exchanges == 2 {
                while handle.snapshot().diagnostics.as_ref().unwrap().trackers[0].exchanges < 2 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        assert_eq!(requests.recv().await.unwrap(), ('A', "stopped".into()));
        if cancel_switch {
            tokio::time::timeout(Duration::from_secs(6), handle.stop()).await
                .expect("stop must interrupt switch cleanup rather than wait for two stop budgets")
                .unwrap();
            old_peer.await.unwrap();
            new_peer.abort();
            assert!(records.lock().unwrap().iter().all(|(name, _, _)| *name == 'A'));
            session.shutdown().await.unwrap();
            for tracker in trackers { tracker.abort(); }
            continue;
        }
        assert_eq!(requests.recv().await.unwrap(), ('B', "started".into()));
        new_ready.notified().await;
        old_peer.await.unwrap();
        // Expiring the preferred URL's backoff must not displace a working fallback.
        tokio::time::sleep(Duration::from_secs(18)).await;
        handle.reannounce();
        assert_eq!(requests.recv().await.unwrap(), ('B', "none".into()));
        release.notify_one();
        handle.wait_complete().await.unwrap();
        assert_eq!(requests.recv().await.unwrap(), ('B', "completed".into()));
        while handle.snapshot().diagnostics.as_ref().unwrap().trackers[1].exchanges < 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        handle.reannounce();
        assert_eq!(requests.recv().await.unwrap(), ('B', "none".into()));
        let mut stops = vec![requests.recv().await.unwrap(), requests.recv().await.unwrap()];
        stops.sort();
        assert_eq!(stops, [('A', "stopped".into()), ('B', "stopped".into())]);
        assert_eq!(requests.recv().await.unwrap(), ('A', "started".into()));
        while handle.snapshot().diagnostics.as_ref().unwrap().trackers[0].exchanges < 4 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        handle.stop().await.unwrap();
        assert_eq!(handle.snapshot().progress.processed_payload_bytes, 6);
        assert_eq!(handle.snapshot().progress.written_payload_bytes, 3);
        new_peer.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(100), stale.accept()).await.is_err());
        let records = records.lock().unwrap().clone();
        assert_eq!(records.iter().filter(|(name, _, _)| *name == 'A').map(|(_, event, _)| event.as_str()).collect::<Vec<_>>(), ["started", "none", "none", "stopped", "stopped", "started", "stopped"]);
        for (_, event, fields) in &records {
            if event == "started" {
                assert_eq!(fields["downloaded"], "0");
                assert_eq!(fields["uploaded"], "0");
                assert_eq!(fields["corrupt"], "0");
            }
        }
        for (name, expected) in [('A', vec![("3", "3"), ("3", "3"), ("0", "0")]), ('B', vec![("3", "0")])] {
            let stops: Vec<_> = records.iter().filter(|(n, event, _)| *n == name && event == "stopped")
                .map(|(_, _, fields)| (fields["downloaded"].as_str(), fields["uploaded"].as_str())).collect();
            assert_eq!(stops, expected, "{records:?}");
        }
        let fallback = records.iter().find(|(name, event, _)| *name == 'B' && event == "started").unwrap();
        assert_eq!(fallback.2["left"], "3");
        assert_eq!(records.iter().rev().find(|(_, event, _)| event == "started").unwrap().2["left"], "0");
        assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abcdef");
        session.shutdown().await.unwrap();
        for tracker in trackers { tracker.abort(); }
        }
    }).await.expect("private tracker isolation, session accounting and stop responsiveness");
}
