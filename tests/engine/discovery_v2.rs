use crate::{payload::Temp, v2_peer as raw};
use spill::{
    engine::{
        profile::Profile,
        session::{DhtOptions, Limits, NetworkOptions, Session, dht::SavedState},
        settings::TorrentSettings,
        torrent::{Destination, RunPolicy, TorrentOptions, TorrentSource},
    },
    protocol::{
        bencode::{self, Node, Value},
        dht::{Contact, NodeId},
        metainfo::OwnedMetainfo,
    },
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, UdpSocket},
};

const V2: &[u8] = include_bytes!("../protocol/fixtures/v2.torrent");
const HYBRID: &[u8] = include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent");
const LENGTHS: &[usize] = &[16384, 0, 65659, 1, 32768, 123];

fn metainfo(fixture: &[u8], url: &str) -> OwnedMetainfo {
    let doc = bencode::decode(fixture, Default::default()).unwrap();
    let Value::Dictionary(fields) = doc.value() else {
        unreachable!()
    };
    let mut fields = fields.clone();
    fields.insert(b"announce", Node::new(Value::Bytes(url.as_bytes())));
    let bytes = bencode::encode(&Node::new(Value::Dictionary(fields)), Default::default()).unwrap();
    let meta = Profile::default().parse_owned(&bytes).unwrap();
    assert_eq!(
        meta.info_hash(),
        Profile::default().parse_owned(fixture).unwrap().info_hash()
    );
    meta
}
async fn session(dht: Option<DhtOptions>) -> Session {
    Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            utp: false,
            allow_local_network: true,
            dht,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap()
}
fn options(meta: &OwnedMetainfo, root: &Temp) -> TorrentOptions {
    let mut options = TorrentOptions::new(
        TorrentSource::Metainfo(meta.clone()),
        Destination::Open(root.0.clone()),
    );
    options.settings = TorrentSettings {
        dht: false,
        pex: false,
        web_seeds: false,
        ..Default::default()
    };
    options.diagnostics = true;
    options
}
fn swarm_keys(meta: &OwnedMetainfo) -> Vec<[u8; 20]> {
    meta.info_hash()
        .v1()
        .map(|hash| *hash.as_bytes())
        .into_iter()
        .chain(meta.info_hash().v2().map(|hash| hash.wire_key()))
        .collect()
}
fn peers(meta: &OwnedMetainfo) -> Vec<Vec<u8>> {
    raw::pieces(
        if meta.info_hash().v1().is_some() {
            LENGTHS
        } else {
            &LENGTHS[..5]
        },
        32768,
    )
}
async fn source(
    key: [u8; 20],
    pieces: Vec<Vec<u8>>,
    bits: u8,
) -> (SocketAddr, tokio::task::JoinHandle<Vec<u32>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        raw::accept(&mut socket, key, key[0]).await;
        raw::serve_advertised(socket, pieces, &[bits], false).await
    });
    (address, task)
}
fn compact(address: SocketAddr) -> Vec<u8> {
    let SocketAddr::V4(address) = address else {
        unreachable!()
    };
    [
        address.ip().octets().as_slice(),
        &address.port().to_be_bytes(),
    ]
    .concat()
}
fn unescape(value: &str) -> Vec<u8> {
    let mut bytes = value.bytes();
    let mut decoded = Vec::new();
    while let Some(byte) = bytes.next() {
        decoded.push(if byte == b'%' {
            let hex = [bytes.next().unwrap(), bytes.next().unwrap()];
            u8::from_str_radix(std::str::from_utf8(&hex).unwrap(), 16).unwrap()
        } else {
            byte
        });
    }
    decoded
}

#[tokio::test]
async fn http_trackers_download_each_swarm_and_checkpoint_completion_independently() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for fixture in [V2, HYBRID] {
            let tracker = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let meta = metainfo(fixture, &format!("http://{}/announce", tracker.local_addr().unwrap()));
            let keys = swarm_keys(&meta);
            let mut endpoints = BTreeMap::new();
            let mut sources = Vec::new();
            for (index, &key) in keys.iter().enumerate() {
                let bits = if keys.len() == 1 { 0xfc } else if index == 0 { 0x70 } else { 0x8e };
                let (address, task) = source(key, peers(&meta), bits).await;
                endpoints.insert(key, address);
                sources.push(task);
            }
            let records = Arc::new(Mutex::new(Vec::new()));
            let recorded = records.clone();
            let tracker = tokio::spawn(async move {
                loop {
                    let (socket, _) = tracker.accept().await.unwrap();
                    let mut socket = BufReader::new(socket);
                    let mut line = String::new();
                    socket.read_line(&mut line).await.unwrap();
                    let target = line.split_whitespace().nth(1).unwrap().to_owned();
                    loop {
                        line.clear();
                        socket.read_line(&mut line).await.unwrap();
                        assert!(!line.to_ascii_lowercase().starts_with("referer:"));
                        if line == "\r\n" { break; }
                    }
                    let (path, query) = target.split_once('?').unwrap();
                    if path == "/announce" {
                        socket.write_all(format!("HTTP/1.1 302 Found\r\nLocation: /redirected?{query}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                        continue;
                    }
                    assert_eq!(path, "/redirected");
                    let fields: BTreeMap<_, _> = query.split('&').map(|pair| pair.split_once('=').unwrap()).collect();
                    let key: [u8; 20] = unescape(fields["info_hash"]).try_into().unwrap();
                    let event = fields.get("event").unwrap_or(&"none").to_string();
                    recorded.lock().unwrap().push((key, event));
                    let address = endpoints[&key];
                    let body = [b"d8:intervali60e5:peers6:".as_slice(), &compact(address), b"e"].concat();
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                    socket.write_all(&body).await.unwrap();
                }
            });
            let session = session(None).await;
            let root = Temp::new();
            let delivery = Arc::new(Mutex::new(Vec::new()));
            let saved = delivery.clone();
            let mut opts = options(&meta, &root);
            opts.run = RunPolicy::UntilComplete;
            opts.persistence.completion_announce = Some(Box::new(move |key, delivered| {
                saved.lock().unwrap().push((key, delivered)); Ok(())
            }));
            let handle = session.admit(opts).unwrap();
            let outcome = tokio::time::timeout(Duration::from_secs(8), handle.outcome()).await.unwrap_or_else(|_| panic!("HTTP {:?} {:?}", handle.snapshot(), records.lock().unwrap()));
            assert!(outcome.full_complete && outcome.cleanup_complete, "{outcome:?}");
            let recovery = handle.recovery().unwrap();
            for key in &keys {
                assert_eq!(delivery.lock().unwrap().iter().filter(|(k, _)| k == key).map(|(_, b)| *b).collect::<Vec<_>>(), [false, true]);
                assert!(recovery.completed_announces.contains(&(*key, true)));
                assert_eq!(records.lock().unwrap().iter().filter(|(k, _)| k == key).map(|(_, e)| e.clone()).collect::<Vec<_>>(), ["started", "completed", "stopped"]);
            }
            for task in sources { assert!(!task.await.unwrap().is_empty()); }
            // A restored acknowledgement suppresses completion for that swarm only.
            let mut recovery = recovery;
            recovery.completed_announces[0].1 = false;
            let mut opts = options(&meta, &root);
            opts.destination = Destination::Recover { root: root.0.clone(), state: recovery };
            opts.run = RunPolicy::UntilComplete;
            let handle = session.admit(opts).unwrap();
            assert!(handle.outcome().await.full_complete);
            for (index, key) in keys.iter().enumerate() {
                assert_eq!(records.lock().unwrap().iter().filter(|(k, e)| k == key && e == "completed").count(), if index == 0 { 2 } else { 1 });
            }
            session.shutdown().await.unwrap();
            tracker.abort();
        }
    }).await.expect("HTTP v2 and hybrid swarm lifecycle");
}

#[tokio::test]
async fn udp_trackers_use_prefix_keys_and_keep_hybrid_contacts_in_their_swarm() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for fixture in [V2, HYBRID] {
            let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let meta = metainfo(
                fixture,
                &format!("udp://{}/announce", tracker.local_addr().unwrap()),
            );
            let keys = swarm_keys(&meta);
            let mut endpoints = BTreeMap::new();
            let mut sources = Vec::new();
            for (index, &key) in keys.iter().enumerate() {
                let bits = if keys.len() == 1 {
                    0xfc
                } else if index == 0 {
                    0x70
                } else {
                    0x8e
                };
                let (address, task) = source(key, peers(&meta), bits).await;
                endpoints.insert(key, address);
                sources.push(task);
            }
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let records = recorded.clone();
            let tracker = tokio::spawn(async move {
                let mut packet = [0; 2048];
                loop {
                    let (length, from) = tracker.recv_from(&mut packet).await.unwrap();
                    let action = u32::from_be_bytes(packet[8..12].try_into().unwrap());
                    let mut reply = packet[8..16].to_vec();
                    if action == 0 {
                        reply.extend_from_slice(&42u64.to_be_bytes());
                    } else {
                        assert_eq!(length, 98);
                        let key: [u8; 20] = packet[16..36].try_into().unwrap();
                        let event = u32::from_be_bytes(packet[80..84].try_into().unwrap());
                        records.lock().unwrap().push((key, event));
                        reply.extend_from_slice(&60u32.to_be_bytes());
                        reply.extend_from_slice(&[0; 8]);
                        reply.extend_from_slice(&compact(endpoints[&key]));
                    }
                    tracker.send_to(&reply, from).await.unwrap();
                }
            });
            let session = session(None).await;
            let root = Temp::new();
            let mut opts = options(&meta, &root);
            opts.run = RunPolicy::UntilComplete;
            let handle = session.admit(opts).unwrap();
            let outcome = tokio::time::timeout(Duration::from_secs(8), handle.outcome())
                .await
                .unwrap_or_else(|_| {
                    panic!("UDP {:?} {:?}", handle.snapshot(), recorded.lock().unwrap())
                });
            assert!(outcome.full_complete, "{outcome:?}");
            for key in keys {
                assert_eq!(
                    recorded
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(k, _)| *k == key)
                        .map(|(_, e)| *e)
                        .collect::<Vec<_>>(),
                    [2, 1, 3]
                );
            }
            for task in sources {
                assert!(!task.await.unwrap().is_empty());
            }
            session.shutdown().await.unwrap();
            tracker.abort();
        }
    })
    .await
    .expect("UDP discovery uses both swarm keys");
}

fn node_bytes<'a>(doc: &'a Node<'a>, field: &[u8]) -> &'a [u8] {
    let Value::Bytes(bytes) = doc.get(field).unwrap().value() else {
        unreachable!()
    };
    bytes
}
#[tokio::test]
async fn dht_discovers_and_announces_both_swarm_keys_without_confusing_contacts() {
    tokio::time::timeout(Duration::from_secs(45), async {
        for fixture in [V2, HYBRID] {
            let meta = Profile::default().parse_owned(fixture).unwrap();
            let keys = swarm_keys(&meta);
            let mut endpoints = BTreeMap::new();
            let mut sources = Vec::new();
            for (index, &key) in keys.iter().enumerate() {
                let bits = if keys.len() == 1 {
                    0xfc
                } else if index == 0 {
                    0x70
                } else {
                    0x8e
                };
                // This raw source advertises no DHT support; the client may do so.
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                endpoints.insert(key, listener.local_addr().unwrap());
                let pieces = peers(&meta);
                sources.push(tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    accept(&mut socket, key, key[0]).await;
                    raw::serve_advertised(socket, pieces, &[bits], false).await
                }));
            }
            let node = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let address = node.local_addr().unwrap();
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let records = recorded.clone();
            let remote = tokio::spawn(async move {
                let mut packet = [0; 4096];
                loop {
                    let (length, from) = node.recv_from(&mut packet).await.unwrap();
                    let doc = bencode::decode(&packet[..length], Default::default()).unwrap();
                    let tx = node_bytes(&doc, b"t");
                    let method = node_bytes(&doc, b"q");
                    let args = doc.get(b"a").unwrap();
                    let mut fields =
                        BTreeMap::from([(b"id".as_slice(), Node::new(Value::Bytes(&[9; 20])))]);
                    let mut values = Vec::new();
                    if method == b"get_peers" {
                        let key: [u8; 20] = node_bytes(args, b"info_hash").try_into().unwrap();
                        records.lock().unwrap().push((key, false));
                        fields.insert(b"token", Node::new(Value::Bytes(b"token")));
                        if let Some(address) = endpoints.get(&key) {
                            values = compact(*address);
                        }
                        fields.insert(
                            b"values",
                            Node::new(Value::List(if values.is_empty() {
                                vec![]
                            } else {
                                vec![Node::new(Value::Bytes(&values))]
                            })),
                        );
                    } else if method == b"announce_peer" {
                        let key: [u8; 20] = node_bytes(args, b"info_hash").try_into().unwrap();
                        assert_eq!(node_bytes(args, b"token"), b"token");
                        records.lock().unwrap().push((key, true));
                    } else if method == b"find_node" {
                        fields.insert(b"nodes", Node::new(Value::Bytes(b"")));
                    }
                    let reply = Node::new(Value::Dictionary(BTreeMap::from([
                        (b"r".as_slice(), Node::new(Value::Dictionary(fields))),
                        (b"t", Node::new(Value::Bytes(tx))),
                        (b"y", Node::new(Value::Bytes(b"r"))),
                    ])));
                    let reply = bencode::encode(&reply, Default::default()).unwrap();
                    node.send_to(&reply, from).await.unwrap();
                }
            });
            let session = session(Some(DhtOptions {
                routers: Vec::new(),
                saved: Some(SavedState {
                    id: NodeId([7; 20]),
                    nodes: vec![Contact {
                        id: NodeId([9; 20]),
                        address,
                    }],
                }),
            }))
            .await;
            let root = Temp::new();
            let mut opts = options(&meta, &root);
            opts.settings.dht = true;
            opts.settings.trackers = false;
            let mut handle = session.admit(opts).unwrap();
            handle.wait_complete().await.unwrap();
            loop {
                let diagnostics = handle.snapshot().diagnostics.unwrap();
                if diagnostics.dht.len() == keys.len()
                    && diagnostics.dht.iter().all(|s| s.announced > 0)
                {
                    break;
                }
                assert!(handle.changed().await);
            }
            for key in keys {
                assert!(recorded.lock().unwrap().contains(&(key, true)));
            }
            handle.stop().await.unwrap();
            for task in sources {
                assert!(!task.await.unwrap().is_empty());
            }
            session.shutdown().await.unwrap();
            remote.abort();
        }
    })
    .await
    .expect("in-process DHT v2 and hybrid discovery");
}

async fn frame(socket: &mut TcpStream) -> Vec<u8> {
    let length = socket.read_u32().await.unwrap();
    assert!(length <= 1024 * 1024);
    let mut frame = vec![0; length as usize];
    socket.read_exact(&mut frame).await.unwrap();
    frame
}
async fn accept(socket: &mut TcpStream, key: [u8; 20], id: u8) {
    let mut hello = [0; 68];
    socket.read_exact(&mut hello).await.unwrap();
    assert_eq!(&hello[28..48], &key);
    hello[20..28].copy_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
    hello[48..].fill(id);
    socket.write_all(&hello).await.unwrap();
}

#[tokio::test]
async fn hybrid_pex_reaches_legacy_and_upgrading_sources_from_either_connection_mode() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for fixture in [V2, HYBRID] {
            let meta = Profile::default().parse_owned(fixture).unwrap();
            let v1 = meta.info_hash().v1().map(|hash| *hash.as_bytes());
            let upgrades: &[bool] = if v1.is_some() {
                &[false, true]
            } else {
                &[false]
            };
            for key in swarm_keys(&meta) {
                for &upgrade in upgrades {
                    let session = session(None).await;
                    let root = Temp::new();
                    let mut opts = options(&meta, &root);
                    opts.settings.trackers = false;
                    opts.settings.pex = true;
                    let mut handle = session.admit(opts).unwrap();
                    while !handle.snapshot().progress.ready {
                        assert!(handle.changed().await);
                    }
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let mut hint = TcpStream::connect(session.local_addr()).await.unwrap();
                    if let Some(v1) = v1 {
                        raw::initiate_hybrid(&mut hint, v1, key, 92, key != v1).await;
                    } else {
                        raw::initiate(&mut hint, key, 92, false).await;
                    }
                    raw::send(&mut hint, 20, b"\0d1:md6:ut_pexi2eee").await;
                    let remote_id = loop {
                        let f = frame(&mut hint).await;
                        if f.starts_with(&[20, 0]) {
                            let doc = bencode::decode(&f[2..], Default::default()).unwrap();
                            let Value::Integer(id) =
                                doc.get(b"m").unwrap().get(b"ut_pex").unwrap().value()
                            else {
                                unreachable!()
                            };
                            break *id as u8;
                        }
                    };
                    let compact = compact(listener.local_addr().unwrap());
                    let payload = [
                        vec![remote_id],
                        b"d5:added6:".to_vec(),
                        compact,
                        b"e".to_vec(),
                    ]
                    .concat();
                    raw::send(&mut hint, 20, &payload).await;
                    let (mut socket, _) = listener.accept().await.unwrap();
                    if let Some(v1) = v1 {
                        let reply = if upgrade {
                            meta.info_hash().v2().unwrap().wire_key()
                        } else {
                            v1
                        };
                        raw::accept_hybrid(&mut socket, v1, reply, 94).await;
                    } else {
                        raw::accept(&mut socket, key, 94).await;
                    }
                    let pieces = peers(&meta);
                    // The hybrid fixture adds a seventh piece for its tail file.
                    let bitfield = if pieces.len() == 7 { 0xfe } else { 0xfc };
                    raw::send(&mut socket, 5, &[bitfield]).await;
                    raw::send(&mut socket, 1, &[]).await;
                    let mut requests = 0;
                    loop {
                        tokio::select! {
                            complete = handle.wait_complete() => { complete.unwrap(); break; }
                            body = frame(&mut socket) => {
                                if body.first() != Some(&6) { continue; }
                                let index =
                                    u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
                                let begin =
                                    u32::from_be_bytes(body[5..9].try_into().unwrap()) as usize;
                                let length =
                                    u32::from_be_bytes(body[9..13].try_into().unwrap()) as usize;
                                let mut payload = body[1..9].to_vec();
                                payload.extend_from_slice(&pieces[index][begin..begin + length]);
                                raw::send(&mut socket, 7, &payload).await;
                                requests += 1;
                            }
                        }
                    }
                    assert!(requests > 0);
                    assert!(handle.snapshot().progress.full_complete);
                    handle.stop().await.unwrap();
                    session.shutdown().await.unwrap();
                }
            }
        }
    })
    .await
    .expect("hybrid PEX uses v1 with upgrade to reach both peer capabilities");
}

#[tokio::test]
async fn a_stalled_hybrid_tracker_swarm_does_not_block_the_other_swarm() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let meta = metainfo(HYBRID, &format!("http://{}/announce", listener.local_addr().unwrap()));
        let v1 = *meta.info_hash().v1().unwrap().as_bytes();
        let v2 = meta.info_hash().v2().unwrap().wire_key();
        let (address, source) = source(v2, peers(&meta), 0xfe).await;
        let tracker = tokio::spawn(async move {
            let mut jobs = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    result = jobs.join_next(), if !jobs.is_empty() => { result.unwrap().unwrap(); }
                    socket = listener.accept() => {
                        let (socket, _) = socket.unwrap();
                        jobs.spawn(async move {
                            let mut socket = BufReader::new(socket);
                            let mut line = String::new();
                            socket.read_line(&mut line).await.unwrap();
                            let query = line.split_whitespace().nth(1).unwrap().split_once('?').unwrap().1;
                            let key = query.split('&').find_map(|pair| pair.strip_prefix("info_hash=")).unwrap();
                            let key: [u8; 20] = unescape(key).try_into().unwrap();
                            if key == v1 { std::future::pending::<()>().await; }
                            assert_eq!(key, v2);
                            loop { line.clear(); socket.read_line(&mut line).await.unwrap(); if line == "\r\n" { break; } }
                            let body = [b"d8:intervali60e5:peers6:".as_slice(), &compact(address), b"e"].concat();
                            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                            socket.write_all(&body).await.unwrap();
                        });
                    }
                }
            }
        });
        let session = session(None).await;
        let root = Temp::new();
        let mut opts = options(&meta, &root);
        opts.run = RunPolicy::UntilComplete;
        let handle = session.admit(opts).unwrap();
        let outcome = handle.outcome().await;
        assert!(outcome.full_complete && outcome.cleanup_complete, "{outcome:?}");
        let states = handle.recovery().unwrap().completed_announces;
        assert!(states.contains(&(v1, false)));
        assert!(states.contains(&(v2, true)));
        assert!(!source.await.unwrap().is_empty());
        session.shutdown().await.unwrap();
        tracker.abort();
    }).await.expect("independent tracker swarm progress and completion grace");
}
