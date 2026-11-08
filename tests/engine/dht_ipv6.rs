use crate::payload::{Temp, seed_metainfo};
use spill::{
    engine::{
        profile::Profile,
        session::{DhtOptions, Limits, NetworkOptions, Session, dht::SavedState},
        settings::TorrentSettings,
        torrent::{Destination, TorrentHandle, TorrentOptions, TorrentSource},
    },
    protocol::{
        bencode::{Node, Value},
        compact,
        dht::{
            Contact, NodeId,
            krpc::{Body, Kind, Message, Query},
        },
        magnet::Magnet,
    },
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::UdpSocket;

fn bytes(value: &[u8]) -> Node<'_> {
    Node::new(Value::Bytes(value))
}
fn dict<'a>(fields: impl IntoIterator<Item = (&'a [u8], Node<'a>)>) -> Node<'a> {
    Node::new(Value::Dictionary(fields.into_iter().collect()))
}
fn settings(dht: bool) -> TorrentSettings {
    TorrentSettings {
        dht,
        trackers: false,
        pex: false,
        web_seeds: false,
        ..Default::default()
    }
}

async fn start_node(
    listen: &str,
    saved: Option<SavedState>,
    routers: Vec<String>,
) -> (Session, Temp, TorrentHandle) {
    let session = Session::bind(
        NetworkOptions {
            listen: listen.parse().unwrap(),
            utp: false,
            allow_local_network: true,
            dht: Some(DhtOptions { saved, routers }),
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap();
    let root = Temp::new();
    std::fs::write(root.0.join("warm"), b"abc").unwrap();
    let meta = Profile::default()
        .parse_owned(&seed_metainfo("warm", b"abc", None))
        .unwrap();
    let mut options = TorrentOptions::new(
        TorrentSource::Metainfo(meta),
        Destination::Adopt(root.0.clone()),
    );
    options.settings = settings(true);
    let handle = session.admit(options).unwrap();
    handle.wait_complete().await.unwrap();
    (session, root, handle)
}

async fn rpc(
    socket: &UdpSocket,
    destination: SocketAddr,
    method: &[u8],
    fields: Vec<(&[u8], Node<'_>)>,
) -> Vec<u8> {
    // IPv6 sources share a /64 query budget.
    tokio::time::sleep(Duration::from_millis(220)).await;
    let mut arguments: BTreeMap<_, _> = fields.into_iter().collect();
    arguments.insert(b"id", bytes(&[11; 20]));
    let wire = Message::new(
        &[b't'; 32],
        Body::ReadOnlyQuery {
            method,
            arguments: Node::new(Value::Dictionary(arguments)),
        },
        None,
        16,
        100,
    )
    .unwrap()
    .encode(Default::default())
    .unwrap();
    socket.send_to(&wire, destination).await.unwrap();
    let mut response = vec![0; 4096];
    let (length, source) =
        tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut response))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(source, destination);
    assert!(
        length <= 1024,
        "BEP 32 response exceeded the datagram budget"
    );
    response.truncate(length);
    response
}

fn response(wire: &[u8]) -> Message<'_> {
    Message::parse(wire, Default::default(), 16, 100).unwrap()
}

async fn contacts(session: &Session, count: usize) -> SavedState {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let saved = session.dht_state().await.unwrap().unwrap();
            if saved.nodes.len() == count {
                return saved;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("verified DHT contacts")
}

/// A reachable remote DHT contact, optionally returning a real spill seed.
async fn remote(
    listen: &str,
    peer: Option<SocketAddr>,
    id: NodeId,
) -> (Contact, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let socket = UdpSocket::bind(listen).await.unwrap();
    let contact = Contact {
        id,
        address: socket.local_addr().unwrap(),
    };
    let announced = Arc::new(AtomicUsize::new(0));
    let count = announced.clone();
    let task = tokio::spawn(async move {
        let mut buffer = [0; 4096];
        loop {
            let (length, source) = socket.recv_from(&mut buffer).await.unwrap();
            let request = Message::parse(&buffer[..length], Default::default(), 16, 100).unwrap();
            let Kind::Query { query, .. } = request.kind() else {
                panic!("DHT request")
            };
            let mut fields = BTreeMap::from([(b"id".as_slice(), bytes(&contact.id.0))]);
            let raw_peer;
            match query {
                Query::FindNode { want, .. } | Query::GetPeers { want, .. } => {
                    let want = want.expect("explicit outbound family");
                    assert!(if contact.address.is_ipv6() {
                        want.ipv6
                    } else {
                        want.ipv4
                    });
                    fields.insert(
                        if contact.address.is_ipv6() {
                            b"nodes6"
                        } else {
                            b"nodes"
                        },
                        bytes(b""),
                    );
                    if matches!(query, Query::GetPeers { .. }) {
                        fields.insert(b"token", bytes(b"token"));
                        if let Some(peer) = peer {
                            raw_peer = match peer {
                                SocketAddr::V4(p) => compact::encode_v4(p).to_vec(),
                                SocketAddr::V6(p) => compact::encode_v6(p).unwrap().to_vec(),
                            };
                            fields
                                .insert(b"values", Node::new(Value::List(vec![bytes(&raw_peer)])));
                        }
                    }
                }
                Query::AnnouncePeer { token, port, .. } => {
                    assert_eq!(token, b"token");
                    assert!(port.unwrap() > 0);
                    assert_eq!(source.is_ipv6(), contact.address.is_ipv6());
                    count.fetch_add(1, Ordering::SeqCst);
                }
                Query::Ping => {}
                _ => panic!("unexpected DHT method"),
            }
            let wire = Message::new(
                request.transaction_id(),
                Body::Response(Node::new(Value::Dictionary(fields))),
                None,
                16,
                100,
            )
            .unwrap()
            .encode(Default::default())
            .unwrap();
            socket.send_to(&wire, source).await.unwrap();
        }
    });
    (contact, announced, task)
}

#[tokio::test]
async fn ipv6_announcements_tokens_family_defaults_and_reply_budget() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut seeds = Vec::new();
        let mut tasks = Vec::new();
        for listen in ["127.0.0.1:0", "[::1]:0"] {
            for id in 1..=8 {
                let (contact, _, task) = remote(listen, None, NodeId([id; 20])).await;
                seeds.push(contact);
                tasks.push(task);
            }
        }
        let saved = SavedState {
            id: NodeId([254; 20]),
            nodes: seeds,
        };
        let (session, _root, _handle) = start_node("[::]:0", Some(saved), Vec::new()).await;
        contacts(&session, 16).await;
        let destination = format!("[::1]:{}", session.local_addr().port())
            .parse()
            .unwrap();
        let client = UdpSocket::bind("[::1]:0").await.unwrap();
        let wire = rpc(
            &client,
            destination,
            b"find_node",
            vec![(b"target", bytes(&[1; 20]))],
        )
        .await;
        assert!(matches!(
            response(&wire).kind(),
            Kind::Response {
                nodes: None,
                nodes6: Some(_),
                ..
            }
        ));
        let hash = [2; 20];
        let wire = rpc(
            &client,
            destination,
            b"get_peers",
            vec![(b"info_hash", bytes(&hash))],
        )
        .await;
        let token = match response(&wire).kind() {
            Kind::Response {
                token: Some(token), ..
            } => token.to_vec(),
            _ => panic!("token"),
        };
        // One more peer than fits in a 1024-byte reply.
        for port in 10000..10019 {
            let wire = rpc(
                &client,
                destination,
                b"announce_peer",
                vec![
                    (b"info_hash", bytes(&hash)),
                    (b"token", bytes(&token)),
                    (b"port", Node::new(Value::Integer(port))),
                ],
            )
            .await;
            assert!(matches!(response(&wire).kind(), Kind::Response { .. }));
        }
        // want selects node referrals, not the family of returned peers.
        let wire = rpc(
            &client,
            destination,
            b"get_peers",
            vec![
                (b"info_hash", bytes(&hash)),
                (
                    b"want",
                    Node::new(Value::List(vec![bytes(b"n4"), bytes(b"n6")])),
                ),
            ],
        )
        .await;
        let message = response(&wire);
        let peers: Vec<_> = message.peers().unwrap().collect();
        assert_eq!(peers.len(), 17);
        assert_eq!(
            message.observed_address(),
            Some(client.local_addr().unwrap())
        );
        assert!(
            wire.len() + 21 > 1024,
            "another compact IPv6 peer must not fit"
        );
        assert!(peers.iter().all(SocketAddr::is_ipv6));
        assert!(matches!(
            message.kind(),
            Kind::Response {
                nodes: Some(_),
                nodes6: Some(_),
                ..
            }
        ));
        let wire = rpc(
            &client,
            destination,
            b"announce_peer",
            vec![
                (b"info_hash", bytes(&[3; 20])),
                (b"token", bytes(&token)),
                (b"port", Node::new(Value::Integer(6881))),
            ],
        )
        .await;
        assert!(matches!(
            response(&wire).kind(),
            Kind::Error { code: 203, .. }
        ));
        assert_eq!(
            response(&wire).observed_address(),
            Some(client.local_addr().unwrap())
        );
        let wire = rpc(&client, destination, b"ping", Vec::new()).await;
        assert!(matches!(response(&wire).kind(), Kind::Response { .. }));
        session.shutdown().await.unwrap();
        for task in tasks {
            task.abort();
        }
    })
    .await
    .expect("IPv6 DHT serving");
}

#[tokio::test]
async fn dual_stack_routing_is_separate_and_saved_contacts_are_reverified() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let (v4, _, task4) = remote("127.0.0.1:0", None, NodeId([9; 20])).await;
        let (v6, _, task6) = remote("[::1]:0", None, NodeId([9; 20])).await;
        assert_eq!(v4.id, v6.id);
        let saved = SavedState {
            id: NodeId([7; 20]),
            nodes: vec![v4, v6],
        };
        let (session, _root, _handle) = start_node("[::]:0", Some(saved), Vec::new()).await;
        let saved = contacts(&session, 2).await;
        assert!(saved.nodes.contains(&v4) && saved.nodes.contains(&v6));
        let port = session.local_addr().port();
        for (listen, destination, want) in [
            (
                "127.0.0.1:0",
                format!("127.0.0.1:{port}"),
                vec![bytes(b"n6")],
            ),
            (
                "[::1]:0",
                format!("[::1]:{port}"),
                vec![bytes(b"n4"), bytes(b"n6")],
            ),
        ] {
            let client = UdpSocket::bind(listen).await.unwrap();
            let wire = rpc(
                &client,
                destination.parse().unwrap(),
                b"find_node",
                vec![
                    (b"target", bytes(&[1; 20])),
                    (b"want", Node::new(Value::List(want))),
                ],
            )
            .await;
            let message = response(&wire);
            let Kind::Response { nodes, nodes6, .. } = message.kind() else {
                panic!("find_node response")
            };
            let node = nodes6.unwrap().iter().next().unwrap();
            assert_eq!(SocketAddr::V6(node.address), v6.address);
            if client.local_addr().unwrap().is_ipv6() {
                assert_eq!(
                    SocketAddr::V4(nodes.unwrap().iter().next().unwrap().address),
                    v4.address
                );
            } else {
                assert!(nodes.is_none());
            }
        }
        // A token issued over IPv6 cannot authorize an IPv4 announcement.
        let client6 = UdpSocket::bind("[::1]:0").await.unwrap();
        let wire = rpc(
            &client6,
            format!("[::1]:{port}").parse().unwrap(),
            b"get_peers",
            vec![(b"info_hash", bytes(&[4; 20]))],
        )
        .await;
        let token = match response(&wire).kind() {
            Kind::Response { token: Some(t), .. } => t.to_vec(),
            _ => panic!("token"),
        };
        let wire = rpc(
            &client6,
            format!("[::1]:{port}").parse().unwrap(),
            b"announce_peer",
            vec![
                (b"info_hash", bytes(&[4; 20])),
                (b"token", bytes(&token)),
                (b"port", Node::new(Value::Integer(0))),
                (b"implied_port", Node::new(Value::Integer(1))),
            ],
        )
        .await;
        assert!(matches!(response(&wire).kind(), Kind::Response { .. }));
        let wire = rpc(
            &client6,
            format!("[::1]:{port}").parse().unwrap(),
            b"get_peers",
            vec![
                (b"info_hash", bytes(&[4; 20])),
                (
                    b"want",
                    Node::new(Value::List(vec![bytes(b"n4"), bytes(b"n6")])),
                ),
            ],
        )
        .await;
        let message = response(&wire);
        assert!(matches!(
            message.kind(),
            Kind::Response {
                nodes: Some(_),
                nodes6: Some(_),
                ..
            }
        ));
        assert_eq!(
            message.peers().unwrap().collect::<Vec<_>>(),
            vec![client6.local_addr().unwrap()]
        );
        let client4 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let wire = rpc(
            &client4,
            format!("127.0.0.1:{port}").parse().unwrap(),
            b"get_peers",
            vec![(b"info_hash", bytes(&[4; 20]))],
        )
        .await;
        assert!(response(&wire).peers().is_none());
        let wire = rpc(
            &client4,
            format!("127.0.0.1:{port}").parse().unwrap(),
            b"announce_peer",
            vec![
                (b"info_hash", bytes(&[4; 20])),
                (b"token", bytes(&token)),
                (b"port", Node::new(Value::Integer(6881))),
            ],
        )
        .await;
        assert!(matches!(
            response(&wire).kind(),
            Kind::Error { code: 203, .. }
        ));
        session.shutdown().await.unwrap();
        let saved = session.dht_state().await.unwrap().unwrap();
        let (restored, _root, _handle) =
            start_node("[::]:0", Some(saved.clone()), Vec::new()).await;
        let verified = contacts(&restored, 2).await;
        assert_eq!(verified.id, saved.id);
        assert!(verified.nodes.contains(&v4) && verified.nodes.contains(&v6));
        restored.shutdown().await.unwrap();
        task4.abort();
        task6.abort();
    })
    .await
    .expect("dual-stack DHT restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipv6_router_bootstrap_discovers_download_and_magnet_peers() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let root = Temp::new();
        let payload = b"IPv6 DHT download";
        std::fs::write(root.0.join("payload"), payload).unwrap();
        let meta = Profile::default()
            .parse_owned(&seed_metainfo("payload", payload, None))
            .unwrap();
        let seed = Session::bind(
            NetworkOptions {
                listen: "[::1]:0".parse().unwrap(),
                utp: false,
                allow_local_network: true,
                ..Default::default()
            },
            Limits::default(),
        )
        .await
        .unwrap();
        let mut options = TorrentOptions::new(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Adopt(root.0.clone()),
        );
        options.settings = settings(false);
        seed.admit(options).unwrap().wait_complete().await.unwrap();
        // Routers are seeds only; the referred contact enters the routing table.
        let (referral, announced, referral_task) =
            remote("[::1]:0", Some(seed.local_addr()), NodeId([9; 20])).await;
        let router = UdpSocket::bind("[::1]:0").await.unwrap();
        let router_address = router.local_addr().unwrap();
        let router_task = tokio::spawn(async move {
            let mut buffer = [0; 4096];
            loop {
                let (length, source) = router.recv_from(&mut buffer).await.unwrap();
                let request = response(&buffer[..length]);
                let SocketAddr::V6(address) = referral.address else {
                    unreachable!()
                };
                let nodes = spill::protocol::dht::krpc::NodeV6 {
                    id: referral.id,
                    address,
                }
                .encode()
                .unwrap();
                let wire = Message::new(
                    request.transaction_id(),
                    Body::Response(dict([
                        (b"id".as_slice(), bytes(&[8; 20])),
                        (b"nodes6", bytes(&nodes)),
                    ])),
                    None,
                    16,
                    100,
                )
                .unwrap()
                .encode(Default::default())
                .unwrap();
                router.send_to(&wire, source).await.unwrap();
            }
        });
        for (listen, magnet) in [
            ("[::1]:0", false),
            ("[::1]:0", true),
            ("[::]:0", false),
            ("[::]:0", true),
        ] {
            let (session, _warm_root, warm) =
                start_node(listen, None, vec![router_address.to_string()]).await;
            contacts(&session, 1).await;
            warm.stop().await.unwrap();
            let destination = Temp::new();
            let source = if magnet {
                TorrentSource::Magnet(
                    Magnet::parse(
                        &format!("magnet:?xt=urn:btih:{}", meta.info_hash()),
                        Default::default(),
                    )
                    .unwrap(),
                )
            } else {
                TorrentSource::Metainfo(meta.clone())
            };
            let mut options =
                TorrentOptions::new(source, Destination::Create(destination.0.clone()));
            options.settings = settings(true);
            let download = session.admit(options).unwrap();
            download.wait_complete().await.unwrap();
            assert_eq!(
                std::fs::read(destination.0.join("payload")).unwrap(),
                payload
            );
            tokio::time::timeout(Duration::from_secs(2), async {
                while announced.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("IPv6 announcement");
            session.shutdown().await.unwrap();
        }
        seed.shutdown().await.unwrap();
        referral_task.abort();
        router_task.abort();
    })
    .await
    .expect("IPv6 DHT-only transfers");
}

#[tokio::test]
async fn cross_family_referrals_cannot_reserve_ids_ahead_of_verified_contacts() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for source_ipv6 in [false, true] {
            let bridge = UdpSocket::bind(if source_ipv6 {
                "[::1]:0"
            } else {
                "127.0.0.1:0"
            })
            .await
            .unwrap();
            let target = UdpSocket::bind(if source_ipv6 {
                "127.0.0.1:0"
            } else {
                "[::1]:0"
            })
            .await
            .unwrap();
            let contact = Contact {
                id: NodeId([9; 20]),
                address: target.local_addr().unwrap(),
            };
            let seed = Contact {
                id: NodeId([8; 20]),
                address: bridge.local_addr().unwrap(),
            };
            let task = tokio::spawn(async move {
                let mut buffer = [0; 4096];
                loop {
                    let (length, source) = bridge.recv_from(&mut buffer).await.unwrap();
                    let request = response(&buffer[..length]);
                    let Kind::Query { query, .. } = request.kind() else {
                        panic!("bridge request")
                    };
                    match query {
                        Query::FindNode { want, .. } => {
                            let want = want.unwrap();
                            assert!(want.ipv4 && want.ipv6);
                        }
                        Query::GetPeers { want, .. } => {
                            let want = want.unwrap();
                            assert_eq!(want.ipv6, source_ipv6);
                            assert_eq!(want.ipv4, !source_ipv6);
                        }
                        _ => {}
                    }
                    let (nodes, nodes6) = match contact.address {
                        SocketAddr::V4(address) => (
                            spill::protocol::dht::krpc::NodeV4 {
                                id: contact.id,
                                address,
                            }
                            .encode()
                            .to_vec(),
                            Vec::new(),
                        ),
                        SocketAddr::V6(address) => (
                            Vec::new(),
                            spill::protocol::dht::krpc::NodeV6 {
                                id: contact.id,
                                address,
                            }
                            .encode()
                            .unwrap()
                            .to_vec(),
                        ),
                    };
                    let wire = Message::new(
                        request.transaction_id(),
                        Body::Response(dict([
                            (b"id".as_slice(), bytes(&seed.id.0)),
                            (b"nodes", bytes(&nodes)),
                            (b"nodes6", bytes(&nodes6)),
                        ])),
                        None,
                        16,
                        100,
                    )
                    .unwrap()
                    .encode(Default::default())
                    .unwrap();
                    bridge.send_to(&wire, source).await.unwrap();
                }
            });
            let saved = SavedState {
                id: NodeId([7; 20]),
                nodes: vec![seed],
            };
            let (session, _root, _warm) = start_node("[::]:0", Some(saved), Vec::new()).await;
            let saved = contacts(&session, 1).await;
            assert_eq!(saved.nodes, vec![seed]);
            let mut buffer = [0; 4096];
            let (length, _) = target.recv_from(&mut buffer).await.unwrap();
            let request = response(&buffer[..length]);
            assert!(matches!(
                request.kind(),
                Kind::Query {
                    query: Query::Ping,
                    ..
                }
            ));
            assert!(
                !session
                    .dht_state()
                    .await
                    .unwrap()
                    .unwrap()
                    .nodes
                    .contains(&contact)
            );
            // The referral falsely pairs a real ID with this silent endpoint.
            // A genuine node at a different endpoint must still be admitted.
            let real = UdpSocket::bind(if source_ipv6 {
                "127.0.0.1:0"
            } else {
                "[::1]:0"
            })
            .await
            .unwrap();
            let genuine = Contact {
                id: contact.id,
                address: real.local_addr().unwrap(),
            };
            let destination = SocketAddr::new(genuine.address.ip(), session.local_addr().port());
            let wire = Message::new(
                b"real",
                Body::Query {
                    method: b"ping",
                    arguments: dict([(b"id".as_slice(), bytes(&genuine.id.0))]),
                },
                None,
                16,
                100,
            )
            .unwrap()
            .encode(Default::default())
            .unwrap();
            real.send_to(&wire, destination).await.unwrap();
            let mut buffer = [0; 4096];
            loop {
                let (length, source) = real.recv_from(&mut buffer).await.unwrap();
                let request = response(&buffer[..length]);
                if matches!(
                    request.kind(),
                    Kind::Query {
                        query: Query::Ping,
                        ..
                    }
                ) {
                    let wire = Message::new(
                        request.transaction_id(),
                        Body::Response(dict([(b"id".as_slice(), bytes(&genuine.id.0))])),
                        None,
                        16,
                        100,
                    )
                    .unwrap()
                    .encode(Default::default())
                    .unwrap();
                    real.send_to(&wire, source).await.unwrap();
                    break;
                }
            }
            let saved = contacts(&session, 2).await;
            assert!(saved.nodes.contains(&genuine));
            assert!(!saved.nodes.contains(&contact));
            session.shutdown().await.unwrap();
            task.abort();
        }
    })
    .await
    .expect("cross-family DHT verification");
}

#[tokio::test]
async fn unreachable_bootstrap_backs_off_instead_of_retrying_every_thirty_seconds() {
    let router = UdpSocket::bind("[::1]:0").await.unwrap();
    let (session, _root, _warm) = start_node(
        "[::1]:0",
        None,
        vec![router.local_addr().unwrap().to_string()],
    )
    .await;
    let mut buffer = [0; 4096];
    tokio::time::timeout(Duration::from_secs(2), router.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    tokio::time::pause();
    async fn advance(seconds: u64) {
        tokio::time::advance(Duration::from_secs(seconds)).await;
        // Drain runnable node, bootstrap and discovery futures without advancing time.
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    }
    advance(6).await; // first transaction times out; next attempt is due in 60s
    advance(30).await;
    assert_eq!(
        router.try_recv_from(&mut buffer).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    advance(30).await;
    router
        .try_recv_from(&mut buffer)
        .expect("second bootstrap round");
    advance(6).await; // second timeout doubles the retry delay to 120s
    for _ in 0..3 {
        advance(30).await;
        assert_eq!(
            router.try_recv_from(&mut buffer).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    advance(30).await;
    router
        .try_recv_from(&mut buffer)
        .expect("third bootstrap round");
    tokio::time::resume();
    session.shutdown().await.unwrap();
}
