use crate::payload::{Temp, seed_metainfo};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, Session},
        torrent::{Destination, TorrentHandle, TorrentOptions, TorrentSource},
    },
    protocol::{
        bencode::{self, Node, Value},
        compact,
        metainfo::OwnedMetainfo,
        peer::{
            Handshake, extension,
            holepunch::{self, Kind, Message},
        },
        utp::{Kind as UtpKind, Packet},
    },
};
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    sync::oneshot,
};

struct Peer {
    socket: TcpStream,
    address: SocketAddr,
    advertised: bool,
}
impl Peer {
    async fn new(session: &Session, meta: &OwnedMetainfo, id: u8, support: bool) -> Self {
        let mut socket = TcpStream::connect(session.local_addr()).await.unwrap();
        let address = socket.local_addr().unwrap();
        socket
            .write_all(
                &Handshake {
                    reserved: [0, 0, 0, 0, 0, 0x10, 0, 0],
                    info_hash: meta.info_hash().wire_key(),
                    peer_id: [id; 20],
                }
                .encode(),
            )
            .await
            .unwrap();
        let mut hello = [0; 68];
        socket.read_exact(&mut hello).await.unwrap();
        assert_eq!(
            Handshake::decode(&hello).unwrap().0.info_hash,
            meta.info_hash().wire_key()
        );
        let capability = if support { "12:ut_holepunchi7e" } else { "" };
        let payload = format!(
            "\0d1:md{capability}11:ut_metadatai1e6:ut_pexi2ee1:pi{}ee",
            address.port()
        );
        send(&mut socket, payload.as_bytes()).await;
        let advertised = loop {
            let bytes = frame(&mut socket).await;
            if bytes.starts_with(&[20, 0]) {
                let hello = extension::Handshake::parse(&bytes[2..], Default::default()).unwrap();
                assert!(matches!(hello.extension_id(b"ut_holepunch"), Some(0 | 3)));
                assert!(matches!(hello.extension_id(b"ut_pex"), Some(0 | 2)));
                let mut map = extension::ExtensionMap::new(&[(b"ut_holepunch", 0)]).unwrap();
                map.apply(&hello).unwrap();
                break map.id_for(b"ut_holepunch") == Some(3);
            }
        };
        send(&mut socket, b"\x01d8:msg_typei0e5:piecei0ee").await;
        while !frame(&mut socket).await.starts_with(&[20, 1]) {}
        Self {
            socket,
            address,
            advertised,
        }
    }
    async fn punch(&mut self, kind: Kind, addr: SocketAddr) {
        let mut payload = vec![3];
        payload.extend_from_slice(&Message { kind, addr }.encode().unwrap());
        send(&mut self.socket, &payload).await;
    }
    async fn next(&mut self) -> Message {
        loop {
            let bytes = frame(&mut self.socket).await;
            if bytes.starts_with(&[20, 7]) {
                return Message::parse(&bytes[2..]).unwrap();
            }
        }
    }
    async fn pex(&mut self, contacts: &[(SocketAddr, u8)]) {
        let mut lists = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for &(address, flags) in contacts {
            match address {
                SocketAddr::V4(a) => {
                    lists[0].extend_from_slice(&compact::encode_v4(a));
                    lists[2].push(flags);
                }
                SocketAddr::V6(a) => {
                    lists[1].extend_from_slice(&compact::encode_v6(a).unwrap());
                    lists[3].push(flags);
                }
            }
        }
        let fields = [b"added".as_slice(), b"added6", b"added.f", b"added6.f"]
            .into_iter()
            .zip(&lists)
            .map(|(key, bytes)| (key, Node::new(Value::Bytes(bytes))))
            .collect::<BTreeMap<_, _>>();
        let mut payload = vec![2];
        payload.extend_from_slice(
            &bencode::encode(&Node::new(Value::Dictionary(fields)), Default::default()).unwrap(),
        );
        send(&mut self.socket, &payload).await;
    }
}
async fn send(socket: &mut TcpStream, extended: &[u8]) {
    socket.write_u32((extended.len() + 1) as u32).await.unwrap();
    socket.write_u8(20).await.unwrap();
    socket.write_all(extended).await.unwrap();
}
async fn frame(socket: &mut TcpStream) -> Vec<u8> {
    let length = socket.read_u32().await.unwrap();
    assert!(length < 1024 * 1024);
    let mut bytes = vec![0; length as usize];
    socket.read_exact(&mut bytes).await.unwrap();
    bytes
}
async fn session(utp: bool, allow_local_network: bool) -> Session {
    Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            utp,
            allow_local_network,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap()
}
fn options(meta: &OwnedMetainfo, root: &Temp, seed: bool) -> TorrentOptions {
    let mut options = TorrentOptions::new(
        TorrentSource::Metainfo(meta.clone()),
        if seed {
            Destination::Adopt(root.0.clone())
        } else {
            Destination::Open(root.0.clone())
        },
    );
    options.settings.trackers = false;
    options.settings.dht = false;
    options.settings.web_seeds = false;
    options.diagnostics = true;
    options
}
async fn ready(handle: &mut TorrentHandle) {
    while !handle.snapshot().progress.ready {
        assert!(handle.changed().await);
    }
}
fn metainfo() -> OwnedMetainfo {
    Profile::default()
        .parse_owned(&seed_metainfo("x", &[42; 128 * 1024], None))
        .unwrap()
}
fn mapped(address: SocketAddr) -> SocketAddr {
    let SocketAddr::V4(a) = address else {
        unreachable!()
    };
    SocketAddr::new(a.ip().to_ipv6_mapped().into(), a.port())
}

#[tokio::test]
async fn simultaneous_punch_reaches_an_idle_seed_through_filtered_udp_mappings() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let meta = metainfo();
        let a = session(true, true).await;
        let b = session(true, true).await;
        let root_a = Temp::new();
        let root_b = Temp::new();
        std::fs::write(root_b.0.join("x"), [42; 128 * 1024]).unwrap();
        let mut leecher = a.admit(options(&meta, &root_a, false)).unwrap();
        let seed = b.admit(options(&meta, &root_b, true)).unwrap();
        ready(&mut leecher).await;
        seed.wait_complete().await.unwrap();
        let mut relay_a = Peer::new(&a, &meta, 81, true).await;
        let mut relay_b = Peer::new(&b, &meta, 82, true).await;
        assert!(relay_a.advertised && relay_b.advertised);
        let pa = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pb = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let public_a = pa.local_addr().unwrap();
        let public_b = pb.local_addr().unwrap();
        let (open, mut opened) = oneshot::channel();
        let (seen, observed) = oneshot::channel();
        let local_a = a.local_addr();
        let local_b = b.local_addr();
        let filter = tokio::spawn(async move {
            let mut buffer_a = [0; 65536];
            let mut buffer_b = [0; 65536];
            let mut active = false;
            let mut syns = [None, None];
            let mut seen = Some(seen);
            loop {
                let (direction, n) = tokio::select! {
                    biased;
                    _ = &mut opened, if !active => {
                        while pa.try_recv_from(&mut buffer_a).is_ok() {}
                        while pb.try_recv_from(&mut buffer_b).is_ok() {}
                        active = true;
                        continue;
                    }
                    result = pa.recv_from(&mut buffer_a) => { let (n, from) = result.unwrap(); assert_eq!(from, local_b); (0, n) }
                    result = pb.recv_from(&mut buffer_b) => { let (n, from) = result.unwrap(); assert_eq!(from, local_a); (1, n) }
                };
                if !active { continue; }
                let bytes = if direction == 0 { &buffer_a[..n] } else { &buffer_b[..n] };
                let packet = Packet::decode(bytes).unwrap();
                if packet.header.kind == UtpKind::Syn {
                    syns[direction] = Some(packet.header.connection_id);
                }
                if syns.iter().all(Option::is_some) {
                    if let Some(seen) = seen.take() { seen.send(syns).unwrap(); }
                    if direction == 0 { pb.send_to(bytes, local_a).await.unwrap(); }
                    else { pa.send_to(bytes, local_b).await.unwrap(); }
                }
            }
        });
        relay_a.pex(&[(public_b, 0x0c)]).await;
        assert_eq!(relay_a.next().await, Message { kind: Kind::Rendezvous, addr: public_b });
        open.send(()).unwrap();
        relay_a.punch(Kind::Connect, public_b).await;
        relay_b.punch(Kind::Connect, public_a).await;
        assert!(observed.await.unwrap().iter().all(Option::is_some));
        leecher.wait_complete().await.unwrap();
        assert_eq!(std::fs::read(root_a.0.join("x")).unwrap(), [42; 128 * 1024]);
        while !leecher.snapshot().diagnostics.as_ref().unwrap().peers.iter()
            .any(|p| p.detail.established && p.transport == "utp") {
            assert!(leecher.changed().await);
        }
        let diagnostics = leecher.snapshot().diagnostics.unwrap();
        assert!(diagnostics.sampled_at.is_some());
        assert!(diagnostics.storage.is_some());
        assert!(diagnostics.holepunch.rendezvous_started >= 1);
        assert!(diagnostics.holepunch.punches_queued >= 1);
        leecher.stop().await.unwrap();
        seed.stop().await.unwrap();
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
        filter.abort();
    }).await.expect("direct UDP holepunch and seed wakeup");
}

#[tokio::test]
async fn relay_uses_negotiated_ids_canonical_endpoints_and_bounded_windows() {
    tokio::time::timeout(Duration::from_secs(18), async {
        let meta = metainfo();
        let relay = session(true, true).await;
        let root = Temp::new();
        let mut handle = relay.admit(options(&meta, &root, false)).unwrap();
        ready(&mut handle).await;
        let mut requester = Peer::new(&relay, &meta, 81, true).await;
        let mut target = Peer::new(&relay, &meta, 82, true).await;
        let unsupported = Peer::new(&relay, &meta, 83, false).await;
        requester
            .punch(Kind::Rendezvous, mapped(target.address))
            .await;
        assert_eq!(
            requester.next().await,
            Message {
                kind: Kind::Connect,
                addr: target.address
            }
        );
        assert_eq!(
            target.next().await,
            Message {
                kind: Kind::Connect,
                addr: requester.address
            }
        );
        let unknown = mapped("127.0.0.1:1".parse().unwrap());
        for (address, error) in [
            (unsupported.address, holepunch::NO_SUPPORT),
            (unknown, holepunch::NO_SUCH_PEER),
            (requester.address, holepunch::NO_SELF),
        ] {
            requester.punch(Kind::Rendezvous, address).await;
            assert_eq!(
                requester.next().await,
                Message {
                    kind: Kind::Error(error),
                    addr: address
                }
            );
        }
        requester.punch(Kind::Rendezvous, target.address).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(150), requester.next())
                .await
                .is_err()
        );
        let mut probes = Vec::new();
        let mut tcp = Vec::new();
        for _ in 0..5 {
            let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            tcp.push(
                tokio::net::TcpListener::bind(probe.local_addr().unwrap())
                    .await
                    .unwrap(),
            );
            requester
                .punch(Kind::Connect, probe.local_addr().unwrap())
                .await;
            probes.push(probe);
        }
        let mut bytes = [0; 1500];
        for probe in &probes[..4] {
            let (n, _) = probe.recv_from(&mut bytes).await.unwrap();
            assert_eq!(
                Packet::decode(&bytes[..n]).unwrap().header.kind,
                UtpKind::Syn
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(150), probes[4].recv_from(&mut bytes))
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_secs(10)).await;
        for listener in &tcp {
            assert!(
                tokio::time::timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err(),
                "punch must never fall back to TCP"
            );
        }
        requester
            .punch(Kind::Connect, probes[4].local_addr().unwrap())
            .await;
        let (n, _) = probes[4].recv_from(&mut bytes).await.unwrap();
        assert_eq!(
            Packet::decode(&bytes[..n]).unwrap().header.kind,
            UtpKind::Syn
        );
        requester.punch(Kind::Rendezvous, target.address).await;
        assert_eq!(requester.next().await.kind, Kind::Connect);
        assert_eq!(target.next().await.kind, Kind::Connect);

        let mapped_peer = session(true, true).await;
        let mapped_root = Temp::new();
        let mapping = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let public = mapping.local_addr().unwrap();
        let local_relay = relay.local_addr();
        let local_target = mapped_peer.local_addr();
        let proxy = tokio::spawn(async move {
            let mut bytes = [0; 65536];
            loop {
                let (n, from) = mapping.recv_from(&mut bytes).await.unwrap();
                let destination = if from == local_target {
                    local_relay
                } else {
                    assert_eq!(from, local_relay);
                    local_target
                };
                mapping.send_to(&bytes[..n], destination).await.unwrap();
            }
        });
        let mut opts = options(&meta, &mapped_root, false);
        opts.peers = vec![public];
        opts.profile.peers = 1;
        let mut mapped_handle = mapped_peer.admit(opts).unwrap();
        loop {
            if mapped_handle
                .snapshot()
                .diagnostics
                .as_ref()
                .is_some_and(|d| {
                    d.peers
                        .iter()
                        .any(|p| p.transport == "utp" && p.detail.established)
                })
            {
                break;
            }
            assert!(mapped_handle.changed().await);
        }
        requester.punch(Kind::Rendezvous, local_target).await;
        assert_eq!(
            requester.next().await,
            Message {
                kind: Kind::Connect,
                addr: public
            }
        );
        mapped_handle.stop().await.unwrap();
        mapped_peer.shutdown().await.unwrap();
        proxy.abort();
        handle.stop().await.unwrap();
        relay.shutdown().await.unwrap();
    })
    .await
    .expect("relay errors, normalization and replenished flood limit");
}

#[tokio::test]
async fn failed_punch_does_not_recurse_and_relay_pending_slot_is_reusable() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let meta = metainfo();
        let session = session(true, true).await;
        let root = Temp::new();
        let mut opts = options(&meta, &root, false);
        opts.profile.peers = 3;
        let mut handle = session.admit(opts).unwrap();
        ready(&mut handle).await;
        let mut relay = Peer::new(&session, &meta, 81, true).await;
        let mut sockets = Vec::new();
        for _ in 0..3 {
            sockets.push(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        }
        sockets.sort_by_key(|socket| {
            spill::protocol::peer::priority::priority(
                session.local_addr(),
                socket.local_addr().unwrap(),
            )
        });
        let no_flag = sockets.pop().unwrap();
        let x = sockets.pop().unwrap();
        let y = sockets.pop().unwrap();
        let addr_x = x.local_addr().unwrap();
        let addr_y = y.local_addr().unwrap();
        relay
            .pex(&[
                (mapped(no_flag.local_addr().unwrap()), 0x04),
                (mapped(addr_x), 0x0c),
                (mapped(addr_y), 0x0c),
            ])
            .await;
        assert_eq!(
            relay.next().await,
            Message {
                kind: Kind::Rendezvous,
                addr: addr_x
            }
        );
        relay.punch(Kind::Connect, addr_x).await;
        relay.punch(Kind::Connect, mapped(addr_x)).await;
        assert_eq!(
            relay.next().await,
            Message {
                kind: Kind::Rendezvous,
                addr: addr_y
            }
        );
        tokio::time::sleep(Duration::from_millis(3400)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(150), relay.next())
                .await
                .is_err()
        );
        let mut bytes = [0; 1500];
        let mut connections = HashSet::new();
        while let Ok((n, _)) = x.try_recv_from(&mut bytes) {
            let packet = Packet::decode(&bytes[..n]).unwrap();
            if packet.header.kind == UtpKind::Syn {
                connections.insert(packet.header.connection_id);
            }
        }
        assert_eq!(
            connections.len(),
            2,
            "one ordinary dial and one punch despite duplicate connect"
        );
        handle.stop().await.unwrap();
        session.shutdown().await.unwrap();
    })
    .await
    .expect("one-shot failure and exact-match relay cleanup");
}

#[tokio::test]
async fn unsolicited_punches_respect_pex_utp_and_local_network_policy() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for (utp, pex, local, private) in [
            (true, false, true, false),
            (false, true, true, false),
            (true, true, false, false),
            (true, true, true, true),
        ] {
            let mut bytes = seed_metainfo("x", &[42; 128 * 1024], None);
            if private {
                bytes.splice(
                    bytes.len() - 2..bytes.len() - 2,
                    b"7:privatei1e".iter().copied(),
                );
            }
            let meta = Profile::default().parse_owned(&bytes).unwrap();
            let session = session(utp, local).await;
            let root = Temp::new();
            let mut opts = options(&meta, &root, false);
            opts.settings.pex = pex;
            let mut handle = session.admit(opts).unwrap();
            ready(&mut handle).await;
            let mut relay = Peer::new(&session, &meta, 81, true).await;
            let mut target = Peer::new(&session, &meta, 82, true).await;
            assert_eq!(relay.advertised, utp && pex && !private);
            let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let tcp = tokio::net::TcpListener::bind(udp.local_addr().unwrap())
                .await
                .unwrap();
            relay.punch(Kind::Connect, udp.local_addr().unwrap()).await;
            if !utp || !pex || private {
                relay.punch(Kind::Rendezvous, target.address).await;
                assert!(
                    tokio::time::timeout(Duration::from_millis(150), target.next())
                        .await
                        .is_err()
                );
            }
            let mut bytes = [0; 1500];
            assert!(
                tokio::time::timeout(Duration::from_millis(200), udp.recv_from(&mut bytes))
                    .await
                    .is_err()
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(150), tcp.accept())
                    .await
                    .is_err()
            );
            handle.stop().await.unwrap();
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("local enablement and egress policy");
}

async fn reset(socket: &UdpSocket) {
    let mut bytes = [0; 1500];
    let (n, address) = socket.recv_from(&mut bytes).await.unwrap();
    let mut header = Packet::decode(&bytes[..n]).unwrap().header;
    assert_eq!(header.kind, UtpKind::Syn);
    header.kind = UtpKind::Reset;
    let mut reply = Vec::new();
    Packet {
        header,
        sack: None,
        payload: &[],
    }
    .encode(&mut reply);
    socket.send_to(&reply, address).await.unwrap();
}

#[tokio::test]
async fn queued_punch_upgrades_keep_priority_and_restore_original_dials_after_expiry() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for (expire, pex) in [(false, false), (true, false), (true, true)] {
            let meta = metainfo();
            let session = Session::bind(
                NetworkOptions {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    allow_local_network: true,
                    ..Default::default()
                },
                Limits {
                    pending_dials: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let mut sockets = Vec::new();
            for _ in 0..4 {
                sockets.push(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            }
            sockets.sort_by_key(|socket| {
                spill::protocol::peer::priority::priority(
                    session.local_addr(),
                    socket.local_addr().unwrap(),
                )
            });
            let guard = sockets.pop().unwrap();
            let other = sockets.pop().unwrap();
            let ordinary = sockets.pop().unwrap();
            let punch = sockets.pop().unwrap();
            let tcp = tokio::net::TcpListener::bind(punch.local_addr().unwrap())
                .await
                .unwrap();
            let second = Profile::default()
                .parse_owned(&seed_metainfo("y", b"other torrent", None))
                .unwrap();
            let second_root = Temp::new();
            let mut second_opts = options(&second, &second_root, false);
            second_opts.peers = vec![guard.local_addr().unwrap(), other.local_addr().unwrap()];
            let mut second_handle = session.admit(second_opts).unwrap();
            ready(&mut second_handle).await;
            let mut bytes = [0; 1500];
            guard.peek_from(&mut bytes).await.unwrap();
            let root = Temp::new();
            let mut opts = options(&meta, &root, false);
            if !pex {
                opts.peers = vec![ordinary.local_addr().unwrap(), punch.local_addr().unwrap()];
            }
            let mut handle = session.admit(opts).unwrap();
            ready(&mut handle).await;
            let mut relay = Peer::new(&session, &meta, 81, true).await;
            if pex {
                relay
                    .pex(&[
                        (ordinary.local_addr().unwrap(), 0),
                        (punch.local_addr().unwrap(), 0),
                    ])
                    .await;
            }
            relay
                .punch(Kind::Connect, punch.local_addr().unwrap())
                .await;
            // Confirm admission while the other torrent holds the only permit.
            send(&mut relay.socket, b"\x01d8:msg_typei0e5:piecei0ee").await;
            while !frame(&mut relay.socket).await.starts_with(&[20, 1]) {}
            if expire {
                // Two unanswered uTP attempts hold the permit beyond the punch lease.
                reset(&ordinary).await;
                reset(&punch).await;
                let (socket, _) = tcp.accept().await.unwrap();
                drop(socket);
            } else {
                reset(&guard).await;
                loop {
                    tokio::select! {
                        _ = punch.peek_from(&mut bytes) => { reset(&punch).await; break; }
                        _ = reset(&other) => {},
                        _ = reset(&ordinary) => panic!("ordinary contact overtook queued punch"),
                    }
                }
                loop {
                    tokio::select! {
                        _ = reset(&ordinary) => break,
                        _ = reset(&other) => {},
                    }
                }
                assert!(
                    tokio::time::timeout(Duration::from_millis(150), tcp.accept())
                        .await
                        .is_err()
                );
            }
            handle.stop().await.unwrap();
            second_handle.stop().await.unwrap();
            session.shutdown().await.unwrap();
        }
    })
    .await
    .expect("queued upgrades, expiry and restored direct/PEX TCP fallback");
}
