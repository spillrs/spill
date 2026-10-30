use crate::common::{Daemon, WebSeed, metainfo, peer_frame, raw_peer};
use crate::payload::{Temp, seed_metainfo};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::time::Duration;

async fn mapping_daemon(root: &std::path::Path, gateway: &str, enabled: bool) -> Daemon {
    std::fs::write(root.join("config.toml"), format!(
        "state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"127.0.0.1:0\"\nallow_local_network=true\nport_mapping={enabled}\nport_mapping_gateway=\"{gateway}\"\ndht_bootstrap=[]\n[defaults]\ndht=false\npex=false\nweb_seeds=true\n",
        root.join("state"), root.join("downloads"), root.join("control.sock"),
    )).unwrap();
    Daemon::start_config(root).await
}

#[tokio::test]
async fn nat_pmp_maps_renews_advertises_and_deletes_on_shutdown() {
    use spill::protocol::{compact::PeersV4, tracker::udp};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
    let gateway = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let gateway_address = gateway.local_addr().unwrap();
    let granted = std::sync::Arc::new(std::sync::atomic::AtomicU16::new(0));
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
    let responder = tokio::spawn({
        let granted = granted.clone();
        let requests = requests.clone();
        async move {
            let mut bytes = [0; 64];
            loop {
                let (n, source) = gateway.recv_from(&mut bytes).await.unwrap();
                let request = &bytes[..n];
                requests.lock().unwrap().push(request.to_vec());
                assert_eq!(request[0], 0);
                let mut response = vec![0, request[1] + 128, 0, 0, 0, 0, 0, 1];
                if request[1] == 0 {
                    assert_eq!(n, 2);
                    response.extend_from_slice(&[203, 0, 113, 7]);
                } else {
                    assert_eq!(n, 12);
                    assert!(matches!(request[1], 1 | 2));
                    let external = granted.load(Ordering::Relaxed);
                    response.extend_from_slice(&request[4..6]);
                    let external = if external == 0 {
                        u16::from_be_bytes([request[6], request[7]])
                    } else {
                        external
                    };
                    response.extend_from_slice(&external.to_be_bytes());
                    let lifetime = u32::from_be_bytes(request[8..12].try_into().unwrap());
                    assert!(matches!(lifetime, 0 | 7200));
                    response.extend_from_slice(
                        &(if lifetime == 0 { 0u32 } else { 2u32 }).to_be_bytes(),
                    );
                }
                gateway.send_to(&response, source).await.unwrap();
            }
        }
    });
    let mut daemon = mapping_daemon(&root.0, &gateway_address.to_string(), true).await;
    let health = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let health = daemon.call(json!({"op":"health"})).await;
            if health["port_mapping"]["protocol"] == "nat-pmp" {
                break health;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let address: std::net::SocketAddr = health["listen"].as_str().unwrap().parse().unwrap();
    assert_eq!(health["advertised_port"], address.port());
    assert_eq!(health["port_mapping"]["external_tcp_port"], address.port());
    assert_eq!(health["port_mapping"]["external_udp_port"], address.port());
    assert_eq!(health["port_mapping"]["external_ip"], "203.0.113.7");
    assert!(health["port_mapping"]["next_renewal"].is_u64());

    let tracker = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", tracker.local_addr().unwrap());
    let (announces, mut received) = tokio::sync::mpsc::unbounded_channel();
    let tracker_task = tokio::spawn(async move {
        let codec = udp::Limits {
            max_packet_len: 65507,
            max_peers: 200,
        };
        let mut bytes = [0; 4096];
        loop {
            let (n, source) = tracker.recv_from(&mut bytes).await.unwrap();
            let request = udp::Request::decode(&bytes[..n], codec).unwrap();
            let kind = match request.kind {
                udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                    connection_id: 17,
                    extensions: &[],
                },
                udp::RequestKind::Announce { announce, .. } => {
                    announces.send((announce.port, announce.event)).unwrap();
                    udp::ResponseKind::Announce {
                        interval: 30,
                        seeders: 1,
                        leechers: 0,
                        peers: udp::Peers::V4(PeersV4::parse(&[], 0).unwrap()),
                    }
                }
                _ => panic!("unexpected tracker request"),
            };
            tracker
                .send_to(
                    &udp::Response {
                        transaction_id: request.transaction_id,
                        kind,
                    }
                    .encode(codec)
                    .unwrap(),
                    source,
                )
                .await
                .unwrap();
        }
    });
    let bytes = seed_metainfo("x", b"abc", Some(&url));
    let hash = *spill::protocol::metainfo::Metainfo::parse(&bytes, Default::default())
        .unwrap()
        .info_hash()
        .as_bytes();
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    let started = std::time::Instant::now();
    let first = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, (address.port(), udp::EVENT_STARTED));
    let external = if address.port() == 54321 {
        54322
    } else {
        54321
    };
    granted.store(external, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let health = daemon.call(json!({"op":"health"})).await;
            if health["advertised_port"] == external {
                assert_eq!(health["port_mapping"]["external_tcp_port"], external);
                assert_eq!(health["port_mapping"]["external_udp_port"], external);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut reserved = [0; 8];
    reserved[5] = 0x10;
    peer.write_all(
        &spill::protocol::peer::Handshake {
            reserved,
            info_hash: hash,
            peer_id: [42; 20],
        }
        .encode(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut handshake = [0; 68];
        peer.read_exact(&mut handshake).await.unwrap();
        loop {
            let frame = peer_frame(&mut peer).await;
            if frame[4..].starts_with(&[20, 0]) {
                let hello = spill::protocol::bencode::decode(&frame[6..], Default::default()).unwrap();
                assert!(matches!(hello.get(b"p").unwrap().value(), spill::protocol::bencode::Value::Integer(p) if *p == i64::from(external)));
                break;
            }
        }
    }).await.unwrap();
    let next = tokio::time::timeout(Duration::from_secs(35), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next, (external, udp::EVENT_NONE));
    assert!(started.elapsed() >= Duration::from_secs(29));
    drop(peer);
    daemon.stop().await;
    let requests = requests.lock().unwrap();
    for opcode in [1, 2] {
        assert!(
            requests
                .iter()
                .any(|r| r[1] == opcode && r[8..12] == 7200u32.to_be_bytes())
        );
        assert!(
            requests
                .iter()
                .any(|r| r[1] == opcode && r[8..12] == [0; 4] && r[6..8] == [0; 2])
        );
    }
    for request in requests.iter().filter(|r| r[1] != 0) {
        assert_eq!(request[4..6], address.port().to_be_bytes());
        if request[8..12] != [0; 4] {
            assert_eq!(request[6..8], address.port().to_be_bytes());
        }
    }
    responder.abort();
    tracker_task.abort();
}

#[tokio::test]
async fn upnp_maps_leases_falls_back_to_permanent_and_deletes_on_shutdown() {
    use axum::{
        Router,
        http::{HeaderMap, StatusCode},
        routing::{get, post},
    };
    use std::sync::{Arc, Mutex, atomic::AtomicBool};
    for fault in [None, Some(725), Some(718)] {
        let root = Temp::new();
        let requests = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
        let first_add = Arc::new(AtomicBool::new(true));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let gateway = format!("{origin}/rootDesc.xml");
        let (base, control) = match fault {
            None => (
                format!("<URLBase>{origin}/descriptions/</URLBase>"),
                "../ctl".to_owned(),
            ),
            Some(725) => (String::new(), "/ctl".to_owned()),
            _ => (String::new(), format!("{origin}/ctl")),
        };
        let description = format!(
            "<?xml version=\"1.0\"?><root>{base}<device><serviceList><service><serviceType>urn:schemas-upnp-org:service:WANIPConnection:2</serviceType><controlURL>{control}</controlURL></service></serviceList></device></root>"
        );
        let router = Router::new().route("/rootDesc.xml", get(move || {
            let description = description.clone();
            async move { description }
        })).route("/ctl", post({
            let requests = requests.clone();
            move |headers: HeaderMap, body: String| {
                let requests = requests.clone();
                let first_add = first_add.clone();
                async move {
                    assert!(headers["content-type"].to_str().unwrap().starts_with("text/xml"));
                    let header = headers["soapaction"].to_str().unwrap();
                    let action = header.strip_prefix('"').unwrap().strip_suffix('"').unwrap()
                        .strip_prefix("urn:schemas-upnp-org:service:WANIPConnection:2#").unwrap();
                    assert!(body.contains(&format!("<u:{action} xmlns:u=\"urn:schemas-upnp-org:service:WANIPConnection:2\">")));
                    requests.lock().unwrap().push((action.to_owned(), body));
                    let error = if action == "AddPortMapping" {
                        match fault {
                            Some(725) if first_add.swap(false, Ordering::Relaxed) => Some(725),
                            Some(718) => Some(718),
                            _ => None,
                        }
                    } else { None };
                    let (status, reply) = if let Some(code) = error {
                        (StatusCode::INTERNAL_SERVER_ERROR, format!("<s:Fault><faultcode>s:Client</faultcode><detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{code}</errorCode></UPnPError></detail></s:Fault>"))
                    } else {
                        let fields = match action {
                            "GetExternalIPAddress" => "<NewExternalIPAddress>203.0.113.8</NewExternalIPAddress>",
                            "AddPortMapping" | "DeletePortMapping" => "",
                            _ => panic!("unexpected action {action}"),
                        };
                        (StatusCode::OK, format!("<u:{action}Response xmlns:u=\"urn:schemas-upnp-org:service:WANIPConnection:2\">{fields}</u:{action}Response>"))
                    };
                    (status, [("content-type", "text/xml")], format!("<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body>{reply}</s:Body></s:Envelope>"))
                }
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let source = WebSeed::new().await;
        let mut daemon = mapping_daemon(&root.0, &gateway, true).await;
        let id = daemon.add(&metainfo(Some(&source.url)), None).await;
        daemon.state(&id, "seeding").await;
        assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
        let health = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let health = daemon.call(json!({"op":"health"})).await;
                if health["port_mapping"]["protocol"] == "upnp"
                    || health["port_mapping"]["last_error"].is_string()
                {
                    break health;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let address: std::net::SocketAddr = health["listen"].as_str().unwrap().parse().unwrap();
        let mapping = &health["port_mapping"];
        assert_eq!(health["advertised_port"], address.port());
        if fault == Some(718) {
            assert!(
                mapping["last_error"]
                    .as_str()
                    .unwrap()
                    .contains("UPnP error 718"),
                "{health}"
            );
            assert!(mapping["protocol"].is_null());
        } else {
            assert_eq!(mapping["protocol"], "upnp");
            assert_eq!(mapping["external_ip"], "203.0.113.8");
            assert_eq!(mapping["external_tcp_port"], address.port());
            assert_eq!(mapping["external_udp_port"], address.port());
            assert!(mapping["last_error"].is_null());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            assert!((now + 3590..=now + 3600).contains(&mapping["next_renewal"].as_u64().unwrap()));
        }
        daemon.stop().await;
        let requests = requests.lock().unwrap();
        let adds: Vec<_> = requests
            .iter()
            .filter(|(action, _)| action == "AddPortMapping")
            .collect();
        assert_eq!(
            adds.len(),
            if fault == Some(718) {
                1
            } else if fault == Some(725) {
                3
            } else {
                2
            }
        );
        for (n, (_, body)) in adds.iter().enumerate() {
            for (key, value) in [
                ("NewRemoteHost", "".to_owned()),
                ("NewExternalPort", address.port().to_string()),
                ("NewInternalPort", address.port().to_string()),
                ("NewInternalClient", "127.0.0.1".into()),
                ("NewEnabled", "1".into()),
                ("NewPortMappingDescription", "spill".into()),
                (
                    "NewLeaseDuration",
                    if fault == Some(725) && n != 0 {
                        "0"
                    } else {
                        "7200"
                    }
                    .into(),
                ),
            ] {
                assert!(body.contains(&format!("<{key}>{value}</{key}>")), "{body}");
            }
        }
        let deletes: Vec<_> = requests
            .iter()
            .filter(|(action, _)| action == "DeletePortMapping")
            .collect();
        if fault == Some(718) {
            assert!(deletes.is_empty());
        } else {
            assert_eq!(deletes.len(), 2);
            for protocol in ["TCP", "UDP"] {
                assert!(adds.iter().any(|(_, body)| {
                    body.contains(&format!("<NewProtocol>{protocol}</NewProtocol>"))
                }));
                assert!(deletes.iter().any(|(_, body)| {
                    body.contains(&format!("<NewProtocol>{protocol}</NewProtocol>"))
                        && body.contains(&format!(
                            "<NewExternalPort>{}</NewExternalPort>",
                            address.port()
                        ))
                        && body.contains("<NewRemoteHost></NewRemoteHost>")
                }));
            }
        }
        server.abort();
    }
}

#[tokio::test]
async fn port_mapping_failure_is_nonfatal_and_disabled_sends_nothing() {
    for enabled in [true, false] {
        let root = Temp::new();
        let gateway = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let source = WebSeed::new().await;
        let mut daemon =
            mapping_daemon(&root.0, &gateway.local_addr().unwrap().to_string(), enabled).await;
        let id = daemon.add(&metainfo(Some(&source.url)), None).await;
        daemon.state(&id, "seeding").await;
        assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
        let health = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let health = daemon.call(json!({"op":"health"})).await;
                if !enabled || health["port_mapping"]["last_error"].is_string() {
                    break health;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let address: std::net::SocketAddr = health["listen"].as_str().unwrap().parse().unwrap();
        assert_eq!(health["advertised_port"], address.port());
        assert_eq!(health["port_mapping"]["enabled"], enabled);
        assert!(health["port_mapping"]["protocol"].is_null());
        let mut bytes = [0; 64];
        let received =
            tokio::time::timeout(Duration::from_millis(500), gateway.recv_from(&mut bytes)).await;
        assert_eq!(received.is_ok(), enabled);
        let hash = *spill::protocol::metainfo::Metainfo::parse(
            &metainfo(Some(&source.url)),
            Default::default(),
        )
        .unwrap()
        .info_hash()
        .as_bytes();
        let mut peer = raw_peer(&address.to_string(), hash, 43).await;
        use tokio::io::AsyncWriteExt;
        peer.write_all(&[0, 0, 0, 1, 2]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if peer_frame(&mut peer).await == [0, 0, 0, 1, 1] {
                    break;
                }
            }
            peer.write_all(&[0, 0, 0, 13, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3])
                .await
                .unwrap();
            loop {
                let frame = peer_frame(&mut peer).await;
                if frame.get(4) == Some(&7) {
                    assert_eq!(&frame[13..], b"abc");
                    break;
                }
            }
        })
        .await
        .unwrap();
        drop(peer);
        daemon.stop().await;
    }
}
