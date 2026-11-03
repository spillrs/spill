use crate::{
    common::{Daemon, DaemonConfig},
    payload::{Temp, seed_metainfo},
};
use serde_json::json;
use spill::protocol::{
    bencode::{Node, Value},
    dht::{
        NodeId,
        krpc::{Body, Kind, Message},
    },
};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::UdpSocket, sync::mpsc};

fn bytes(value: &[u8]) -> Node<'_> {
    Node::new(Value::Bytes(value))
}

#[tokio::test]
async fn daemon_saves_and_reverifies_ipv6_contacts_across_restart() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let root = Temp::new();
        std::fs::create_dir_all(root.0.join("downloads")).unwrap();
        std::fs::write(root.0.join("downloads/x"), b"abc").unwrap();
        let config = DaemonConfig {
            listen: "[::1]:0".parse().unwrap(),
            dht: true,
            web_seeds: false,
            peers: None,
            cache_bytes: None,
            write_queue_bytes: None,
            active_downloads: 4,
            active_seeds: 16,
            ..Default::default()
        };
        let socket = Arc::new(UdpSocket::bind("[::1]:0").await.unwrap());
        let address = socket.local_addr().unwrap();
        let id = NodeId([9; 20]);
        let (sender, mut received) = mpsc::channel(16);
        let remote = socket.clone();
        let task = tokio::spawn(async move {
            let mut buffer = [0; 4096];
            loop {
                let (length, source) = remote.recv_from(&mut buffer).await.unwrap();
                let message =
                    Message::parse(&buffer[..length], Default::default(), 16, 100).unwrap();
                match message.kind() {
                    Kind::Query { .. } => {
                        let fields = BTreeMap::from([
                            (b"id".as_slice(), bytes(&id.0)),
                            (b"nodes6", bytes(b"")),
                            (b"token", bytes(b"token")),
                        ]);
                        let wire = Message::new(
                            message.transaction_id(),
                            Body::Response(Node::new(Value::Dictionary(fields))),
                            None,
                            16,
                            100,
                        )
                        .unwrap()
                        .encode(Default::default())
                        .unwrap();
                        remote.send_to(&wire, source).await.unwrap();
                    }
                    Kind::Response { .. } => sender.send(buffer[..length].to_vec()).await.unwrap(),
                    _ => panic!("unexpected daemon DHT packet"),
                }
            }
        });
        let mut daemon = Daemon::start(&root.0, config).await;
        let torrent = daemon.add(&seed_metainfo("x", b"abc", None), None).await;
        let mut saved_identity = None;
        for restart in [false, true] {
            daemon.state(&torrent, "seeding").await;
            let destination: SocketAddr = daemon.call(json!({"op":"health"})).await["listen"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            // On first admission, a normal query makes the daemon probe us. On
            // restart, only restored contacts can populate its routing table.
            let method: &[u8] = if restart { b"find_node" } else { b"ping" };
            let mut fields = BTreeMap::from([(b"id".as_slice(), bytes(&id.0))]);
            if restart {
                fields.insert(b"target", bytes(&id.0));
            }
            let arguments = Node::new(Value::Dictionary(fields));
            let body = if restart {
                Body::ReadOnlyQuery { method, arguments }
            } else {
                Body::Query { method, arguments }
            };
            let wire = Message::new(b"hello", body, None, 16, 100)
                .unwrap()
                .encode(Default::default())
                .unwrap();
            socket.send_to(&wire, destination).await.unwrap();
            received.recv().await.unwrap();
            tokio::time::timeout(Duration::from_secs(4), async {
                loop {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    let arguments = Node::new(Value::Dictionary(BTreeMap::from([
                        (b"id".as_slice(), bytes(&id.0)),
                        (b"target", bytes(&id.0)),
                    ])));
                    let wire = Message::new(
                        b"check",
                        Body::ReadOnlyQuery {
                            method: b"find_node",
                            arguments,
                        },
                        None,
                        16,
                        100,
                    )
                    .unwrap()
                    .encode(Default::default())
                    .unwrap();
                    socket.send_to(&wire, destination).await.unwrap();
                    let wire = received.recv().await.unwrap();
                    let message = Message::parse(&wire, Default::default(), 16, 100).unwrap();
                    if let Kind::Response {
                        nodes6: Some(nodes),
                        ..
                    } = message.kind()
                        && nodes
                            .iter()
                            .any(|n| n.id == id && SocketAddr::V6(n.address) == address)
                    {
                        break;
                    }
                }
            })
            .await
            .expect("daemon verified IPv6 contact");
            daemon.stop().await;
            let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
            let (node_id, nodes): (Vec<u8>, Vec<u8>) = db
                .query_row("SELECT node_id,nodes FROM dht", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .unwrap();
            let nodes: Vec<([u8; 20], SocketAddr)> = serde_json::from_slice(&nodes).unwrap();
            assert_eq!(nodes, vec![(id.0, address)]);
            if let Some(expected) = &saved_identity {
                assert_eq!(&node_id, expected);
            } else {
                saved_identity = Some(node_id);
            }
            drop(db);
            if !restart {
                daemon = Daemon::start_config(&root.0).await;
            }
        }
        task.abort();
    })
    .await
    .expect("daemon IPv6 DHT persistence");
}
