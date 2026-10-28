mod admission;
mod fallback;
mod flow_control;
mod shutdown;
mod transfer;
use crate::common::Daemon;
use serde_json::json;
use std::time::Duration;

pub(super) async fn utp_tracker(
    address: std::net::SocketAddr,
) -> (String, tokio::task::JoinHandle<()>) {
    utp_tracker_gated(address, None).await
}

async fn utp_tracker_gated(
    address: std::net::SocketAddr,
    mut gate: Option<std::sync::Arc<tokio::sync::Notify>>,
) -> (String, tokio::task::JoinHandle<()>) {
    use spill::protocol::{compact::PeersV4, tracker::udp};
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", socket.local_addr().unwrap());
    let address = match address {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let port = address.port().to_be_bytes();
    let mut peers = address.ip().octets().to_vec();
    peers.extend_from_slice(&port);
    let task = tokio::spawn(async move {
        let limits = udp::Limits {
            max_packet_len: 65507,
            max_peers: 200,
        };
        let mut bytes = [0; 4096];
        loop {
            let (n, source) = socket.recv_from(&mut bytes).await.unwrap();
            let request = udp::Request::decode(&bytes[..n], limits).unwrap();
            let kind = match request.kind {
                udp::RequestKind::Connect { .. } => udp::ResponseKind::Connect {
                    connection_id: 17,
                    extensions: &[],
                },
                udp::RequestKind::Announce { .. } => {
                    if let Some(gate) = gate.take() {
                        gate.notified().await;
                    }
                    udp::ResponseKind::Announce {
                        interval: 30,
                        seeders: 1,
                        leechers: 1,
                        peers: udp::Peers::V4(PeersV4::parse(&peers, 1).unwrap()),
                    }
                }
                _ => panic!("unexpected tracker request"),
            };
            let reply = udp::Response {
                transaction_id: request.transaction_id,
                kind,
            }
            .encode(limits)
            .unwrap();
            socket.send_to(&reply, source).await.unwrap();
        }
    });
    (url, task)
}

async fn transport_daemon(root: &std::path::Path, utp: bool) -> Daemon {
    transport_daemon_at(root, utp, "127.0.0.1:0").await
}

async fn transport_daemon_at(root: &std::path::Path, utp: bool, listen: &str) -> Daemon {
    std::fs::write(root.join("config.toml"), format!(
        "state_dir={:?}\ndownload_dir={:?}\ncontrol_socket={:?}\nlisten=\"{listen}\"\nallow_local_network=true\nport_mapping=false\nutp={utp}\npeers=4\nwrite_queue_bytes=16384\ndht_bootstrap=[]\n[defaults]\ndht=false\npex=false\nweb_seeds=false\ntrackers=true\nupload_limit=262144\n",
        root.join("state"), root.join("downloads"), root.join("control.sock"),
    )).unwrap();
    Daemon::start_config(root).await
}

pub(super) async fn wait_transport(daemon: &Daemon, id: &str, transport: &str) {
    let mut last = json!(null);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            if row["transfer"]["peers"].as_array().is_some_and(|peers| {
                peers
                    .iter()
                    .any(|p| p["transport"] == transport && p["detail"]["established"] == true)
            }) {
                break;
            }
            last = row;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("established {transport} peer transport in diagnostics: {last}"));
}
