mod admission;
mod fallback;
mod flow_control;
mod shutdown;
mod transfer;
use crate::common::{Daemon, DaemonConfig};
use serde_json::json;
use std::time::Duration;

pub(super) async fn utp_tracker(
    address: std::net::SocketAddr,
) -> (String, tokio::task::JoinHandle<()>) {
    utp_tracker_gated(address, None).await
}

async fn utp_tracker_gated(
    address: std::net::SocketAddr,
    gate: Option<std::sync::Arc<tokio::sync::Notify>>,
) -> (String, tokio::task::JoinHandle<()>) {
    use axum::{Router, routing::get};
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", socket.local_addr().unwrap());
    let address = match address {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let port = address.port().to_be_bytes();
    let mut peers = address.ip().octets().to_vec();
    peers.extend_from_slice(&port);
    let mut body = b"d8:intervali300e12:min intervali0e5:peers6:".to_vec();
    body.extend_from_slice(&peers);
    body.push(b'e');
    let gate = std::sync::Arc::new(std::sync::Mutex::new(gate));
    let app = Router::new().route(
        "/announce",
        get(move || {
            let gate = gate.clone();
            let body = body.clone();
            async move {
                let first = gate.lock().unwrap().take();
                if let Some(gate) = first {
                    gate.notified().await;
                }
                body
            }
        }),
    );
    let task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    (url, task)
}

async fn transport_daemon(root: &std::path::Path, utp: bool) -> Daemon {
    transport_daemon_at(root, utp, "127.0.0.1:0", 262144).await
}

async fn transport_daemon_at(
    root: &std::path::Path,
    utp: bool,
    listen: &str,
    upload_limit: u64,
) -> Daemon {
    Daemon::start(
        root,
        DaemonConfig {
            listen: listen.parse().unwrap(),
            utp: Some(utp),
            cache_bytes: None,
            write_queue_bytes: Some(16384),
            active_downloads: 4,
            active_seeds: 16,
            web_seeds: false,
            trackers: true,
            upload_limit,
            ..Default::default()
        },
    )
    .await
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
