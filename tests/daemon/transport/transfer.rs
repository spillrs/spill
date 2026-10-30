use super::{transport_daemon_at, utp_tracker, wait_transport};
use crate::payload::{Temp, seed_metainfo};
use serde_json::json;
use sha1::Digest;
use std::time::Duration;

async fn utp_transfer(chaos: bool, dual_stack: bool, strip_sack: bool) {
    let seed_root = Temp::new();
    let leech_root = Temp::new();
    let size = if chaos {
        4 * 1024 * 1024
    } else {
        2 * 1024 * 1024
    };
    let payload: Vec<u8> = (0..size).map(|i| (i * 31 % 251) as u8).collect();
    std::fs::create_dir_all(seed_root.0.join("downloads")).unwrap();
    std::fs::write(seed_root.0.join("downloads/x"), &payload).unwrap();
    let listen = if dual_stack { "[::]:0" } else { "127.0.0.1:0" };
    let mut seed = transport_daemon_at(&seed_root.0, true, listen).await;
    let mut seed_address: std::net::SocketAddr = seed.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    seed_address.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    let relay = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let contact = relay.local_addr().unwrap();
    let tiny_piece_headers = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_headers = tiny_piece_headers.clone();
    let relay_task = tokio::spawn(async move {
        let mut bytes = [0; 65536];
        let mut leech = None;
        let mut count = 0;
        let mut data = [0usize; 2];
        let mut held = None;
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tokio::select! {
                received = relay.recv_from(&mut bytes) => {
                    let (n, source) = received.unwrap();
                    let destination = if source == seed_address {
                        let Some(destination) = leech else { continue; }; destination
                    } else { leech = Some(source); seed_address };
                    let decoded = spill::protocol::utp::Packet::decode(&bytes[..n]).unwrap();
                    if source == seed_address && decoded.payload.len() == 13 && decoded.payload[4] == 7 {
                        observed_headers.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    if !chaos {
                        relay.send_to(&bytes[..n], destination).await.unwrap();
                        continue;
                    }
                    count += 1;
                    if n >= 20 && bytes[0] == 1 {
                        let direction = usize::from(source == seed_address);
                        data[direction] += 1;
                        if (100..110).contains(&(data[direction] % 2048)) { continue; }
                    }
                    if count % 20 == 0 { continue; }
                    let mut packet = Vec::new();
                    let mut decoded = spill::protocol::utp::Packet::decode(&bytes[..n]).unwrap();
                    if strip_sack { decoded.sack = None; }
                    decoded.encode(&mut packet);
                    if held.is_none() && count % 7 == 0 { held = Some((packet, destination)); continue; }
                    relay.send_to(&packet, destination).await.unwrap();
                    if count % 13 == 0 { relay.send_to(&packet, destination).await.unwrap(); }
                    if let Some((packet, target)) = held.take() { relay.send_to(&packet, target).await.unwrap(); }
                }
                _ = tick.tick() => {
                    if let Some((packet, target)) = held.take() { relay.send_to(&packet, target).await.unwrap(); }
                }
            }
        }
    });
    let (url, tracker) = utp_tracker(contact).await;
    let meta = seed_metainfo("x", &payload, Some(&url));
    let id = seed.add(&seed_metainfo("x", &payload, None), None).await;
    seed.state(&id, "seeding").await;
    let mut leech = transport_daemon_at(&leech_root.0, true, listen).await;
    leech.add(&meta, None).await;
    wait_transport(&leech, &id, "utp").await;
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let row = leech.call(json!({"op":"get","id":id})).await;
            assert!(row["last_error"].is_null(), "{row}");
            if row["state"] == "seeding" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("uTP download completion");
    let downloaded = std::fs::read(leech_root.0.join("downloads/x")).unwrap();
    assert_eq!(
        sha1::Sha1::digest(&downloaded),
        sha1::Sha1::digest(&payload)
    );
    assert_eq!(
        tiny_piece_headers.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "vectored piece headers must be coalesced with their payload"
    );
    if chaos {
        let row = seed.call(json!({"op":"get","id":id})).await;
        assert!(
            row["transfer"]["utp_retransmits"].as_u64().unwrap() > 0,
            "{row}"
        );
    }
    leech.stop().await;
    seed.stop().await;
    tracker.abort();
    relay_task.abort();
}

#[tokio::test]
async fn utp_transfer_between_two_daemons() {
    utp_transfer(false, false, false).await;
}

#[tokio::test]
async fn utp_survives_loss_and_reordering() {
    utp_transfer(true, false, false).await;
}

#[tokio::test]
async fn utp_recovers_burst_loss_without_selective_acks() {
    utp_transfer(true, false, true).await;
}

#[tokio::test]
async fn utp_ipv4_transfer_on_dual_stack_listeners() {
    utp_transfer(false, true, false).await;
}
