use crate::common::{Daemon, WebSeed, metainfo, peer_frame, raw_peer};
use crate::payload::{Temp, seed_metainfo};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn accounting_survives_restart_without_counting_existing_data() {
    let root = Temp::new();
    let source = WebSeed::files(&[("/first", b"abc"), ("/second", b"abc")], true).await;
    let mut bytes = b"d8:url-listl".to_vec();
    for path in ["first", "second"] {
        let url = format!("{}{path}", source.url);
        bytes.extend_from_slice(format!("{}:{url}", url.len()).as_bytes());
    }
    bytes.push(b'e');
    bytes.extend_from_slice(&metainfo(None)[1..]);
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;

    for round in 0..2 {
        let address = daemon.call(json!({"op":"health"})).await["listen"]
            .as_str()
            .unwrap()
            .to_owned();
        let meta = spill::engine::profile::Profile::DEFAULT
            .parse_owned(&bytes)
            .unwrap();
        let mut peer = raw_peer(&address, *meta.info_hash().as_bytes(), 42).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            peer.write_all(&[0, 0, 0, 1, 2]).await.unwrap();
            while peer_frame(&mut peer).await != [0, 0, 0, 1, 1] {}
            let mut request = vec![0, 0, 0, 13, 6];
            request.extend_from_slice(&[0; 8]);
            request.extend_from_slice(&3u32.to_be_bytes());
            peer.write_all(&request).await.unwrap();
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
        // Let the ready, completed owner accumulate an idle seeding interval too.
        tokio::time::sleep(Duration::from_secs(2)).await;
        daemon.call(json!({"op":"pause","id":id})).await;
        let saved = daemon.state(&id, "paused").await;
        assert_eq!(
            saved["downloaded_bytes"], 6,
            "corrupt payload counts as traffic"
        );
        assert_eq!(saved["uploaded_bytes"], 3 * (round + 1));
        assert_eq!(saved["hash_failed_bytes"], 3);
        assert_eq!(saved["hash_failed_pieces"], 1);
        assert_eq!(saved["verified_bytes"], 3);
        assert_eq!(saved["wanted_bytes"], 3);
        assert_eq!(saved["wanted_verified_bytes"], 3);
        assert_eq!(saved["completed"], true);
        assert!(saved["transfer"].is_null());
        assert!(saved["seed_seconds"].as_f64().unwrap() > 0.0);
        let added = saved["added_at"].as_u64().unwrap();
        let completed = saved["completed_at"].as_u64().unwrap();
        let download = saved["last_download_at"].as_u64().unwrap();
        let upload = saved["last_upload_at"].as_u64().unwrap();
        assert!(before <= added && added <= download && download <= upload);
        assert!(added <= completed && completed <= upload);
        daemon.stop().await;
        daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
        let restarted = daemon.state(&id, "paused").await;
        for field in [
            "downloaded_bytes",
            "uploaded_bytes",
            "hash_failed_bytes",
            "hash_failed_pieces",
            "seed_seconds",
            "added_at",
            "completed_at",
            "last_download_at",
            "last_upload_at",
            "completed",
            "verified_bytes",
            "wanted_bytes",
            "wanted_verified_bytes",
        ] {
            assert_eq!(restarted[field], saved[field], "{field}");
        }
        if round == 0 {
            daemon.call(json!({"op":"resume","id":id})).await;
            let running = daemon.state(&id, "seeding").await;
            assert_eq!(running["transfer"]["downloaded_bytes"], 0);
            assert_eq!(running["transfer"]["uploaded_bytes"], 0);
            assert_eq!(running["transfer"]["hash_failed_bytes"], 0);
            assert_eq!(running["transfer"]["hash_failed_pieces"], 0);
        }
    }
    daemon
        .call(json!({"op":"set","id":id,"values":{"file_priorities":["skip"]}}))
        .await;
    let skipped = daemon.state(&id, "paused").await;
    assert_eq!(skipped["wanted_bytes"], 0);
    assert_eq!(skipped["wanted_verified_bytes"], 0);
    assert_eq!(skipped["verified_bytes"], 3);
    assert_eq!(skipped["completed"], false);
    assert!(skipped["completed_at"].is_u64());
    daemon
        .call(json!({"op":"set","id":id,"values":{"file_priorities":["normal"]}}))
        .await;
    let selected = daemon.state(&id, "paused").await;
    assert_eq!(selected["wanted_bytes"], 3);
    assert_eq!(selected["wanted_verified_bytes"], 3);
    assert_eq!(selected["completed"], false);
    daemon.call(json!({"op":"recheck","id":id})).await;
    let checked = daemon.state(&id, "paused").await;
    assert_eq!(checked["downloaded_bytes"], 6);
    assert_eq!(checked["uploaded_bytes"], 6);
    assert_eq!(checked["completed"], true);

    std::fs::write(root.0.join("downloads/x"), b"bad").unwrap();
    daemon.call(json!({"op":"recheck","id":id})).await;
    let damaged = daemon.state(&id, "paused").await;
    assert_eq!(damaged["completed"], false);
    assert_eq!(damaged["verified_bytes"], 0);
    assert_eq!(damaged["downloaded_bytes"], 6);
    assert_eq!(
        damaged["hash_failed_bytes"], 3,
        "disk corruption is not swarm traffic"
    );
    assert_eq!(damaged["hash_failed_pieces"], 1);
    daemon.call(json!({"op":"resume","id":id})).await;
    daemon.state(&id, "seeding").await;
    daemon.call(json!({"op":"pause","id":id})).await;
    let repaired = daemon.state(&id, "paused").await;
    assert_eq!(repaired["downloaded_bytes"], 9);
    assert_eq!(repaired["hash_failed_bytes"], 3);
    assert_eq!(repaired["hash_failed_pieces"], 1);

    std::fs::write(root.0.join("downloads/adopted"), b"abc").unwrap();
    let adopted = daemon
        .add(&seed_metainfo("adopted", b"abc", None), None)
        .await;
    daemon.state(&adopted, "seeding").await;
    daemon.call(json!({"op":"pause","id":adopted})).await;
    let row = daemon.state(&adopted, "paused").await;
    assert_eq!(row["completed"], true);
    assert!(row["completed_at"].is_u64());
    assert_eq!(row["verified_bytes"], 3);
    assert_eq!(row["downloaded_bytes"], 0);
    assert_eq!(row["uploaded_bytes"], 0);
    assert_eq!(row["hash_failed_bytes"], 0);
    assert_eq!(row["hash_failed_pieces"], 0);
    assert!(row["last_download_at"].is_null());
    assert!(row["last_upload_at"].is_null());
    daemon.stop().await;
}
