use crate::{common::Daemon, metadata_peer, payload::Temp};
use serde_json::json;
use sha1::{Digest, Sha1};
use spill::{
    engine::profile::Profile,
    protocol::bencode::{self, Node, Value},
};
use std::time::Duration;
use tokio::net::TcpListener;

fn small_hybrid() -> Vec<u8> {
    let doc = bencode::decode(
        include_bytes!("../protocol/fixtures/v2-single.torrent"),
        Default::default(),
    )
    .unwrap();
    let Value::Dictionary(outer) = doc.value() else {
        unreachable!()
    };
    let Value::Dictionary(info) = doc.get(b"info").unwrap().value() else {
        unreachable!()
    };
    let mut info = info.clone();
    let sha1 = Sha1::digest([0]);
    info.insert(b"length", Node::new(Value::Integer(1)));
    info.insert(b"pieces", Node::new(Value::Bytes(&sha1)));
    let mut outer = outer.clone();
    outer.insert(b"info", Node::new(Value::Dictionary(info)));
    bencode::encode(&Node::new(Value::Dictionary(outer)), Default::default()).unwrap()
}

async fn seeding(daemon: &Daemon, id: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let row = daemon.call(json!({"op":"get", "id":id})).await;
            assert!(row["last_error"].is_null(), "{row}");
            if row["state"] == "seeding" && row["layers_pending"] == false {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("magnet resolution and TCP fallback")
}

#[tokio::test]
async fn magnets_keep_primary_ids_learn_aliases_and_recover_authenticated_layers() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let small = small_hybrid();
        for (fixture, v2_topic) in [
            (small.as_slice(), false),
            (
                include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent").as_slice(),
                false,
            ),
            (
                include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent").as_slice(),
                true,
            ),
            (
                include_bytes!("../protocol/fixtures/v2.torrent").as_slice(),
                true,
            ),
        ] {
            let meta = Profile::default().parse_owned(fixture).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (url, tracker) = metadata_peer::tracker(&[address]).await;
            let has_layers = meta
                .layout()
                .files()
                .iter()
                .any(|file| file.length() > u64::from(meta.layout().piece_length()));
            let needs_layers = meta.info_hash().v1().is_none() && has_layers;
            let mut source = tokio::spawn(metadata_peer::serve_full(
                listener,
                meta.clone(),
                if needs_layers {
                    4
                } else if has_layers && meta.info_hash().v1().is_some() && !v2_topic {
                    7
                } else {
                    0
                },
            ));
            let root = Temp::new();
            let mut daemon = Daemon::start(&root.0, "trackers=true\nweb_seeds=false", None).await;
            let topic = if v2_topic {
                format!("btmh:1220{}", meta.info_hash().v2().unwrap())
            } else {
                format!("btih:{}", meta.info_hash().v1().unwrap())
            };
            let magnet = format!("magnet:?xt=urn:{topic}&tr={url}");
            let added = daemon.call(json!({"op":"add", "magnet":magnet})).await;
            let id = added["id"].as_str().unwrap();
            let expected = if v2_topic {
                meta.info_hash().v2().unwrap().to_string()
            } else {
                meta.info_hash().v1().unwrap().to_string()
            };
            assert_eq!(id, expected);
            if needs_layers {
                let waiting = loop {
                    let row = daemon.call(json!({"op":"get", "id":id})).await;
                    assert!(row["last_error"].is_null(), "{row}");
                    if row["info_validated"] == true
                        && row["v2"] == meta.info_hash().v2().unwrap().to_string()
                        && meta
                            .info_hash()
                            .v1()
                            .is_none_or(|hash| row["v1"] == hash.to_string())
                    {
                        break row;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                };
                assert_eq!(waiting["metadata_ready"], false);
                assert_eq!(waiting["layers_pending"], false);
                assert_eq!(waiting["hash_bytes"], 0);
                assert_eq!(
                    waiting["format"],
                    if meta.info_hash().v1().is_some() {
                        "hybrid"
                    } else {
                        "v2"
                    }
                );
                for hash in [
                    meta.info_hash().v1().map(|hash| hash.to_string()),
                    meta.info_hash().v2().map(|hash| hash.to_string()),
                ]
                .into_iter()
                .flatten()
                {
                    assert_eq!(daemon.call(json!({"op":"get", "id":hash})).await["id"], id);
                    assert_eq!(
                        daemon.call(json!({"op":"get", "id":&hash[..16]})).await["id"],
                        id
                    );
                }
                daemon.call(json!({"op":"pause", "id":id})).await;
                daemon.state(id, "paused").await;
                source.abort();
                assert!(source.await.unwrap_err().is_cancelled());
                let listener = TcpListener::bind(address).await.unwrap();
                source = tokio::spawn(metadata_peer::serve_full(listener, meta.clone(), 0));
                daemon.call(json!({"op":"resume", "id":id})).await;
            }
            let seeded = seeding(&daemon, id).await;
            assert_eq!(seeded["layers_pending"], false);
            let db = rusqlite::Connection::open(root.0.join("state/catalog.sqlite")).unwrap();
            let stored: Vec<u8> = db
                .query_row("SELECT metainfo FROM torrents WHERE id=?1", [id], |row| {
                    row.get(0)
                })
                .unwrap();
            let stored = Profile::default().parse_owned(&stored).unwrap();
            assert!(!stored.layers_pending());
            assert_eq!(stored.info_bytes(), meta.info_bytes());
            for piece in 0..meta.layout().piece_count() {
                assert_eq!(stored.verification(piece), meta.verification(piece));
            }
            drop(db);
            assert_eq!(seeded["metadata_ready"], true);
            assert_eq!(seeded["info_validated"], true);
            assert_eq!(seeded["v2"], meta.info_hash().v2().unwrap().to_string());
            assert_eq!(seeded["hash_bytes"], seeded["hash_total_bytes"]);
            for hash in [
                meta.info_hash().v1().map(|hash| hash.to_string()),
                Some(meta.info_hash().v2().unwrap().to_string()),
            ]
            .into_iter()
            .flatten()
            {
                assert_eq!(daemon.call(json!({"op":"get", "id":hash})).await["id"], id);
                let topic = if hash.len() == 40 {
                    format!("btih:{hash}")
                } else {
                    format!("btmh:1220{hash}")
                };
                assert_ne!(
                    daemon
                        .raw(json!({"op":"add", "magnet":format!("magnet:?xt=urn:{topic}")}))
                        .await["code"],
                    0
                );
            }
            daemon.call(json!({"op":"pause", "id":id})).await;
            daemon.state(id, "paused").await;
            daemon.stop().await;
            source.abort();
            tracker.abort();
            let mut daemon = Daemon::start_config(&root.0).await;
            let saved = daemon
                .call(json!({"op":"get", "id":meta.info_hash().v2().unwrap().to_string()}))
                .await;
            assert_eq!(saved["id"], id);
            assert_eq!(saved["metadata_ready"], true);
            daemon
                .call(json!({"op":"set", "id":id, "values":{"trackers":false}}))
                .await;
            daemon.call(json!({"op":"resume", "id":id})).await;
            assert_eq!(seeding(&daemon, id).await["completed"], true);
            daemon.stop().await;
        }
    })
    .await
    .expect("daemon magnet aliases and acquired-layer restart");
}

#[tokio::test]
async fn private_v2_magnets_fail_before_layers_or_payload_and_retain_failure() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let doc = bencode::decode(
            include_bytes!("../protocol/fixtures/v2.torrent"),
            Default::default(),
        )
        .unwrap();
        let Value::Dictionary(outer) = doc.value() else {
            unreachable!()
        };
        let Value::Dictionary(info) = doc.get(b"info").unwrap().value() else {
            unreachable!()
        };
        let mut info = info.clone();
        info.insert(b"private", Node::new(Value::Integer(1)));
        let mut outer = outer.clone();
        outer.insert(b"info", Node::new(Value::Dictionary(info)));
        let meta = Profile::default()
            .parse_owned(
                &bencode::encode(&Node::new(Value::Dictionary(outer)), Default::default()).unwrap(),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (url, tracker) = metadata_peer::tracker(&[listener.local_addr().unwrap()]).await;
        let source = tokio::spawn(metadata_peer::serve_full(listener, meta.clone(), 4));
        let root = Temp::new();
        let mut daemon = Daemon::start(&root.0, "trackers=true\nweb_seeds=false", None).await;
        let id = meta.info_hash().to_string();
        daemon
            .call(json!({"op":"add", "magnet":format!("magnet:?xt=urn:btmh:1220{id}&tr={url}")}))
            .await;
        let failed = daemon.failed(&id).await;
        assert_eq!(failed["state"], "paused");
        assert_eq!(failed["intent"], "paused");
        assert_eq!(failed["retry_at"], 0);
        assert_eq!(failed["metadata_ready"], false);
        assert_eq!(failed["hash_bytes"], 0);
        assert_eq!(
            failed["last_error"],
            "private torrents require a .torrent file"
        );
        assert_eq!(
            std::fs::read_dir(root.0.join("downloads")).unwrap().count(),
            0
        );
        daemon.stop().await;
        let mut daemon = Daemon::start_config(&root.0).await;
        let saved = daemon.call(json!({"op":"get", "id":id})).await;
        assert_eq!(saved["last_error"], failed["last_error"]);
        assert_eq!(saved["state"], "paused");
        assert_eq!(saved["retry_at"], 0);
        daemon.stop().await;
        source.abort();
        tracker.abort();
    })
    .await
    .expect("private magnet rejection survives restart");
}

#[tokio::test]
async fn learned_catalog_alias_collision_fails_without_publishing_metadata() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let meta = Profile::default().parse_owned(include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (url, tracker) = metadata_peer::tracker(&[listener.local_addr().unwrap()]).await;
        let source = tokio::spawn(metadata_peer::serve_full(listener, meta.clone(), 0));
        let root = Temp::new();
        let mut daemon = Daemon::start(&root.0, "trackers=true\nweb_seeds=false", None).await;
        let v2 = meta.info_hash().v2().unwrap().to_string();
        let v1 = meta.info_hash().v1().unwrap().to_string();
        daemon.call(json!({"op":"add", "magnet":format!("magnet:?xt=urn:btmh:1220{v2}"), "paused":true})).await;
        daemon.call(json!({"op":"add", "magnet":format!("magnet:?xt=urn:btih:{v1}&tr={url}")})).await;
        let failed = daemon.failed(&v1).await;
        assert_eq!(failed["state"], "paused");
        assert_eq!(failed["intent"], "paused");
        assert_eq!(failed["retry_at"], 0);
        assert_eq!(failed["metadata_ready"], false);
        assert!(failed["v2"].is_null());
        assert_eq!(failed["last_error"], "torrent already exists");
        assert_eq!(std::fs::read_dir(root.0.join("downloads")).unwrap().count(), 0);
        daemon.stop().await;
        let mut daemon = Daemon::start_config(&root.0).await;
        let saved = daemon.call(json!({"op":"get", "id":v1})).await;
        assert_eq!(saved["metadata_ready"], false);
        assert_eq!(saved["last_error"], failed["last_error"]);
        assert_eq!(saved["state"], "paused");
        assert_eq!(saved["retry_at"], 0);
        assert_eq!(daemon.call(json!({"op":"get", "id":v2})).await["state"], "paused");
        daemon.stop().await;
        source.abort(); tracker.abort();
    }).await.expect("learned duplicate persists without payload preparation");
}
