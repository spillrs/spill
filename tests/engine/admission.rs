use crate::payload::{Temp, seed_metainfo};
use spill::engine::{
    profile::{BLOCK_LEN, Profile},
    session::{Limits, NetworkOptions, Session},
    torrent::{Destination, RunPolicy, TorrentOptions, TorrentSource},
};
use spill::protocol::magnet::Magnet;
use std::{io, time::Duration};

#[tokio::test]
async fn invalid_profiles_do_not_panic_or_consume_admission() {
    let profile = Profile {
        peers: 1,
        cache_bytes: BLOCK_LEN as usize,
        write_queue_bytes: BLOCK_LEN as usize,
    };
    let meta = profile
        .parse_owned(&seed_metainfo("x", b"abc", None))
        .unwrap();
    let session = Session::offline(Limits {
        torrents: 1,
        peers: 1,
        payload_bytes: profile.cache_bytes + profile.write_queue_bytes,
        ..Default::default()
    });
    let online = Session::bind(
        NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap();
    let root = Temp::new();
    let magnet = Magnet::parse(
        &format!("magnet:?xt=urn:btih:{}", meta.info_hash()),
        Default::default(),
    )
    .unwrap();
    for invalid in [
        Profile {
            peers: 0,
            ..profile
        },
        Profile {
            peers: 4097,
            ..profile
        },
        Profile {
            cache_bytes: BLOCK_LEN as usize - 1,
            ..profile
        },
        Profile {
            cache_bytes: 1024 * 1024 * 1024 + 1,
            ..profile
        },
        Profile {
            write_queue_bytes: BLOCK_LEN as usize - 1,
            ..profile
        },
        Profile {
            write_queue_bytes: 1024 * 1024 * 1024 + 1,
            ..profile
        },
    ] {
        let mut options = TorrentOptions::new(
            TorrentSource::Metainfo(meta.clone()),
            Destination::Open(root.0.clone()),
        );
        options.profile = invalid;
        options.run = RunPolicy::Recheck;
        let error = session.admit(options).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "invalid torrent resource limits");
        assert!(session.torrent(meta.info_hash()).is_none());
        let error = online
            .inspect(magnet.clone(), invalid, Default::default())
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "invalid torrent resource limits");
    }
    online.shutdown().await.unwrap();

    std::fs::write(root.0.join("x"), b"abc").unwrap();
    let mut options = TorrentOptions::new(
        TorrentSource::Metainfo(meta),
        Destination::Open(root.0.clone()),
    );
    options.profile = profile;
    options.run = RunPolicy::Recheck;
    let observer = session.admit(options).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(5), observer.outcome())
        .await
        .expect("recheck completion");
    assert!(outcome.recheck_finished && outcome.wanted_complete && outcome.full_complete);
    assert!(outcome.cleanup_complete && outcome.error.is_none());
    assert!(session.torrent(observer.info_hash()).is_none());
    assert!(std::sync::Arc::ptr_eq(&outcome, &observer.outcome().await));
    assert!(observer.snapshot().progress.completed);
    assert_eq!(std::fs::read(root.0.join("x")).unwrap(), b"abc");
    session.shutdown().await.unwrap();
}
