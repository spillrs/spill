use crate::payload::Temp;
use sha1::{Digest, Sha1};
use spill::{
    engine::{
        create::{self, Format, Options},
        profile::Profile,
        session::{Limits, NetworkOptions, Session},
        settings::TorrentSettings,
        torrent::{Destination, RunPolicy, TorrentOptions, TorrentSource},
    },
    protocol::{bencode::Value, identity::TorrentIdentity},
};
use std::{
    ffi::OsString,
    io,
    os::unix::{ffi::OsStringExt, fs::symlink},
    time::Duration,
};

#[test]
fn created_v2_and_hybrid_match_reference_vectors() {
    let vectors = [
        ("vectors/block", 16384),
        ("vectors/empty", 0),
        ("vectors/odd", 65659),
        ("vectors/one", 1),
        ("vectors/piece", 32768),
    ];
    for (format, fixture, name, files) in [
        (
            Format::V2,
            include_bytes!("../protocol/fixtures/v2.torrent").as_slice(),
            "vectors",
            vectors.as_slice(),
        ),
        (
            Format::Hybrid,
            include_bytes!("../protocol/fixtures/hybrid.torrent").as_slice(),
            "vectors",
            vectors.as_slice(),
        ),
        (
            Format::V2,
            include_bytes!("../protocol/fixtures/v2-single.torrent").as_slice(),
            "single",
            [("single", 1)].as_slice(),
        ),
        (
            Format::V2,
            include_bytes!("../protocol/fixtures/v2-copies.torrent").as_slice(),
            "copies",
            [
                ("copies/a", 65659),
                ("copies/b", 65659),
                ("copies/c", 65659),
            ]
            .as_slice(),
        ),
    ] {
        let root = Temp::new();
        let source = root.0.join(name);
        for &(path, length) in files {
            let path = root.0.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let bytes: Vec<_> = (0..length).map(|i| (i % 251) as u8).collect();
            std::fs::write(path, bytes).unwrap();
        }
        let options = Options {
            format,
            piece_length: 1 << 15,
            creation_date: None,
            announce: vec![vec!["http://example.invalid/announce".into()]],
            ..Default::default()
        };
        let created = create::create(&source, &options, &mut |_, _| Ok(())).unwrap();
        let actual = Profile::default().parse_owned(&created.metainfo).unwrap();
        let expected = Profile::default().parse_owned(fixture).unwrap();
        assert_eq!(created.info_hash, expected.info_hash());
        assert_eq!(actual.info_bytes(), expected.info_bytes());
        assert_eq!(
            actual.layout().piece_count(),
            expected.layout().piece_count()
        );
        for index in 0..actual.layout().piece_count() {
            assert_eq!(actual.verification(index), expected.verification(index));
        }
        if name == "copies" {
            let document = actual.decode_document();
            let Value::Dictionary(layers) = document.get(b"piece layers").unwrap().value() else {
                panic!("piece layers dictionary")
            };
            assert_eq!(layers.len(), 1);
        }
    }
}

#[tokio::test]
async fn created_directory_is_reproducible_and_transfers_across_file_boundaries() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let source = Temp::new();
        let bundle = source.0.join("bundle");
        std::fs::create_dir_all(bundle.join("nested/empty-dir")).unwrap();
        let a = (0..32767).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let b = (0..70001).map(|i| (i % 239) as u8).collect::<Vec<_>>();
        // Deliberately create files out of order, including empty entries on a
        // piece boundary and inside a cross-file piece.
        std::fs::write(bundle.join("nested/b"), &b).unwrap();
        std::fs::write(bundle.join("z"), b"tail").unwrap();
        std::fs::write(bundle.join("a"), &a).unwrap();
        std::fs::write(bundle.join("a-empty"), []).unwrap();
        std::fs::write(bundle.join(".hidden"), b"h").unwrap();
        std::fs::write(bundle.join("nested/0-empty"), []).unwrap();
        std::fs::write(bundle.join("nested-first"), b"prefix").unwrap();
        for format in [Format::V1, Format::V2, Format::Hybrid] {
            let mut options = Options {
                format,
                piece_length: 1 << 15,
                threads: 1,
                creation_date: None,
                ..Default::default()
            };
            let first = create::create(&bundle, &options, &mut |_, _| Ok(())).unwrap();
            options.threads = 4;
            let mut progress = Vec::new();
            let parallel = create::create(&bundle, &options, &mut |done, total| {
                progress.push((done, total));
                Ok(())
            })
            .unwrap();
            assert_eq!(parallel.metainfo, first.metainfo);
            if format == Format::V1 {
                assert_eq!(progress, (0..=4).map(|n| (n, 4)).collect::<Vec<_>>());
            } else {
                assert_eq!(
                    progress,
                    (0..=parallel.piece_count)
                        .map(|n| (n, parallel.piece_count))
                        .collect::<Vec<_>>()
                );
            }
            let meta = Profile::default().parse_owned(&parallel.metainfo).unwrap();
            assert_eq!(parallel.info_hash, meta.info_hash());
            assert!(matches!(
                (format, parallel.info_hash),
                (Format::V1, TorrentIdentity::V1(_))
                    | (Format::V2, TorrentIdentity::V2(_))
                    | (Format::Hybrid, TorrentIdentity::Hybrid(..))
            ));
            let stream = [
                b"h".as_slice(),
                a.as_slice(),
                b"prefix".as_slice(),
                b.as_slice(),
                b"tail".as_slice(),
            ]
            .concat();
            if format == Format::V1 {
                assert_eq!(parallel.total_length, stream.len() as u64);
                for (index, chunk) in stream.chunks(1 << 15).enumerate() {
                    assert_eq!(
                        meta.view().piece_hash(index as u32).unwrap().as_slice(),
                        Sha1::digest(chunk).as_slice()
                    );
                }
                assert_eq!(meta.layout().files().len(), 7);
            }
            let paths: Vec<_> = meta
                .layout()
                .files()
                .iter()
                .filter(|f| f.kind() == spill::protocol::metainfo::FileKind::Data)
                .map(|f| f.path().iter().map(|p| p.as_ref()).collect::<Vec<_>>())
                .collect();
            let nested = paths
                .iter()
                .position(|p| p == &vec![b"nested".as_slice(), b"b".as_slice()])
                .unwrap();
            let prefix = paths
                .iter()
                .position(|p| p == &vec![b"nested-first".as_slice()])
                .unwrap();
            assert_eq!(nested < prefix, format != Format::V1);
            let network = || NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(),
                allow_local_network: true,
                ..Default::default()
            };
            let settings = TorrentSettings {
                trackers: false,
                dht: false,
                pex: false,
                web_seeds: false,
                ..Default::default()
            };
            let seed = Session::bind(network(), Limits::default()).await.unwrap();
            let mut seed_options = TorrentOptions::new(
                TorrentSource::Metainfo(meta.clone()),
                Destination::Adopt(source.0.clone()),
            );
            seed_options.settings = settings.clone();
            seed.admit(seed_options)
                .unwrap()
                .wait_complete()
                .await
                .unwrap();
            let destination = Temp::new();
            let downloader = Session::bind(network(), Limits::default()).await.unwrap();
            let mut download = TorrentOptions::new(
                TorrentSource::Metainfo(meta),
                Destination::Create(destination.0.clone()),
            );
            download.settings = settings;
            download.peers = vec![seed.local_addr()];
            download.run = RunPolicy::UntilComplete;
            let handle = downloader.admit(download).unwrap();
            handle.join().await.unwrap();
            handle.wait_complete().await.unwrap();
            downloader.shutdown().await.unwrap();
            seed.shutdown().await.unwrap();
            for relative in [
                ".hidden",
                "a",
                "a-empty",
                "nested-first",
                "nested/0-empty",
                "nested/b",
                "z",
            ] {
                assert_eq!(
                    std::fs::read(destination.0.join("bundle").join(relative)).unwrap(),
                    std::fs::read(bundle.join(relative)).unwrap()
                );
            }
            assert!(!destination.0.join("bundle/nested/empty-dir").exists());
        }
    })
    .await
    .expect("created torrent transfer");
}

#[tokio::test]
async fn empty_and_non_utf8_sources_can_be_adopted_and_links_keep_their_names() {
    let root = Temp::new();
    let name = OsString::from_vec(b"empty-\xff".to_vec());
    let source = root.0.join(&name);
    std::fs::write(&source, []).unwrap();
    let options = Options {
        creation_date: None,
        ..Default::default()
    };
    let created = create::create(&source, &options, &mut |_, _| Ok(())).unwrap();
    assert_eq!(created.name, name);
    assert_eq!(created.piece_count, 0);
    let meta = Profile::default().parse_owned(&created.metainfo).unwrap();
    assert_eq!(meta.name(), b"empty-\xff");
    let session = Session::offline(Limits::default());
    let mut torrent = TorrentOptions::new(
        TorrentSource::Metainfo(meta),
        Destination::Adopt(root.0.clone()),
    );
    torrent.run = RunPolicy::Recheck;
    let outcome = session.admit(torrent).unwrap().outcome().await;
    assert!(outcome.recheck_finished && outcome.full_complete && outcome.error.is_none());
    session.shutdown().await.unwrap();

    let directory = root.0.join("directory");
    std::fs::create_dir(&directory).unwrap();
    std::fs::create_dir(directory.join("sub")).unwrap();
    std::fs::write(directory.join("sub/x"), b"abc").unwrap();
    symlink("sub/x", directory.join("file-link")).unwrap();
    symlink("sub", directory.join("dir-link")).unwrap();
    symlink("directory", root.0.join("root-link")).unwrap();
    let created = create::create(&root.0.join("root-link"), &options, &mut |_, _| Ok(())).unwrap();
    assert_eq!(created.name, "root-link");
    let meta = Profile::default().parse_owned(&created.metainfo).unwrap();
    assert_eq!(meta.layout().files().len(), 3);
    assert_eq!(meta.layout().total_length(), 9);
    assert_eq!(meta.layout().files()[0].path()[0].as_ref(), b"dir-link");
    assert_eq!(meta.layout().files()[1].path()[0].as_ref(), b"file-link");
    symlink("..", directory.join("sub/cycle")).unwrap();
    let error = create::create(&directory, &options, &mut |_, _| Ok(())).unwrap_err();
    assert!(error.to_string().contains("cycle"));
}

#[test]
fn creation_cancels_joins_and_rejects_changed_payloads_without_modifying_them() {
    let root = Temp::new();
    let source = root.0.join("x");
    let payload = vec![7; 1024 * 1024];
    std::fs::write(&source, &payload).unwrap();
    let options = Options {
        piece_length: 1 << 15,
        threads: 4,
        ..Default::default()
    };
    let error = create::create(&source, &options, &mut |done, _| {
        if done == 1 {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "cancel creation",
            ))
        } else {
            Ok(())
        }
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(std::fs::read(&source).unwrap(), payload);
    let error = create::create(&source, &options, &mut |done, _| {
        if done == 0 {
            std::fs::write(&source, b"changed")?;
        }
        Ok(())
    })
    .unwrap_err();
    assert!(error.to_string().contains("source changed"));
    assert_eq!(std::fs::read(&source).unwrap(), b"changed");
    let recovered = create::create(&source, &options, &mut |_, _| Ok(())).unwrap();
    let meta = Profile::default().parse(&recovered.metainfo).unwrap();
    assert_eq!(
        meta.piece_hash(0).unwrap().as_slice(),
        Sha1::digest(b"changed").as_slice()
    );
    assert!(matches!(
        meta.document().get(b"creation date").unwrap().value(),
        Value::Integer(_)
    ));
}
