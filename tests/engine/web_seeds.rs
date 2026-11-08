use crate::{
    payload::{Temp, seed_metainfo},
    v2_peer::payload,
};
use spill::{
    engine::{
        profile::Profile,
        session::{Limits, NetworkOptions, Session},
        settings::TorrentSettings,
        torrent::{Destination, TorrentOptions, TorrentSource},
    },
    protocol::{
        bencode::{self, Node, Value},
        metainfo::{FileKind, OwnedMetainfo},
    },
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

fn rename<'a>(node: &Node<'a>) -> Node<'a> {
    fn component(bytes: &[u8]) -> &[u8] {
        match bytes {
            b"odd" => "odd é space".as_bytes(),
            _ => bytes,
        }
    }
    Node::new(match node.value() {
        Value::Dictionary(fields) => Value::Dictionary(
            fields
                .iter()
                .map(|(key, value)| (component(key), rename(value)))
                .collect(),
        ),
        Value::List(list) => Value::List(list.iter().map(rename).collect()),
        Value::Bytes(bytes) => Value::Bytes(component(bytes)),
        value => value.clone(),
    })
}
fn metainfo(fixture: &[u8], urls: &[String]) -> OwnedMetainfo {
    let doc = bencode::decode(fixture, Default::default()).unwrap();
    let doc = rename(&doc);
    let Value::Dictionary(fields) = doc.value() else {
        unreachable!()
    };
    let mut fields = fields.clone();
    fields.insert(
        b"url-list",
        Node::new(Value::List(
            urls.iter()
                .map(|url| Node::new(Value::Bytes(url.as_bytes())))
                .collect(),
        )),
    );
    let bytes = bencode::encode(&Node::new(Value::Dictionary(fields)), Default::default()).unwrap();
    Profile::default().parse_owned(&bytes).unwrap()
}

#[tokio::test]
async fn v2_and_hybrid_web_seeds_respect_file_tails_padding_and_encoded_paths() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for fixture in [
            include_bytes!("../protocol/fixtures/v2.torrent").as_slice(),
            include_bytes!("../protocol/fixtures/v2-leaves.torrent").as_slice(),
            include_bytes!("../protocol/fixtures/hybrid.torrent").as_slice(),
            include_bytes!("../protocol/fixtures/libtorrent-hybrid.torrent").as_slice(),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let meta = metainfo(fixture, &[format!("{url}a/"), format!("{url}b/")]);
            let bad_mirror = Arc::new(Mutex::new(None::<String>));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let seen = requests.clone();
            let server = tokio::spawn(async move {
                loop {
                    let (socket, _) = listener.accept().await.unwrap();
                    let seen = seen.clone();
                    let bad_mirror = bad_mirror.clone();
                    tokio::spawn(async move {
                        let mut socket = BufReader::new(socket);
                        let mut line = String::new();
                        if socket.read_line(&mut line).await.unwrap() == 0 { return; }
                        let path = line.split_whitespace().nth(1).unwrap().to_owned();
                        let (mirror, relative) = path[1..].split_once('/').unwrap();
                        let corrupt = {
                            let mut bad = bad_mirror.lock().unwrap();
                            bad.get_or_insert_with(|| mirror.to_owned()) == mirror
                        };
                        let length = match format!("/{relative}").as_str() {
                            "/vectors/block" => 16384,
                            "/vectors/odd%20%C3%A9%20space" => 65659,
                            "/vectors/one" => 1,
                            "/vectors/piece" => 32768,
                            "/vectors/tail" => 123,
                            _ => panic!("unexpected HTTP path: {path}"),
                        };
                        let mut range = None;
                        loop {
                            line.clear();
                            socket.read_line(&mut line).await.unwrap();
                            if line == "\r\n" { break; }
                            if let Some(value) = line.to_ascii_lowercase().strip_prefix("range: bytes=") {
                                let (start, end) = value.trim().split_once('-').unwrap();
                                range = Some((start.parse::<usize>().unwrap(), end.parse::<usize>().unwrap()));
                            }
                        }
                        let (start, end) = range.unwrap();
                        assert!(start <= end && end < length, "request crossed real file tail");
                        seen.lock().unwrap().push((path, start, end));
                        let mut body = payload(start, end - start + 1);
                        if corrupt { body[0] ^= 1; }
                        if socket.write_all(format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{length}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.is_ok() {
                            // Retiring a corrupt mirror can close an in-flight response.
                            let _ = socket.write_all(&body).await;
                        }
                    });
                }
            });
            let session = Session::bind(NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(), allow_local_network: true, utp: false,
                ..Default::default()
            }, Limits::default()).await.unwrap();
            let root = Temp::new();
            let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta.clone()), Destination::Create(root.0.clone()));
            options.settings = TorrentSettings { trackers: false, dht: false, pex: false, web_seeds: true, ..Default::default() };
            let handle = session.admit(options).unwrap();
            handle.wait_complete().await.unwrap();
            assert!(handle.snapshot().progress.hash_failed_pieces > 0);
            assert!(!requests.lock().unwrap().is_empty());
            for file in meta.layout().files().iter().filter(|file| file.kind() == FileKind::Data) {
                let mut path = root.0.clone();
                if meta.layout().is_multi_file() { path.push(std::str::from_utf8(meta.name()).unwrap()); }
                for part in file.path() { path.push(std::str::from_utf8(part).unwrap()); }
                assert_eq!(std::fs::read(path).unwrap(), payload(0, file.length() as usize));
            }
            session.shutdown().await.unwrap();
            server.abort();
        }
    }).await.unwrap();
}

#[tokio::test]
async fn gzip_labeled_web_seed_preserves_raw_archive_bytes() {
    tokio::time::timeout(Duration::from_secs(10), async {
        const ARCHIVE: &[u8] = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\xcbH\xcd\xc9\xc9\x07\x00\x86\xa6\x106\x05\x00\x00\x00";
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/archive.gz", listener.local_addr().unwrap());
        let meta = metainfo(&seed_metainfo("archive.gz", ARCHIVE, None), &[url]);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(&mut socket);
            let mut range_seen = false;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" { break; }
                if line.to_ascii_lowercase().starts_with("range:") {
                    assert_eq!(line.trim(), format!("range: bytes=0-{}", ARCHIVE.len() - 1));
                    range_seen = true;
                }
            }
            assert!(range_seen);
            socket.write_all(format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 0-{}/{}\r\nContent-Encoding: x-gzip\r\nConnection: close\r\n\r\n",
                ARCHIVE.len(), ARCHIVE.len() - 1, ARCHIVE.len()
            ).as_bytes()).await.unwrap();
            socket.write_all(ARCHIVE).await.unwrap();
        });
        let session = Session::bind(NetworkOptions {
            listen: "127.0.0.1:0".parse().unwrap(), allow_local_network: true, utp: false,
            ..Default::default()
        }, Limits::default()).await.unwrap();
        let root = Temp::new();
        let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta), Destination::Create(root.0.clone()));
        options.settings = TorrentSettings { trackers: false, dht: false, pex: false, web_seeds: true, ..Default::default() };
        let handle = session.admit(options).unwrap();
        handle.wait_complete().await.unwrap();
        assert_eq!(handle.snapshot().progress.hash_failed_pieces, 0);
        assert_eq!(std::fs::read(root.0.join("archive.gz")).unwrap(), ARCHIVE);
        server.await.unwrap();
        session.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn inconsistent_hybrid_commitments_fail_content_with_an_honest_web_seed() {
    use sha1::{Digest, Sha1};
    use sha2::Sha256;
    use std::collections::BTreeMap;
    tokio::time::timeout(Duration::from_secs(15), async {
        for v1_matches in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/x", listener.local_addr().unwrap());
            let sha1 = Sha1::digest(if v1_matches { b"abc" } else { b"abd" });
            let root = Sha256::digest(if v1_matches { b"abd" } else { b"abc" });
            let leaf = Node::new(Value::Dictionary(BTreeMap::from([
                (b"length".as_slice(), Node::new(Value::Integer(3))),
                (b"pieces root".as_slice(), Node::new(Value::Bytes(&root))),
            ])));
            let tree = Node::new(Value::Dictionary(BTreeMap::from([
                (b"x".as_slice(), Node::new(Value::Dictionary(BTreeMap::from([(b"".as_slice(), leaf)])))),
            ])));
            let info = Node::new(Value::Dictionary(BTreeMap::from([
                (b"file tree".as_slice(), tree),
                (b"length".as_slice(), Node::new(Value::Integer(3))),
                (b"meta version".as_slice(), Node::new(Value::Integer(2))),
                (b"name".as_slice(), Node::new(Value::Bytes(b"x"))),
                (b"piece length".as_slice(), Node::new(Value::Integer(32768))),
                (b"pieces".as_slice(), Node::new(Value::Bytes(&sha1))),
            ])));
            let doc = Node::new(Value::Dictionary(BTreeMap::from([
                (b"info".as_slice(), info),
                (b"piece layers".as_slice(), Node::new(Value::Dictionary(BTreeMap::new()))),
                (b"url-list".as_slice(), Node::new(Value::Bytes(url.as_bytes()))),
            ])));
            let bytes = bencode::encode(&doc, Default::default()).unwrap();
            let meta = Profile::default().parse_owned(&bytes).unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(&mut socket);
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    if line == "\r\n" { break; }
                }
                socket.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 0-2/3\r\nConnection: close\r\n\r\nabc").await.unwrap();
            });
            let session = Session::bind(NetworkOptions {
                listen: "127.0.0.1:0".parse().unwrap(), allow_local_network: true, utp: false,
                ..Default::default()
            }, Limits::default()).await.unwrap();
            let root = Temp::new();
            let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta), Destination::Create(root.0.clone()));
            options.settings = TorrentSettings { trackers: false, dht: false, pex: false, web_seeds: true, ..Default::default() };
            let handle = session.admit(options).unwrap();
            let outcome = handle.outcome().await;
            assert!(outcome.cleanup_complete && !outcome.full_complete);
            assert!(
                outcome.error.as_ref().unwrap().to_string()
                    .contains("inconsistent hybrid piece commitments"),
                "{outcome:?}"
            );
            assert_eq!(handle.snapshot().progress.verified_pieces, 0);
            session.shutdown().await.unwrap();
            server.await.unwrap();
        }
    }).await.unwrap();
}
