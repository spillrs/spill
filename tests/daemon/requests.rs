use crate::common::{Daemon, Temp, peer_frame, raw_peer, seed_metainfo};
use serde_json::json;
use sha1::Digest;
use std::time::Duration;

#[tokio::test]
async fn overlapping_uploads_in_opposite_orders_do_not_deadlock() {
    use spill::protocol::peer::message::{BlockRequest, Message};
    use tokio::io::AsyncWriteExt;
    const BLOCK: usize = 16384;
    const BLOCKS: usize = 128;
    let payload: Vec<_> = (0..BLOCK * BLOCKS).map(|i| (i / BLOCK) as u8).collect();
    let bytes = seed_metainfo("x", &payload, None);
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&bytes)
        .unwrap()
        .info_hash()
        .as_bytes();
    let root = Temp::new();
    std::fs::create_dir_all(root.0.join("downloads")).unwrap();
    std::fs::write(root.0.join("downloads/x"), &payload).unwrap();
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "seeding").await;
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut a = raw_peer(&address, hash, 11).await;
    let mut b = raw_peer(&address, hash, 12).await;
    async fn unchoke(peer: &mut tokio::net::TcpStream) {
        peer.write_all(&[0, 0, 0, 1, 2]).await.unwrap();
        loop {
            if matches!(
                Message::decode(&peer_frame(peer).await, 4 * 1024 * 1024)
                    .unwrap()
                    .0,
                Message::Unchoke
            ) {
                break;
            }
        }
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(unchoke(&mut a), unchoke(&mut b));
    })
    .await
    .unwrap();
    let requests = |reverse: bool| {
        let mut bytes = Vec::new();
        for n in 0..BLOCKS {
            let block = if reverse { BLOCKS - 1 - n } else { n };
            let message = Message::Request(BlockRequest {
                index: 0,
                begin: (block * BLOCK) as u32,
                length: BLOCK as u32,
            });
            let start = bytes.len();
            bytes.resize(start + message.encoded_len(4 * 1024 * 1024).unwrap(), 0);
            message
                .encode(&mut bytes[start..], 4 * 1024 * 1024)
                .unwrap();
        }
        bytes
    };
    let forward = requests(false);
    let reverse = requests(true);
    async fn receive(peer: &mut tokio::net::TcpStream, payload: &[u8]) {
        let mut seen = [false; BLOCKS];
        for _ in 0..BLOCKS {
            loop {
                let frame = peer_frame(peer).await;
                if let Message::Piece {
                    index,
                    begin,
                    block,
                } = Message::decode(&frame, 4 * 1024 * 1024).unwrap().0
                {
                    assert_eq!(index, 0);
                    assert_eq!(block, &payload[begin as usize..begin as usize + BLOCK]);
                    assert!(!seen[begin as usize / BLOCK]);
                    seen[begin as usize / BLOCK] = true;
                    break;
                }
            }
        }
    }
    let completed = tokio::time::timeout(Duration::from_secs(5), async {
        let (a_sent, b_sent) = tokio::join!(a.write_all(&forward), b.write_all(&reverse));
        a_sent.unwrap();
        b_sent.unwrap();
        tokio::join!(receive(&mut a, &payload), receive(&mut b, &payload));
    })
    .await;
    if completed.is_err() {
        let row = daemon.call(json!({"op":"get", "id":id})).await;
        panic!("overlapping upload queues must drain before request timeout: {row}");
    }
    daemon.stop().await;
}

#[tokio::test]
async fn endgame_requests_progress_while_other_duplicates_are_stalled() {
    use spill::protocol::peer::{
        Handshake,
        message::{BlockRequest, Message},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    async fn connect(address: &str, hash: [u8; 20], id: u8) -> TcpStream {
        let mut peer = TcpStream::connect(address).await.unwrap();
        let mut reserved = [0; 8];
        reserved[7] = 4;
        peer.write_all(
            &Handshake {
                reserved,
                info_hash: hash,
                peer_id: [id; 20],
            }
            .encode(),
        )
        .await
        .unwrap();
        let mut hello = [0; 68];
        peer.read_exact(&mut hello).await.unwrap();
        assert_ne!(hello[27] & 4, 0);
        peer.write_all(&[0, 0, 0, 1, 14, 0, 0, 0, 1, 1])
            .await
            .unwrap();
        peer
    }
    async fn frame(peer: &mut TcpStream) -> Vec<u8> {
        let length = peer.read_u32().await.unwrap();
        let mut bytes = length.to_be_bytes().to_vec();
        bytes.resize(4 + length as usize, 0);
        peer.read_exact(&mut bytes[4..]).await.unwrap();
        bytes
    }
    async fn request(peer: &mut TcpStream) -> BlockRequest {
        loop {
            if let Message::Request(request) =
                Message::decode(&frame(peer).await, 1024 * 1024).unwrap().0
            {
                return request;
            }
        }
    }
    async fn respond(peer: &mut TcpStream, request: BlockRequest, payload: &[u8]) {
        let start = request.begin as usize;
        let message = Message::Piece {
            index: request.index,
            begin: request.begin,
            block: &payload[start..start + request.length as usize],
        };
        let mut bytes = vec![0; message.encoded_len(1024 * 1024).unwrap()];
        message.encode(&mut bytes, 1024 * 1024).unwrap();
        peer.write_all(&bytes).await.unwrap();
    }

    const BLOCK: usize = 16384;
    let payload: Vec<_> = (0..16 * BLOCK).map(|i| ((i / BLOCK) ^ i) as u8).collect();
    let mut bytes = format!(
        "d4:infod6:lengthi{}e4:name1:x12:piece lengthi{}e6:pieces20:",
        payload.len(),
        payload.len()
    )
    .into_bytes();
    bytes.extend_from_slice(&sha1::Sha1::digest(&payload));
    bytes.extend_from_slice(b"ee");
    let hash = *spill::engine::profile::Profile::DEFAULT
        .parse_owned(&bytes)
        .unwrap()
        .info_hash()
        .as_bytes();
    let root = Temp::new();
    let mut daemon = Daemon::start(&root.0, "web_seeds=false", None).await;
    let id = daemon.add(&bytes, None).await;
    daemon.state(&id, "downloading").await;
    let address = daemon.call(json!({"op":"health"})).await["listen"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut primary = connect(&address, hash, 1).await;
    let mut held = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        let first = request(&mut primary).await;
        // Grow the normal peer window past four before withholding the tail.
        tokio::time::sleep(Duration::from_millis(300)).await;
        respond(&mut primary, first, &payload).await;
        while held.len() < 8 {
            let request = request(&mut primary).await;
            if request.begin < (8 * BLOCK) as u32 {
                respond(&mut primary, request, &payload).await;
            } else {
                held.push(request);
            }
        }
    })
    .await
    .expect("primary must acquire the eight tail blocks");
    primary.write_all(&[0, 0, 0, 1, 0]).await.unwrap();

    let mut stalled = connect(&address, hash, 2).await;
    let mut duplicates = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..4 {
            let request = request(&mut stalled).await;
            assert!(held.contains(&request));
            duplicates.push(request);
        }
    })
    .await
    .expect("second peer must acquire four duplicate requests");
    // Fast choke retains these requests: the peer may still fulfill them later.
    stalled.write_all(&[0, 0, 0, 1, 0]).await.unwrap();

    let mut healthy = connect(&address, hash, 3).await;
    let mut rescued = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..4 {
            let request = request(&mut healthy).await;
            assert!(held.contains(&request));
            assert!(!duplicates.contains(&request));
            assert!(!rescued.contains(&request));
            rescued.push(request);
        }
    })
    .await
    .expect("healthy peer must receive the other tail blocks without a 30-second timeout");

    let mut fourth = connect(&address, hash, 4).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), request(&mut fourth))
            .await
            .is_err(),
        "each outstanding block already has two live copies"
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = daemon.call(json!({"op":"get","id":id})).await;
            let peers = row["transfer"]["peers"].as_array().unwrap();
            if peers.len() == 4 {
                let free = row["transfer"]["writer_free_slots"].as_u64().unwrap();
                let slots = spill::engine::profile::Profile::DEFAULT.write_queue_bytes
                    / spill::engine::profile::BLOCK_LEN as usize;
                assert!(free <= slots as u64, "{row}");
                assert_eq!(row["transfer"]["upload_reads_in_flight"], 0);
                assert!(
                    peers
                        .iter()
                        .all(|peer| peer["detail"]["fast_extension"] == true)
                );
                assert!(
                    peers
                        .iter()
                        .any(|peer| peer["detail"]["remote_choking"] == true
                            && peer["detail"]["assigned_requests"] == 8)
                );
                assert_eq!(
                    peers
                        .iter()
                        .map(|peer| peer["detail"]["authorized_requests"].as_u64().unwrap())
                        .sum::<u64>(),
                    16
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("JSON must expose the stalled Fast peers and outstanding requests");
    for request in duplicates {
        respond(&mut stalled, request, &payload).await;
    }
    for request in rescued {
        respond(&mut healthy, request, &payload).await;
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut canceled = Vec::new();
        while canceled.len() < held.len() {
            if let Message::Cancel(request) =
                Message::decode(&frame(&mut primary).await, 1024 * 1024)
                    .unwrap()
                    .0
            {
                assert!(held.contains(&request));
                assert!(!canceled.contains(&request));
                canceled.push(request);
            }
        }
    })
    .await
    .expect("fulfilled duplicates must cancel all original requests");
    daemon.state(&id, "seeding").await;
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), payload);
    daemon.stop().await;
}
