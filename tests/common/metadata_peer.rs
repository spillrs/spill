//! Independent BEP 9 / BEP 52 source with hand-encoded frames.
use crate::v2_peer as raw;
use spill::protocol::bencode::{self, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn extended(socket: &mut TcpStream, id: u8, payload: &[u8]) {
    let mut frame = ((payload.len() + 2) as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&[20, id]);
    frame.extend_from_slice(payload);
    socket.write_all(&frame).await.unwrap();
}

pub async fn tracker(contacts: &[std::net::SocketAddr]) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let compact: Vec<_> = contacts
        .iter()
        .flat_map(|address| {
            let std::net::SocketAddr::V4(address) = address else {
                unreachable!()
            };
            [
                address.ip().octets().as_slice(),
                &address.port().to_be_bytes(),
            ]
            .concat()
        })
        .collect();
    let task = tokio::spawn(async move {
        // A cancelled announce can leave an idle pooled connection open.
        let mut requests = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                Some(result) = requests.join_next() => { result.unwrap(); }
                accepted = listener.accept() => {
                    let (socket, _) = accepted.unwrap();
                    let compact = compact.clone();
                    requests.spawn(async move {
                        let mut socket = BufReader::new(socket);
                        let mut line = String::new();
                        loop {
                            line.clear();
                            if socket.read_line(&mut line).await.unwrap() == 0 {
                                return;
                            }
                            if line == "\r\n" {
                                break;
                            }
                        }
                        let mut body = format!("d8:intervali1e5:peers{}:", compact.len()).into_bytes();
                        body.extend_from_slice(&compact);
                        body.push(b'e');
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = socket.write_all(header.as_bytes()).await;
                        let _ = socket.write_all(&body).await;
                    });
                }
            }
        }
    });
    (url, task)
}

pub fn hash_reply(meta: &spill::protocol::metainfo::OwnedMetainfo, header: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let doc = meta.decode_document();
    let Value::Bytes(layer) = doc
        .get(b"piece layers")
        .unwrap()
        .get(&header[..32])
        .unwrap()
        .value()
    else {
        unreachable!()
    };
    let base = u32::from_be_bytes(header[32..36].try_into().unwrap());
    let index = u32::from_be_bytes(header[36..40].try_into().unwrap()) as usize;
    let count = u32::from_be_bytes(header[40..44].try_into().unwrap()) as usize;
    let proofs = u32::from_be_bytes(header[44..48].try_into().unwrap());
    let mut padding = [0; 32];
    for _ in 0..base {
        padding = Sha256::digest([padding, padding].concat()).into();
    }
    let mut nodes: Vec<[u8; 32]> = layer.as_chunks::<32>().0.to_vec();
    nodes.resize(nodes.len().next_power_of_two(), padding);
    let mut reply = header.to_vec();
    for hash in &nodes[index..index + count] {
        reply.extend_from_slice(hash);
    }
    let mut position = index / count;
    let skip = count.ilog2();
    for level in 0..skip + proofs.saturating_sub(skip - 1) {
        if level >= skip {
            reply.extend_from_slice(&nodes[position ^ 1]);
            position /= 2;
        }
        nodes = nodes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| Sha256::digest([pair[0], pair[1]].concat()).into())
            .collect();
    }
    reply
}

/// `fault`: 1 corrupts metadata, 2 corrupts proofs, 3 rejects hashes, 4 stalls
/// hash replies, 7 accepts only v1 without upgrade. Payload and layers come
/// from the independent reference fixtures.
pub async fn serve_full(
    listener: TcpListener,
    meta: spill::protocol::metainfo::OwnedMetainfo,
    fault: u8,
) {
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let meta = meta.clone();
        tasks.spawn(async move {
            let mut hello = [0; 68];
            if socket.read_exact(&mut hello).await.is_err() {
                return;
            }
            let key = hello[28..48].to_vec();
            let identity = meta.info_hash();
            assert!(
                key == identity.wire_key()
                    || identity.v2().is_some_and(|hash| key == hash.wire_key())
            );
            if fault == 7 {
                assert_eq!(key.as_slice(), identity.v1().unwrap().as_bytes());
                assert_eq!(hello[27] & 0x10, 0, "v1-only source cannot upgrade");
            }
            // Bootstrap and payload connections keep the key the caller chose.
            hello[20..28].copy_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
            hello[48..].fill(95);
            if socket.write_all(&hello).await.is_err() {
                return;
            }
            let v2 = identity.v2().is_some_and(|hash| key == hash.wire_key());
            let hello = format!(
                "d1:md11:ut_metadatai3ee13:metadata_sizei{}ee",
                meta.info_bytes().len()
            );
            extended(&mut socket, 0, hello.as_bytes()).await;
            let mut metadata_id = None;
            let mut bits = vec![0; meta.layout().piece_count().div_ceil(8) as usize];
            for index in 0..meta.layout().piece_count() {
                bits[index as usize / 8] |= 0x80 >> (index % 8);
            }
            raw::send(&mut socket, 5, &bits).await;
            raw::send(&mut socket, 1, &[]).await;
            while let Ok(frame) = raw::frame(&mut socket).await {
                match frame.as_slice() {
                    [20, 0, payload @ ..] => {
                        let doc = bencode::decode(payload, Default::default()).unwrap();
                        let Value::Integer(id) =
                            doc.get(b"m").unwrap().get(b"ut_metadata").unwrap().value()
                        else {
                            unreachable!()
                        };
                        metadata_id = Some(*id as u8);
                    }
                    [20, 3, payload @ ..] => {
                        let doc = bencode::decode(payload, Default::default()).unwrap();
                        let Value::Integer(piece) = doc.get(b"piece").unwrap().value() else {
                            unreachable!()
                        };
                        let mut info = meta.info_bytes().to_vec();
                        if fault == 1 {
                            *info.last_mut().unwrap() ^= 1;
                        }
                        let start = *piece as usize * 16384;
                        let end = (start + 16384).min(info.len());
                        let mut reply = format!(
                            "d8:msg_typei1e5:piecei{piece}e10:total_sizei{}ee",
                            info.len()
                        )
                        .into_bytes();
                        reply.extend_from_slice(&info[start..end]);
                        extended(&mut socket, metadata_id.unwrap(), &reply).await;
                    }
                    [21, header @ ..] => {
                        assert!(v2, "hash requests require v2 mode");
                        assert_eq!(header.len(), 48);
                        if fault == 4 {
                            loop {
                                if socket.write_all(&[0; 4]).await.is_err() {
                                    return;
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            }
                        }
                        if fault == 3 {
                            raw::send(&mut socket, 23, header).await;
                            continue;
                        }
                        let mut reply = hash_reply(&meta, header);
                        if fault == 2 {
                            reply[48] ^= 1;
                        }
                        if fault == 5 {
                            reply[0] ^= 1;
                        }
                        if fault == 6 {
                            reply.extend_from_slice(&[0; 32]);
                        }
                        raw::send(&mut socket, 22, &reply).await;
                    }
                    [6, request @ ..] => {
                        let index = u32::from_be_bytes(request[..4].try_into().unwrap());
                        let begin = u32::from_be_bytes(request[4..8].try_into().unwrap());
                        let length = u32::from_be_bytes(request[8..12].try_into().unwrap());
                        let start =
                            meta.layout().piece_range(index).unwrap().start + u64::from(begin);
                        let mut reply = request[..8].to_vec();
                        for segment in meta
                            .layout()
                            .segments(start..start + u64::from(length))
                            .unwrap()
                        {
                            if segment.kind == spill::protocol::metainfo::FileKind::Padding {
                                reply.resize(reply.len() + segment.length as usize, 0);
                            } else {
                                reply.extend(
                                    (segment.file_offset..segment.file_offset + segment.length)
                                        .map(|i| (i % 251) as u8),
                                );
                            }
                        }
                        raw::send(&mut socket, 7, &reply).await;
                    }
                    _ => {}
                }
            }
        });
    }
}
