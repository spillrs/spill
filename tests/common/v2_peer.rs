//! Independent peer frames and deterministic payloads for the BEP 52 fixtures.
use std::io;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub fn payload(offset: usize, length: usize) -> Vec<u8> {
    (offset..offset + length).map(|i| (i % 251) as u8).collect()
}

pub fn pieces(lengths: &[usize], piece_length: usize) -> Vec<Vec<u8>> {
    lengths
        .iter()
        .flat_map(|&length| {
            (0..length)
                .step_by(piece_length)
                .map(move |offset| payload(offset, (length - offset).min(piece_length)))
        })
        .collect()
}

fn hello(key: [u8; 20], identity: u8) -> Vec<u8> {
    let mut hello = b"\x13BitTorrent protocol".to_vec();
    hello.extend_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
    hello.extend_from_slice(&key);
    hello.extend_from_slice(&[identity; 20]);
    hello
}

pub async fn initiate(peer: &mut TcpStream, key: [u8; 20], identity: u8, fast: bool) {
    peer.set_nodelay(true).unwrap();
    let mut hello = hello(key, identity);
    if fast {
        hello[27] |= 0x04;
    }
    peer.write_all(&hello).await.unwrap();
    check_hello(peer, key).await;
}
pub async fn accept(peer: &mut TcpStream, key: [u8; 20], identity: u8) {
    peer.set_nodelay(true).unwrap();
    check_hello(peer, key).await;
    peer.write_all(&hello(key, identity)).await.unwrap();
}
async fn check_hello(peer: &mut TcpStream, key: [u8; 20]) -> [u8; 68] {
    let mut bytes = [0; 68];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..20], b"\x13BitTorrent protocol");
    assert_eq!(&bytes[28..48], &key);
    assert_eq!(bytes[27] & 1, 0, "direct-only peers must not advertise DHT");
    bytes
}

pub async fn accept_hybrid(peer: &mut TcpStream, v1: [u8; 20], reply: [u8; 20], identity: u8) {
    peer.set_nodelay(true).unwrap();
    let bytes = check_hello(peer, v1).await;
    assert_ne!(bytes[27] & 0x10, 0, "hybrid dial must offer upgrade");
    peer.write_all(&hello(reply, identity)).await.unwrap();
}

pub async fn initiate_hybrid(
    peer: &mut TcpStream,
    key: [u8; 20],
    expected: [u8; 20],
    identity: u8,
    upgrade: bool,
) {
    peer.set_nodelay(true).unwrap();
    let mut bytes = hello(key, identity);
    if upgrade {
        bytes[27] |= 0x10;
    }
    peer.write_all(&bytes).await.unwrap();
    check_hello(peer, expected).await;
}

pub async fn frame(peer: &mut TcpStream) -> io::Result<Vec<u8>> {
    let length = peer.read_u32().await?;
    assert!(length <= 1024 * 1024);
    let mut body = vec![0; length as usize];
    peer.read_exact(&mut body).await?;
    if body.first() == Some(&20) {
        assert!(
            !body.windows(6).any(|bytes| bytes == b"ut_pex"),
            "v2 must not advertise PEX"
        );
    }
    Ok(body)
}
pub async fn send(peer: &mut TcpStream, id: u8, payload: &[u8]) {
    write_frame(peer, id, payload).await.unwrap();
}
async fn write_frame(peer: &mut TcpStream, id: u8, payload: &[u8]) -> io::Result<()> {
    let mut bytes = ((payload.len() + 1) as u32).to_be_bytes().to_vec();
    bytes.push(id);
    bytes.extend_from_slice(payload);
    peer.write_all(&bytes).await
}
pub async fn next(peer: &mut TcpStream, id: u8) -> Vec<u8> {
    loop {
        let body = frame(peer).await.unwrap();
        if body.first() == Some(&id) {
            return body;
        }
        assert_ne!(body.first(), Some(&21), "no outbound hash requests");
    }
}
pub fn request(index: u32, begin: u32, length: u32) -> Vec<u8> {
    [
        index.to_be_bytes(),
        begin.to_be_bytes(),
        length.to_be_bytes(),
    ]
    .concat()
}

pub async fn serve_with_delayed_have(
    mut socket: TcpStream,
    key: [u8; 20],
    pieces: Vec<Vec<u8>>,
    offering: tokio::sync::oneshot::Receiver<()>,
) {
    accept(&mut socket, key, 97).await;
    send(&mut socket, 5, &[0xbc]).await;
    send(&mut socket, 1, &[]).await;
    let mut offering = Some(offering);
    loop {
        tokio::select! {
            _ = async { offering.as_mut().unwrap().await.unwrap() }, if offering.is_some() => {
                send(&mut socket, 4, &1u32.to_be_bytes()).await;
                offering = None;
            }
            body = frame(&mut socket) => {
                let Ok(body) = body else { break };
                if body.first() == Some(&6) {
                    let index = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
                    let begin = u32::from_be_bytes(body[5..9].try_into().unwrap()) as usize;
                    let length = u32::from_be_bytes(body[9..13].try_into().unwrap()) as usize;
                    let mut reply = body[1..9].to_vec();
                    reply.extend_from_slice(&pieces[index][begin..begin + length]);
                    send(&mut socket, 7, &reply).await;
                }
            }
        }
    }
}

pub async fn serve(
    mut peer: TcpStream,
    key: [u8; 20],
    identity: u8,
    pieces: Vec<Vec<u8>>,
    corrupt: bool,
) -> Vec<u32> {
    accept(&mut peer, key, identity).await;
    let mut bits = vec![0; pieces.len().div_ceil(8)];
    for index in 0..pieces.len() {
        bits[index / 8] |= 0x80 >> (index % 8);
    }
    serve_advertised(peer, pieces, &bits, corrupt).await
}

pub async fn serve_advertised(
    mut peer: TcpStream,
    pieces: Vec<Vec<u8>>,
    bits: &[u8],
    corrupt: bool,
) -> Vec<u32> {
    send(&mut peer, 5, bits).await;
    send(&mut peer, 1, &[]).await;
    let mut requests = Vec::new();
    let mut corrupted = None;
    while let Ok(body) = frame(&mut peer).await {
        match body.first() {
            Some(6) => {
                assert_eq!(body.len(), 13);
                let index = u32::from_be_bytes(body[1..5].try_into().unwrap());
                let begin = u32::from_be_bytes(body[5..9].try_into().unwrap()) as usize;
                let length = u32::from_be_bytes(body[9..13].try_into().unwrap()) as usize;
                let mut bytes = body[1..9].to_vec();
                bytes.extend_from_slice(&pieces[index as usize][begin..begin + length]);
                if corrupt && corrupted.is_none() {
                    bytes[8] ^= 1;
                    corrupted = Some(index);
                }
                let sent: io::Result<()> = async {
                    if requests.is_empty() {
                        write_frame(&mut peer, 0, &[]).await?;
                    }
                    write_frame(&mut peer, 7, &bytes).await?;
                    if requests.is_empty() {
                        write_frame(&mut peer, 1, &[]).await?;
                    }
                    Ok(())
                }
                .await;
                requests.push(index);
                if let Err(error) = sent {
                    assert!(
                        matches!(
                            error.kind(),
                            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                        ),
                        "{error}"
                    );
                    break;
                }
            }
            Some(4) if corrupt => {
                let index = u32::from_be_bytes(body[1..5].try_into().unwrap());
                assert_ne!(Some(index), corrupted, "corrupt data was advertised");
            }
            Some(21) => panic!("metainfo downloads need no hash requests"),
            _ => {}
        }
    }
    requests
}
