use spill::protocol::peer::mse::{Error, Method, Methods, Negotiated, Negotiation, torrent_key};

const WIRE: [u8; 20] = [17; 20];

fn vector(mode: &str, field: &str) -> Vec<u8> {
    let key = format!("{mode}.{field}=");
    let line = include_str!("fixtures/mse.md")
        .lines()
        .find_map(|line| line.strip_prefix(&key))
        .unwrap();
    hex(line)
}

fn hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn handshake(id: u8) -> Vec<u8> {
    spill::protocol::peer::Handshake {
        reserved: [0; 8],
        info_hash: WIRE,
        peer_id: [id; 20],
    }
    .encode()
    .to_vec()
}

fn initiator() -> (Negotiation, Vec<u8>) {
    Negotiation::initiator(
        std::array::from_fn(|i| i as u8 + 1),
        &[85; 512],
        WIRE,
        Methods::Both,
        &handshake(65),
    )
}

fn responder(methods: Methods) -> Negotiation {
    Negotiation::responder(std::array::from_fn(|i| i as u8 + 21), methods, &[102; 512])
}

fn lookup(request: [u8; 20]) -> Option<[u8; 20]> {
    (request == torrent_key(WIRE)).then_some(WIRE)
}

fn finish(codec: &mut Negotiation, bytes: &[u8], chunk: usize) -> (Vec<u8>, Negotiated) {
    let mut output = Vec::new();
    for (i, part) in bytes.chunks(chunk).enumerate() {
        let progress = codec.receive(part, lookup).unwrap();
        output.extend(progress.output);
        if let Some(mut negotiated) = progress.negotiated {
            negotiated
                .surplus
                .extend_from_slice(&bytes[(i * chunk + part.len())..]);
            return (output, negotiated);
        }
    }
    panic!("incomplete vector");
}

#[test]
fn independent_transcripts_fragmentation_and_coalesced_application_data() {
    for (mode, method, accepted) in [
        ("rc4", Method::Rc4, Methods::Both),
        ("plaintext", Method::Plaintext, Methods::Plaintext),
    ] {
        for chunk in [1, 7, usize::MAX] {
            let (mut a, public) = initiator();
            let mut b = responder(accepted);
            assert_eq!(public, vector(mode, "public_a"));
            let response = b.receive(&public[..96], lookup).unwrap();
            assert_eq!(response.output, vector(mode, "public_b"));
            assert!(response.negotiated.is_none());
            let request = a.receive(&response.output, lookup).unwrap();
            assert_eq!(request.output, vector(mode, "request"));
            assert!(request.negotiated.is_none());

            let mut incoming = public[96..].to_vec();
            incoming.extend(&request.output);
            incoming.extend(vector(mode, "application_a"));
            let (reply, mut b) = finish(&mut b, &incoming, chunk);
            assert_eq!(reply, vector(mode, "reply"));
            assert_eq!(b.initial, handshake(65));
            assert_eq!(b.method, method);
            assert_eq!(b.wire_key, WIRE);
            if method == Method::Rc4 {
                b.decrypt.apply(&mut b.surplus);
            }
            assert_eq!(b.surplus, [0, 0, 0, 1, 2]);

            let mut incoming = reply;
            incoming.extend(vector(mode, "application_b"));
            let (output, mut a) = finish(&mut a, &incoming, chunk);
            assert!(output.is_empty());
            assert!(a.initial.is_empty());
            assert_eq!(a.method, method);
            if method == Method::Rc4 {
                a.decrypt.apply(&mut a.surplus);
            }
            assert_eq!(a.surplus, [handshake(66), vec![0, 0, 0, 1, 2]].concat());
            let mut application = vec![0, 0, 0, 1, 2];
            if method == Method::Rc4 {
                a.encrypt.apply(&mut application);
            }
            assert_eq!(application, vector(mode, "application_a"));
            let mut application = [handshake(66), vec![0, 0, 0, 1, 2]].concat();
            if method == Method::Rc4 {
                b.encrypt.apply(&mut application);
            }
            assert_eq!(application, vector(mode, "application_b"));
        }
    }
}

#[test]
fn rejects_invalid_dh_public_values() {
    let prime = hex(
        "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563",
    );
    let mut one = vec![0; 96];
    one[95] = 1;
    let mut minus_one = prime.clone();
    minus_one[95] -= 1;
    for bytes in [vec![0; 96], one, minus_one, prime, vec![255; 96]] {
        for initiates in [true, false] {
            let mut codec = if initiates {
                initiator().0
            } else {
                responder(Methods::Both)
            };
            assert!(matches!(
                codec.receive(&bytes, lookup),
                Err(Error::PublicKey)
            ));
        }
    }
}

#[test]
fn rejects_sync_beyond_maximum_padding() {
    let mut b = responder(Methods::Both);
    b.receive(&vector("rc4", "public_a")[..96], lookup).unwrap();
    let mut input = vec![85; 513];
    input.extend(vector("rc4", "request"));
    assert!(matches!(b.receive(&input, lookup), Err(Error::Sync)));

    let (mut a, _) = initiator();
    a.receive(&vector("rc4", "public_b")[..96], lookup).unwrap();
    let mut input = vec![102; 513];
    input.extend(vector("rc4", "reply"));
    assert!(matches!(a.receive(&input, lookup), Err(Error::Sync)));
}

#[test]
fn rejects_bad_offer_verification_padding_and_unknown_torrent() {
    for (offset, mask, error) in [
        (40, 1, Error::Verification),
        (51, 3, Error::Method),
        (52, 3, Error::Padding),
    ] {
        let mut b = responder(Methods::Both);
        b.receive(&vector("rc4", "public_a")[..96], lookup).unwrap();
        let mut request = vector("rc4", "request");
        request[offset] ^= mask;
        assert!(matches!(b.receive(&request, lookup), Err(actual) if actual == error));
    }
    let mut b = responder(Methods::Both);
    b.receive(&vector("rc4", "public_a")[..96], lookup).unwrap();
    assert!(matches!(
        b.receive(&vector("rc4", "request"), |_| None),
        Err(Error::UnknownTorrent)
    ));
}

#[test]
fn rejects_bad_or_unoffered_selection() {
    for selected in [0, 3, 4, 128] {
        let (mut a, _) = initiator();
        a.receive(&vector("rc4", "public_b")[..96], lookup).unwrap();
        let mut reply = vector("rc4", "reply");
        reply[11] ^= 2 ^ selected;
        assert!(matches!(a.receive(&reply, lookup), Err(Error::Method)));
    }
    let (mut a, public) = Negotiation::initiator(
        std::array::from_fn(|i| i as u8 + 1),
        &[],
        WIRE,
        Methods::Rc4,
        &[],
    );
    a.receive(&vector("rc4", "public_b"), lookup).unwrap();
    assert!(matches!(
        a.receive(&vector("plaintext", "reply"), lookup),
        Err(Error::Method)
    ));

    let (mut a, _) = Negotiation::initiator(
        std::array::from_fn(|i| i as u8 + 1),
        &[],
        WIRE,
        Methods::Rc4,
        &[],
    );
    let mut b = responder(Methods::Plaintext);
    let response = b.receive(&public, lookup).unwrap();
    let request = a.receive(&response.output, lookup).unwrap();
    assert!(matches!(
        b.receive(&request.output, lookup),
        Err(Error::Method)
    ));
}

#[test]
fn maximum_encrypted_padding_and_application_stream_positions() {
    for (mode, accepted) in [("rc4", Methods::Both), ("plaintext", Methods::Plaintext)] {
        let (mut a, _) = initiator();
        a.receive(&vector(mode, "public_b"), lookup).unwrap();
        let mut response = vector(mode, "reply_padded");
        response.extend(vector(mode, "application_b_padded"));
        let (_, mut stream) = finish(&mut a, &response, 1);
        if stream.method == Method::Rc4 {
            stream.decrypt.apply(&mut stream.surplus);
        }
        assert_eq!(
            stream.surplus,
            [handshake(66), vec![0, 0, 0, 1, 2]].concat()
        );

        let mut b = responder(accepted);
        b.receive(&vector(mode, "public_a")[..96], lookup).unwrap();
        let mut request = vector(mode, "request_padded");
        request.extend(vector(mode, "application_a_padded"));
        let (_, mut stream) = finish(&mut b, &request, 1);
        assert_eq!(stream.initial, handshake(65));
        if stream.method == Method::Rc4 {
            stream.decrypt.apply(&mut stream.surplus);
        }
        assert_eq!(stream.surplus, [0, 0, 0, 1, 2]);
    }
}

#[test]
fn unknown_offer_bits_are_ignored_and_oversized_response_padding_is_rejected() {
    let mut b = responder(Methods::Both);
    b.receive(&vector("rc4", "public_a")[..96], lookup).unwrap();
    let mut request = vector("rc4", "request");
    request[48] ^= 128;
    assert_eq!(
        b.receive(&request, lookup)
            .unwrap()
            .negotiated
            .unwrap()
            .method,
        Method::Rc4
    );

    let (mut a, _) = initiator();
    a.receive(&vector("rc4", "public_b"), lookup).unwrap();
    let mut reply = vector("rc4", "reply");
    reply[12] ^= 3;
    assert!(matches!(a.receive(&reply, lookup), Err(Error::Padding)));
}

#[test]
fn negotiation_completes_only_after_the_last_byte() {
    let public = vector("rc4", "public_a");
    let request = vector("rc4", "request");
    let transcript = [public, request].concat();
    let mut b = responder(Methods::Both);
    for byte in &transcript[..transcript.len() - 1] {
        let progress = b.receive(&[*byte], lookup).unwrap();
        assert!(progress.negotiated.is_none());
    }
    assert!(
        b.receive(&transcript[transcript.len() - 1..], lookup)
            .unwrap()
            .negotiated
            .is_some()
    );
}
