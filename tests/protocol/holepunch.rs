use spill::protocol::{
    bencode::{self, Limits, Node, Value},
    peer::{
        holepunch::{self, Error, Kind, Message},
        pex,
    },
};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, SocketAddrV6},
};

#[test]
fn holepunch_golden_vectors_and_lengths() {
    let v4: SocketAddr = "192.0.2.1:6881".parse().unwrap();
    let v6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
    let endpoints: &[(SocketAddr, &[u8])] = &[
        (v4, &[0, 192, 0, 2, 1, 0x1a, 0xe1]),
        (
            v6,
            &[
                1, 0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x1a, 0xe1,
            ],
        ),
    ];
    for &(addr, endpoint) in endpoints {
        for (message_type, kind, code) in [
            (0, Kind::Rendezvous, 0u32),
            (1, Kind::Connect, 0),
            (
                2,
                Kind::Error(holepunch::NO_SUCH_PEER),
                holepunch::NO_SUCH_PEER,
            ),
            (
                2,
                Kind::Error(holepunch::NOT_CONNECTED),
                holepunch::NOT_CONNECTED,
            ),
            (2, Kind::Error(holepunch::NO_SUPPORT), holepunch::NO_SUPPORT),
            (2, Kind::Error(holepunch::NO_SELF), holepunch::NO_SELF),
            (2, Kind::Error(0xffff_ffff), 0xffff_ffff),
        ] {
            let message = Message { kind, addr };
            let mut wire = vec![message_type];
            wire.extend_from_slice(endpoint);
            let short_len = wire.len();
            wire.extend_from_slice(&code.to_be_bytes());
            assert_eq!(message.encode().unwrap(), wire);
            assert_eq!(Message::parse(&wire), Ok(message));
            for end in 0..wire.len() {
                if message_type != 2 && end == short_len {
                    assert_eq!(Message::parse(&wire[..end]), Ok(message));
                } else {
                    assert_eq!(Message::parse(&wire[..end]), Err(Error::Length));
                }
            }
            wire.push(0);
            assert_eq!(Message::parse(&wire), Err(Error::Length));
        }
    }
    assert_eq!(Message::parse(&[3, 0]), Err(Error::MessageType(3)));
    assert_eq!(Message::parse(&[0, 2]), Err(Error::AddressType(2)));
    let addr = SocketAddrV6::new("fe80::1".parse().unwrap(), 6881, 0, 1);
    assert!(
        Message {
            kind: Kind::Connect,
            addr: addr.into()
        }
        .encode()
        .is_err()
    );
}

#[test]
fn pex_flags_do_not_change_address_alignment() {
    let v4 = [192, 0, 2, 1, 0x1a, 0xe1, 192, 0, 2, 2, 0x1a, 0xe2];
    let v6 = [0u8; 36];
    for (flags, expected) in [
        (None, None),
        (
            Some(&[pex::UTP | pex::HOLEPUNCH, 0][..]),
            Some(&[0x0c, 0][..]),
        ),
        (Some(&[0x0c][..]), None),
        (Some(&[0x0c, 0, 0][..]), None),
    ] {
        let mut fields = BTreeMap::new();
        fields.insert(b"added".as_slice(), Node::new(Value::Bytes(&v4)));
        fields.insert(b"added6".as_slice(), Node::new(Value::Bytes(&v6)));
        if let Some(flags) = flags {
            fields.insert(b"added.f".as_slice(), Node::new(Value::Bytes(flags)));
            fields.insert(b"added6.f".as_slice(), Node::new(Value::Bytes(flags)));
        }
        let wire =
            bencode::encode(&Node::new(Value::Dictionary(fields)), Limits::default()).unwrap();
        let message = pex::Message::parse(&wire, Limits::default(), 4, 0).unwrap();
        assert_eq!(message.added_v4_flags(), expected);
        assert_eq!(message.added_v6_flags(), expected);
        let addresses: Vec<_> = message.added_v4().iter().collect();
        assert_eq!(
            addresses,
            vec![
                "192.0.2.1:6881".parse().unwrap(),
                "192.0.2.2:6882".parse().unwrap()
            ]
        );
        assert_eq!(message.added_v6().len(), 2);
        assert_eq!(message.encode(Limits::default()).unwrap(), wire);
    }
}
