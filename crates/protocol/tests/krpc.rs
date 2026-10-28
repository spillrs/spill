//! BEP 32 family handling is separate from the common BEP 5 round-trips.
use protocol::{
    bencode::{self, Node, Value},
    dht::{
        NodeId,
        krpc::{Body, Error, Kind, Message, NodeV6, NodesV6, Query, Want},
    },
};
use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
};
const ID: &[u8; 20] = b"abcdefghij0123456789";
const TARGET: &[u8; 20] = b"mnopqrstuvwxyz123456";
fn bytes(value: &[u8]) -> Node<'_> {
    Node::new(Value::Bytes(value))
}
fn integer(value: i64) -> Node<'static> {
    Node::new(Value::Integer(value))
}
fn dict<'a>(entries: &[(&'a [u8], Node<'a>)]) -> Node<'a> {
    Node::new(Value::Dictionary(
        entries.iter().cloned().collect::<BTreeMap<_, _>>(),
    ))
}
fn parse(input: &[u8]) -> Result<Message<'_>, Error> {
    Message::parse(input, bencode::Limits::default(), 8, 16)
}
fn request<'a>(method: &'a [u8], args: Node<'a>) -> Result<Message<'a>, Error> {
    Message::new(
        b"aa",
        Body::Query {
            method,
            arguments: args,
        },
        None,
        8,
        16,
    )
}
fn response<'a>(
    fields: &[(&'a [u8], Node<'a>)],
    nodes: usize,
    peers: usize,
) -> Result<Message<'a>, Error> {
    Message::new(b"aa", Body::Response(dict(fields)), None, nodes, peers)
}

#[test]
fn bep32_want_is_explicit_extensible_and_validated_only_for_supported_queries() {
    for (method, key) in [
        (b"find_node".as_slice(), b"target".as_slice()),
        (b"get_peers", b"info_hash"),
    ] {
        for (hints, expected) in [
            (
                vec![b"n4".as_slice()],
                Want {
                    ipv4: true,
                    ipv6: false,
                },
            ),
            (
                vec![b"n6".as_slice()],
                Want {
                    ipv4: false,
                    ipv6: true,
                },
            ),
            (
                vec![b"n6".as_slice(), b"future", b"n4", b"n6"],
                Want {
                    ipv4: true,
                    ipv6: true,
                },
            ),
            (
                vec![b"N4".as_slice(), b"", b"\0\xff"],
                Want {
                    ipv4: false,
                    ipv6: false,
                },
            ),
        ] {
            let hints = Node::new(Value::List(hints.into_iter().map(bytes).collect()));
            let message = request(
                method,
                dict(&[
                    (b"id", bytes(ID)),
                    (key, bytes(TARGET)),
                    (b"want", hints.clone()),
                ]),
            )
            .unwrap();
            let wire = message.encode(bencode::Limits::default()).unwrap();
            let parsed = parse(&wire).unwrap();
            let Kind::Query {
                query: Query::FindNode { want, .. } | Query::GetPeers { want, .. },
                ..
            } = parsed.kind()
            else {
                panic!("query")
            };
            assert_eq!(want, Some(expected));
            assert_eq!(
                parsed.document().get(b"a").unwrap().get(b"want"),
                Some(&hints)
            );
        }
        for invalid in [
            integer(0),
            bytes(b"n4"),
            Node::new(Value::List(vec![])),
            Node::new(Value::List(vec![bytes(b"n4"), integer(6)])),
        ] {
            assert_eq!(
                request(
                    method,
                    dict(&[(b"id", bytes(ID)), (key, bytes(TARGET)), (b"want", invalid)])
                )
                .unwrap_err(),
                Error::InvalidField("want")
            );
        }
    }
    // A familiar field on another method is not an advertised capability.
    for method in [b"ping".as_slice(), b"future"] {
        assert!(request(method, dict(&[(b"id", bytes(ID)), (b"want", integer(0))])).is_ok());
    }
    let wire = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:wanti0e4:wantl2:n6ee1:q9:find_node1:t0:1:y1:qe";
    assert!(matches!(
        parse(wire).unwrap().kind(),
        Kind::Query {
            query: Query::FindNode {
                want: Some(Want {
                    ipv4: false,
                    ipv6: true
                }),
                ..
            },
            ..
        }
    ));
}
#[test]
fn bep32_nodes6_preserve_bits_tails_and_share_the_node_budget() {
    let expected = NodeV6 {
        id: NodeId(*ID),
        address: SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc000, 0x201), 0, 0, 0),
    };
    // Independent compact fixture: mapped IPv6 stays IPv6, port zero survives.
    let mut record = ID.to_vec();
    record.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 192, 0, 2, 1, 0, 0]);
    assert_eq!(expected.encode().unwrap().as_slice(), record);
    for tail in 0..38 {
        let mut raw = record.repeat(2);
        raw.extend(std::iter::repeat_n(255, tail));
        let view = NodesV6::parse(&raw, 2).unwrap();
        assert_eq!(view.as_bytes().as_ptr(), raw.as_ptr());
        assert_eq!(view.iter().collect::<Vec<_>>(), [expected; 2]);
        assert_eq!(view.iter().next_back(), Some(expected));
        assert_eq!(view.remainder(), &raw[76..]);
        assert_eq!(NodesV6::parse(&raw, 1).unwrap_err(), Error::NodeLimit);
        let fields = [
            (b"id".as_slice(), bytes(ID)),
            (b"nodes", bytes(&[0; 26])),
            (b"nodes6", bytes(&raw)),
        ];
        assert_eq!(response(&fields, 2, 0).unwrap_err(), Error::NodeLimit);
        let message = response(&fields, 3, 0).unwrap();
        let wire = message.encode(bencode::Limits::default()).unwrap();
        let parsed = Message::parse(&wire, bencode::Limits::default(), 3, 0).unwrap();
        let Kind::Response {
            nodes: Some(v4),
            nodes6: Some(v6),
            ..
        } = parsed.kind()
        else {
            panic!("both families")
        };
        assert_eq!(v4.iter().len(), 1);
        assert_eq!(v6.as_bytes(), raw);
    }
    assert_eq!(NodesV6::parse(&record[..37], 0).unwrap().iter().len(), 0);
    let mut scoped = expected;
    scoped.address.set_scope_id(1);
    assert!(scoped.encode().is_err());
    scoped.address.set_scope_id(0);
    scoped.address.set_flowinfo(1);
    assert!(scoped.encode().is_err());
}
#[test]
fn bep32_hybrid_values_are_exact_ordered_and_bounded_not_filtered_by_want() {
    let v4 = [192, 0, 2, 1, 0x1a, 0xe1];
    let v6 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 192, 0, 2, 1, 0, 0];
    let fields = [
        (b"id".as_slice(), bytes(ID)),
        (b"nodes6", bytes(&[0; 38])),
        (
            b"values",
            Node::new(Value::List(vec![bytes(&v6), bytes(&v4), bytes(&v6)])),
        ),
    ];
    assert_eq!(response(&fields, 1, 2).unwrap_err(), Error::PeerLimit);
    let message = response(&fields, 1, 3).unwrap();
    let wire = message.encode(bencode::Limits::default()).unwrap();
    let parsed = parse(&wire).unwrap();
    let peers = parsed.peers().unwrap().collect::<Vec<_>>();
    assert!(
        matches!(peers[0], SocketAddr::V6(addr) if addr.ip().to_ipv4_mapped() == Some(Ipv4Addr::new(192, 0, 2, 1)) && addr.port() == 0)
    );
    assert_eq!(
        peers[1],
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 6881))
    );
    assert_eq!(peers[0], peers[2]);
    assert_eq!(parsed.peers().unwrap().next_back(), Some(peers[2]));
    for len in 0..=38 {
        let data = vec![0; len];
        let result = response(
            &[
                (b"id", bytes(ID)),
                (b"values", Node::new(Value::List(vec![bytes(&data)]))),
            ],
            0,
            1,
        );
        assert_eq!(result.is_ok(), matches!(len, 6 | 18), "length {len}");
    }
}
