//! Small independent vectors, round-trips and malformed-input checks per codec.
use protocol::bencode::{self, Limits};
use protocol::identity::InfoHashV1;

#[test]
fn handshake_wire_and_truncations() {
    use protocol::peer::{HANDSHAKE_LEN, Handshake};
    let handshake = Handshake {
        reserved: [0xff; 8],
        info_hash: [1; 20],
        peer_id: [2; 20],
    };
    let wire = handshake.encode();
    assert_eq!(&wire[..20], b"\x13BitTorrent protocol");
    assert_eq!(Handshake::decode(&wire), Ok((handshake, HANDSHAKE_LEN)));
    for end in 0..wire.len() {
        assert!(Handshake::decode(&wire[..end]).is_err());
    }
    for offset in 0..20 {
        let mut bad = wire;
        bad[offset] ^= 1;
        assert!(Handshake::decode(&bad).is_err());
    }
}

#[test]
fn peer_frames_roundtrip_bounds_and_truncations() {
    use protocol::peer::message::{BlockRequest, Message};
    let request = BlockRequest {
        index: 1,
        begin: 2,
        length: 3,
    };
    let messages = [
        Message::KeepAlive,
        Message::Choke,
        Message::Unchoke,
        Message::Interested,
        Message::NotInterested,
        Message::Have(7),
        Message::Bitfield(&[0x80]),
        Message::Request(request),
        Message::Cancel(request),
        Message::Piece {
            index: 1,
            begin: 2,
            block: b"\0\xff",
        },
        Message::Port(6881),
        Message::SuggestPiece(2),
        Message::HaveAll,
        Message::HaveNone,
        Message::Reject(request),
        Message::AllowedFast(2),
        Message::Extended {
            id: 7,
            payload: b"de",
        },
        Message::Unknown {
            id: 255,
            payload: b"\xff",
        },
    ];
    for message in messages {
        let mut wire = vec![0; message.encoded_len(1024).unwrap()];
        let len = message.encode(&mut wire, 1024).unwrap();
        assert_eq!(Message::decode(&wire, 1024), Ok((message, len)));
        for end in 0..len {
            assert!(Message::decode(&wire[..end], 1024).is_err());
        }
        let mut short = vec![0x55; len - 1];
        assert!(message.encode(&mut short, 1024).is_err());
        assert!(short.iter().all(|&b| b == 0x55));
        if len > 4 {
            assert!(Message::decode(&wire, (len - 5) as u32).is_err());
        }
    }
    assert_eq!(
        Message::decode(b"\0\0\0\x01\x02", 10),
        Ok((Message::Interested, 5))
    );
    for wire in [b"\0\0\0\x02\x00".as_slice(), b"\xff\xff\xff\xff"] {
        assert!(Message::decode(wire, 1024).is_err());
    }
    assert!(
        Message::Unknown {
            id: 0,
            payload: b""
        }
        .encoded_len(1024)
        .is_err()
    );
}

#[test]
fn block_and_bitfield_layout_boundaries() {
    use protocol::{
        metainfo::Metainfo,
        peer::message::{BlockRequest, Message},
    };
    let wire = b"d4:infod6:lengthi5e4:name1:x12:piece lengthi4e6:pieces40:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaee";
    let meta = Metainfo::parse(wire, Limits::default()).unwrap();
    for (index, begin, length, valid) in [
        (0, 0, 4, true),
        (1, 0, 1, true),
        (1, 1, 1, false),
        (2, 0, 1, false),
        (0, u32::MAX, 1, false),
        (0, 0, 0, false),
        (0, 0, 5, false),
    ] {
        assert_eq!(
            BlockRequest {
                index,
                begin,
                length
            }
            .validate(meta.layout(), 4)
            .is_ok(),
            valid
        );
    }
    assert!(
        Message::Bitfield(&[0xc0])
            .validate_layout(meta.layout(), 4)
            .is_ok()
    );
    assert!(
        Message::Bitfield(&[0x81])
            .validate_layout(meta.layout(), 4)
            .is_err()
    );
    assert!(
        Message::Bitfield(&[])
            .validate_layout(meta.layout(), 4)
            .is_err()
    );
}

#[test]
fn owned_dht_requests_roundtrip() {
    use protocol::dht::{NodeId, OwnedQuery, krpc::Message};
    let hash = InfoHashV1::from_bytes([2; 20]);
    for query in [
        OwnedQuery::Ping,
        OwnedQuery::FindNode(NodeId([3; 20])),
        OwnedQuery::GetPeers(hash),
        OwnedQuery::AnnouncePeer {
            hash,
            port: 6881,
            token: vec![4],
        },
    ] {
        let wire = query
            .encode(b"tx", NodeId([1; 20]), Limits::default(), 8, 100)
            .unwrap();
        let message = Message::parse(&wire, Limits::default(), 8, 100).unwrap();
        assert_eq!(message.transaction_id(), b"tx");
        assert!(message.document().get(b"ro").is_some());
        assert_eq!(message.encode(Limits::default()).unwrap(), wire);
    }
}

#[test]
fn metainfo_original_identity_and_invalid_shapes() {
    use protocol::metainfo::{Metainfo, OwnedMetainfo};
    let wire = b"d4:infod6:lengthi0e4:name01:x12:piece lengthi1e6:pieces0:ee";
    let meta = Metainfo::parse(wire, Limits::default()).unwrap();
    assert_eq!(meta.info_bytes().as_ptr(), wire[7..].as_ptr());
    assert_eq!(
        meta.info_hash(),
        InfoHashV1::from_info_bytes(meta.info_bytes())
    );
    let owned = OwnedMetainfo::parse(wire, Limits::default()).unwrap();
    assert_eq!(owned.source_bytes(), wire);
    assert_eq!(owned.info_hash(), meta.info_hash());
    let canonical =
        bencode::encode(meta.document().get(b"info").unwrap(), Limits::default()).unwrap();
    assert_ne!(InfoHashV1::from_info_bytes(&canonical), meta.info_hash());
    for end in 0..wire.len() {
        assert!(Metainfo::parse(&wire[..end], Limits::default()).is_err());
    }
    for bad in [
        b"de".as_slice(),
        b"d4:infoi1ee",
        b"d4:infod6:lengthi-1e4:name1:x12:piece lengthi0e6:pieces0:ee",
    ] {
        assert!(Metainfo::parse(bad, Limits::default()).is_err());
    }
}

#[test]
fn magnet_decoding_and_limits() {
    use protocol::magnet::{Limits, Magnet};
    let uri = "magnet:?xt=urn:btih:000102030405060708090a0b0c0d0e0f10111213&dn=a+b%00%ff&tr=udp%3A%2F%2Fx%3A80";
    let magnet = Magnet::parse(uri, Limits::default()).unwrap();
    assert_eq!(
        magnet.info_hash(),
        InfoHashV1::from_bytes(std::array::from_fn(|i| i as u8))
    );
    assert_eq!(magnet.display_name(), Some(b"a b\0\xff".as_slice()));
    assert_eq!(magnet.trackers().collect::<Vec<_>>(), [b"udp://x:80"]);
    for bad in [
        "magnet:?xt=urn:btih:bad",
        "http://x",
        "magnet:?xt=urn:btih:%zz",
    ] {
        assert!(Magnet::parse(bad, Limits::default()).is_err());
    }
    assert!(
        Magnet::parse(
            uri,
            Limits {
                max_parameters: 1,
                ..Limits::default()
            }
        )
        .is_err()
    );
    assert!(
        Magnet::parse(
            uri,
            Limits {
                max_input_bytes: uri.len() - 1,
                ..Limits::default()
            }
        )
        .is_err()
    );
}

#[test]
fn extension_updates_are_directional_and_atomic() {
    use protocol::peer::extension::{ExtensionMap, Handshake};
    let parse = |wire| Handshake::parse(wire, Limits::default()).unwrap();
    let mut remote = ExtensionMap::new(&[(b"ut_metadata", 0), (b"ut_pex", 0)]).unwrap();
    let wire = b"d1:md11:ut_metadatai7e6:ut_pexi9eee";
    let handshake = parse(wire);
    remote.apply(&handshake).unwrap();
    assert_eq!(remote.id_for(b"ut_metadata"), Some(7));
    let encoded = handshake.encode(Limits::default()).unwrap();
    assert_eq!(parse(&encoded).document(), handshake.document());
    let before = remote.clone();
    assert!(remote.apply(&parse(b"d1:md6:ut_pexi7eee")).is_err());
    assert_eq!(remote, before);
    remote.apply(&parse(b"d1:md6:ut_pexi0eee")).unwrap();
    assert_eq!(remote.id_for(b"ut_pex"), None);
    assert_eq!(remote.id_for(b"ut_metadata"), Some(7));
    for end in 0..wire.len() {
        assert!(Handshake::parse(&wire[..end], Limits::default()).is_err());
    }
}

#[test]
fn pex_family_caps_and_roundtrip() {
    use protocol::peer::pex::Message;
    let wire = b"d5:added6:\x7f\0\0\x01\x1a\xe1e";
    let message = Message::parse(wire, Limits::default(), 1, 1).unwrap();
    assert_eq!(message.encode(Limits::default()).unwrap(), wire);
    assert!(Message::parse(wire, Limits::default(), 0, 1).is_err());
    for end in 0..wire.len() {
        assert!(Message::parse(&wire[..end], Limits::default(), 1, 1).is_err());
    }
    assert!(Message::parse(b"i1e", Limits::default(), 1, 1).is_err());
}

#[test]
fn krpc_queries_replies_and_errors_roundtrip() {
    use protocol::dht::krpc::Message;
    let cases: &[&[u8]] = &[
        b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
        b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe",
        b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe",
        b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe",
        b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re",
        b"d1:eli201e5:errore1:t2:aa1:y1:ee",
    ];
    for &wire in cases {
        let message = Message::parse(wire, Limits::default(), 8, 16).unwrap();
        assert_eq!(message.transaction_id(), b"aa");
        assert_eq!(message.encode(Limits::default()).unwrap(), wire);
        for end in 0..wire.len() {
            assert!(Message::parse(&wire[..end], Limits::default(), 8, 16).is_err());
        }
    }
    for bad in [
        b"de".as_slice(),
        b"d1:ad2:id1:xe1:q4:ping1:t2:aa1:y1:qe",
        b"d1:rd2:id20:abcdefghij01234567895:nodesi1ee1:t2:aa1:y1:re",
    ] {
        assert!(Message::parse(bad, Limits::default(), 8, 16).is_err());
    }
}

#[test]
fn http_tracker_reply_and_query_bounds() {
    use protocol::tracker::http::{AnnounceRequest, AnnounceResponse};
    let wire = b"d8:intervali1800e5:peers6:\x7f\0\0\x01\x1a\xe1e";
    let response = AnnounceResponse::parse(wire, Limits::default(), 1).unwrap();
    assert_eq!(
        bencode::encode(response.document(), Limits::default()).unwrap(),
        wire
    );
    assert_eq!(response.peers_v4().iter().next().unwrap().port(), 6881);
    assert!(AnnounceResponse::parse(wire, Limits::default(), 0).is_err());
    for end in 0..wire.len() {
        assert!(AnnounceResponse::parse(&wire[..end], Limits::default(), 1).is_err());
    }
    assert!(AnnounceResponse::parse(b"i1e", Limits::default(), 1).is_err());
    let warning = AnnounceResponse::parse(
        b"d8:intervali30e12:min intervali120e5:peers0:10:tracker id3:tag15:warning message9:Low ratioe",
        Limits::default(), 1,
    ).unwrap();
    assert_eq!(warning.warning_message(), Some(b"Low ratio".as_slice()));
    assert_eq!(warning.tracker_id(), Some(b"tag".as_slice()));
    assert_eq!(warning.min_interval_secs(), Some(120));
    let request = AnnounceRequest {
        info_hash: InfoHashV1::from_bytes([0; 20]),
        peer_id: [255; 20],
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: 3,
        event: None,
        numwant: None,
        key: None,
        tracker_id: None,
    };
    let query = request.encode_query(Some("pass=a%2fb"), 4096).unwrap();
    assert!(query.starts_with("pass=a%2fb&info_hash=%00"));
    assert!(query.contains("peer_id=%FF%FF"));
    assert!(request.encode_query(None, 1).is_err());
    assert!(request.encode_query(Some("%69nfo_hash=x"), 4096).is_err());
}

#[test]
fn udp_tracker_roundtrip_and_malformed_datagrams() {
    use protocol::tracker::udp::{self, Family, Request, RequestKind, Response};
    let limits = udp::Limits {
        max_packet_len: 4096,
        max_peers: 4,
    };
    let connect = Request {
        connection_id: udp::PROTOCOL_ID,
        transaction_id: 7,
        kind: RequestKind::Connect { extensions: b"" },
    };
    let wire = connect.encode(limits).unwrap();
    assert_eq!(&wire[..8], &udp::PROTOCOL_ID.to_be_bytes());
    assert_eq!(
        Request::decode(&wire, limits)
            .unwrap()
            .encode(limits)
            .unwrap(),
        wire
    );
    for end in 0..16 {
        assert!(Request::decode(&wire[..end], limits).is_err());
    }
    let mut bad = wire;
    bad[0] ^= 1;
    assert!(Request::decode(&bad, limits).is_err());
    let announce = udp::Announce {
        info_hash: InfoHashV1::from_bytes([1; 20]),
        peer_id: [2; 20],
        downloaded: 3,
        left: 4,
        uploaded: 5,
        event: udp::EVENT_STARTED,
        ip: std::net::Ipv4Addr::UNSPECIFIED,
        key: 6,
        num_want: -1,
        port: 6881,
    };
    let wire = Request {
        connection_id: 1,
        transaction_id: 2,
        kind: RequestKind::Announce {
            announce,
            extensions: b"tail",
        },
    }
    .encode(limits)
    .unwrap();
    assert_eq!(
        Request::decode(&wire, limits)
            .unwrap()
            .encode(limits)
            .unwrap(),
        wire
    );
    for end in 0..98 {
        assert!(Request::decode(&wire[..end], limits).is_err());
    }
    for wire in [
        b"\0\0\0\0\0\0\0\x07\0\0\0\0\0\0\0\x01".as_slice(),
        b"\0\0\0\x03\0\0\0\x07error",
    ] {
        let response = Response::decode(wire, Family::V4, limits).unwrap();
        assert_eq!(response.encode(limits).unwrap(), wire);
        for end in 0..8 {
            assert!(Response::decode(&wire[..end], Family::V4, limits).is_err());
        }
    }
}
