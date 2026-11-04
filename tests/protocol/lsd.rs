use spill::protocol::lsd::{self, Announce};

#[test]
fn golden_multiple_hashes_and_round_trip() {
    let packet = b"BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 6881\r\nInfohash: 0123456789abcdef0123456789abcdef01234567\r\nInfohash: FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\r\ncookie: abc\r\nX-Unknown: ignored\r\n\r\n\r\n";
    let mut first = [0; 20];
    first.copy_from_slice(&[
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
        0xef, 0x01, 0x23, 0x45, 0x67,
    ]);
    let expected = Announce {
        port: 6881,
        hashes: vec![first, [255; 20]],
        cookie: Some("abc"),
    };
    assert_eq!(lsd::decode(packet), Some(expected));
    for ipv6 in [false, true] {
        let encoded = lsd::encode(ipv6, 6881, &[first, [255; 20]], "abc");
        assert_eq!(
            lsd::decode(&encoded).unwrap().hashes,
            vec![first, [255; 20]]
        );
    }
    let without_cookie = String::from_utf8(packet.to_vec())
        .unwrap()
        .replace("cookie: abc\r\n", "");
    assert_eq!(lsd::decode(without_cookie.as_bytes()).unwrap().cookie, None);
}

#[test]
fn malformed_announcements_are_rejected() {
    let valid = String::from_utf8(lsd::encode(false, 6881, &[[0; 20]], "a")).unwrap();
    for (from, to) in [
        ("BT-SEARCH", "SEARCH"),
        ("239.192.152.143:6771", "239.1.2.3:6771"),
        ("Port: 6881", "Port: 0"),
        ("Port: 6881", "Port: 65536"),
        ("Port: 6881", "Port: +6881"),
        ("Port: 6881\r\n", ""),
        ("Port: 6881", "Port: 6881\r\nPort: 6881"),
        ("Host:", "Unknown:"),
        ("Infohash:", "Unknown:"),
        (
            "0000000000000000000000000000000000000000",
            "z000000000000000000000000000000000000000",
        ),
        ("0000000000000000000000000000000000000000", "0000"),
        (
            "0000000000000000000000000000000000000000",
            "+100000000000000000000000000000000000000",
        ),
        ("cookie: a", "\r\ncookie: a"),
        ("cookie: a", "cookie: a\r\ncookie: b"),
        ("\r\n", "\n"),
    ] {
        assert!(
            lsd::decode(valid.replace(from, to).as_bytes()).is_none(),
            "{from} -> {to}"
        );
    }
    assert!(lsd::decode(&vec![0; lsd::MAX_PACKET + 1]).is_none());
}
