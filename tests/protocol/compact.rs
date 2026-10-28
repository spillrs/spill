use spill::protocol::compact::*;

#[test]
fn compact_endpoints_roundtrip_borrowing_tails_and_limits() {
    let v4 = [192, 0, 2, 1, 0x1a, 0xe1];
    let v6 = [0; 18];
    let address = decode_v4(&v4).unwrap();
    assert_eq!(address.to_string(), "192.0.2.1:6881");
    assert_eq!(encode_v4(address), v4);
    assert_eq!(encode_v6(decode_v6(&v6).unwrap()).unwrap(), v6);
    for length in 0..=36 {
        assert_eq!(decode_v4(&vec![0; length]).is_ok(), length == 6);
        assert_eq!(decode_v6(&vec![0; length]).is_ok(), length == 18);
    }
    for tail in 0..18 {
        let wire = [v6.repeat(2), vec![0xff; tail]].concat();
        let peers = PeersV6::parse(&wire, 2).unwrap();
        assert_eq!(peers.iter().count(), 2);
        assert_eq!(peers.remainder().as_ptr(), wire[36..].as_ptr());
        assert_eq!(peers.remainder(), vec![0xff; tail]);
        assert!(PeersV6::parse(&wire, 1).is_err());
    }
    for tail in 0..6 {
        let wire = [v4.repeat(2), vec![0xff; tail]].concat();
        let peers = PeersV4::parse(&wire, 2).unwrap();
        assert_eq!(peers.iter().collect::<Vec<_>>(), [address; 2]);
        assert_eq!(peers.remainder(), vec![0xff; tail]);
        assert!(PeersV4::parse(&wire, 1).is_err());
    }
    let scoped = "[fe80::1%2]:6881".parse().unwrap();
    assert!(encode_v6(scoped).is_err());
}
