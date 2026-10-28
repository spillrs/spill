use spill::protocol::nat_pmp::{self, Reply};
use std::net::Ipv4Addr;

#[test]
fn requests_and_replies_match_rfc_6886() {
    assert_eq!(nat_pmp::address_request(), [0, 0]);
    for opcode in [1, 2] {
        assert_eq!(
            nat_pmp::mapping_request(opcode, 6881, 54321, 7200),
            [0, opcode, 0, 0, 0x1a, 0xe1, 0xd4, 0x31, 0, 0, 0x1c, 0x20]
        );
        assert_eq!(
            nat_pmp::mapping_request(opcode, 6881, 54321, 0),
            [0, opcode, 0, 0, 0x1a, 0xe1, 0xd4, 0x31, 0, 0, 0, 0]
        );
        assert!(matches!(
            nat_pmp::reply(
                &[
                    0,
                    opcode + 128,
                    0,
                    0,
                    0,
                    0,
                    0,
                    42,
                    0x1a,
                    0xe1,
                    0xd4,
                    0x31,
                    0,
                    0,
                    0x1c,
                    0x20
                ],
                opcode
            ),
            Some(Reply::Mapping {
                internal: 6881,
                external: 54321,
                lifetime: 7200
            })
        ));
    }
    assert!(
        matches!(nat_pmp::reply(&[0, 128, 0, 0, 0, 0, 0, 42, 203, 0, 113, 7], 0),
        Some(Reply::Address(ip)) if ip == Ipv4Addr::new(203, 0, 113, 7))
    );
    assert!(matches!(
        nat_pmp::reply(&[0, 130, 0, 2, 0, 0, 0, 42], 2),
        Some(Reply::Refused(2))
    ));
}

#[test]
fn truncated_wrong_version_and_unrelated_replies_are_ignored() {
    let reply = [
        0, 130, 0, 0, 0, 0, 0, 42, 0x1a, 0xe1, 0xd4, 0x31, 0, 0, 0x1c, 0x20,
    ];
    for n in 0..reply.len() {
        assert!(nat_pmp::reply(&reply[..n], 2).is_none());
    }
    assert!(nat_pmp::reply(&reply, 1).is_none());
    let mut wrong = reply;
    wrong[0] = 2;
    assert!(nat_pmp::reply(&wrong, 2).is_none());
    let mut trailing = reply.to_vec();
    trailing.push(0);
    assert!(nat_pmp::reply(&trailing, 2).is_none());
}
