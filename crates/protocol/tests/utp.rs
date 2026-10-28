use protocol::utp::{
    Error, HEADER_LEN, Header, Kind, MAX_EXTENSIONS, Packet, seq_diff, seq_less, timestamp_diff,
};

const SYN: [u8; 20] = [
    0x41, 0, 0x12, 0x34, 0x01, 0x02, 0x03, 0x04, 0, 0, 0, 0, 0, 4, 0, 0, 0xff, 0xff, 0, 0,
];
const STATE_SACK: [u8; 26] = [
    0x21, 1, 0x12, 0x34, 0x01, 0x02, 0x03, 0x05, 0, 0, 0, 1, 0, 4, 0, 0, 0, 7, 0xff, 0xff, 0, 4,
    0x81, 0x01, 0, 0,
];

#[test]
fn captured_transmission_syn_and_sack() {
    // Transmission 4.1.3 UDP payloads from isolated netem interop, retaining
    // libutp's real clock offset and selective-ACK layout.
    let syn = [
        0x41, 0x00, 0x98, 0x69, 0x58, 0xa8, 0x94, 0xf4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
        0x00, 0x48, 0x73, 0x00, 0x00,
    ];
    let state = [
        0x21, 0x01, 0x9d, 0xf1, 0x56, 0xab, 0x20, 0x73, 0x56, 0xa2, 0x38, 0xe6, 0x00, 0x04, 0x00,
        0x00, 0x45, 0x68, 0x34, 0xee, 0x00, 0x04, 0x02, 0x00, 0x00, 0x00,
    ];
    for (wire, header, sack) in [
        (
            syn.as_slice(),
            Header {
                kind: Kind::Syn,
                connection_id: 39017,
                timestamp_us: 1487443188,
                timestamp_diff_us: 0,
                wnd_size: 262144,
                seq_nr: 18547,
                ack_nr: 0,
            },
            None,
        ),
        (
            state.as_slice(),
            Header {
                kind: Kind::State,
                connection_id: 40433,
                timestamp_us: 1454055539,
                timestamp_diff_us: 1453471974,
                wnd_size: 262144,
                seq_nr: 17768,
                ack_nr: 13550,
            },
            Some([2, 0, 0, 0].as_slice()),
        ),
    ] {
        let packet = Packet::decode(wire).unwrap();
        assert_eq!(
            packet,
            Packet {
                header,
                sack,
                payload: &[]
            }
        );
        let mut encoded = Vec::new();
        packet.encode(&mut encoded);
        assert_eq!(encoded, wire);
    }
}

#[test]
fn golden_syn() {
    let packet = Packet::decode(&SYN).unwrap();
    assert_eq!(
        packet.header,
        Header {
            kind: Kind::Syn,
            connection_id: 0x1234,
            timestamp_us: 0x01020304,
            timestamp_diff_us: 0,
            wnd_size: 256 * 1024,
            seq_nr: 0xffff,
            ack_nr: 0,
        }
    );
    assert_eq!(packet.sack, None);
    assert!(packet.payload.is_empty());
    let mut bytes = Vec::new();
    packet.encode(&mut bytes);
    assert_eq!(bytes, SYN);
}

#[test]
fn golden_state_with_sack() {
    let packet = Packet::decode(&STATE_SACK).unwrap();
    assert_eq!(packet.header.kind, Kind::State);
    assert_eq!(packet.header.connection_id, 0x1234);
    assert_eq!(packet.header.timestamp_us, 0x01020305);
    assert_eq!(packet.header.timestamp_diff_us, 1);
    assert_eq!(packet.header.wnd_size, 256 * 1024);
    assert_eq!(packet.header.seq_nr, 7);
    assert_eq!(packet.header.ack_nr, 0xffff);
    assert_eq!(packet.sack, Some([0x81, 1, 0, 0].as_slice()));
    assert!(packet.payload.is_empty());
    let mut bytes = Vec::new();
    packet.encode(&mut bytes);
    assert_eq!(bytes, STATE_SACK);
}

#[test]
fn close_reason_and_unknown_extensions_are_skipped() {
    let mut bytes = SYN.to_vec();
    bytes[0] = 0x11;
    bytes[1] = 2;
    bytes.extend_from_slice(&[99, 4, 0, 0, 0, 3, 1, 0, 0, 4, 1, 0, 0, 0]);
    bytes.extend_from_slice(b"payload");
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.header.kind, Kind::Fin);
    assert_eq!(packet.sack, Some([1, 0, 0, 0].as_slice()));
    assert_eq!(packet.payload, b"payload");
}

#[test]
fn round_trips_append_to_caller_buffer() {
    for kind in [Kind::Data, Kind::Fin, Kind::State, Kind::Reset, Kind::Syn] {
        for sack in [None, Some([0xff, 0, 0, 0].as_slice())] {
            for payload in [b"".as_slice(), b"data", &[0xff; 1200]] {
                let packet = Packet {
                    header: Header {
                        kind,
                        connection_id: u16::MAX,
                        timestamp_us: u32::MAX,
                        timestamp_diff_us: u32::MAX - 1,
                        wnd_size: 0,
                        seq_nr: 0,
                        ack_nr: u16::MAX,
                    },
                    sack,
                    payload,
                };
                let mut bytes = vec![42];
                packet.encode(&mut bytes);
                assert_eq!(bytes[0], 42);
                assert_eq!(Packet::decode(&bytes[1..]).unwrap(), packet);
            }
        }
    }
}

#[test]
fn truncated_header_and_extension_chain() {
    for length in 0..HEADER_LEN {
        assert_eq!(Packet::decode(&SYN[..length]), Err(Error::Truncated));
    }
    for length in HEADER_LEN..STATE_SACK.len() {
        assert_eq!(Packet::decode(&STATE_SACK[..length]), Err(Error::Truncated));
    }
    let mut bytes = STATE_SACK;
    bytes[20] = 2;
    assert_eq!(Packet::decode(&bytes), Err(Error::Truncated));
    bytes[20] = 0;
    bytes[21] = 255;
    assert_eq!(Packet::decode(&bytes), Err(Error::Truncated));
}

#[test]
fn rejects_versions_and_types() {
    for version in 0..16 {
        let mut bytes = SYN;
        bytes[0] = 0x40 | version;
        if version != 1 {
            assert_eq!(Packet::decode(&bytes), Err(Error::Version));
        }
    }
    for kind in 5..16 {
        let mut bytes = SYN;
        bytes[0] = kind << 4 | 1;
        assert_eq!(Packet::decode(&bytes), Err(Error::Kind));
    }
}

#[test]
fn rejects_invalid_sack_lengths_and_duplicates() {
    for length in 0..=255u8 {
        let mut bytes = SYN.to_vec();
        bytes[1] = 1;
        bytes.extend_from_slice(&[0, length]);
        bytes.resize(HEADER_LEN + 2 + usize::from(length), 0);
        if length == 0 || !length.is_multiple_of(4) {
            assert_eq!(Packet::decode(&bytes), Err(Error::Sack));
        } else {
            assert_eq!(
                Packet::decode(&bytes).unwrap().sack.unwrap().len(),
                usize::from(length)
            );
        }
    }
    let mut bytes = STATE_SACK.to_vec();
    bytes[20] = 1;
    bytes.extend_from_slice(&[0, 4, 0, 0, 0, 0]);
    assert_eq!(Packet::decode(&bytes), Err(Error::Sack));
}

#[test]
fn bounds_extension_count_including_empty_unknown_extensions() {
    let mut bytes = SYN.to_vec();
    bytes[1] = 2;
    for _ in 0..MAX_EXTENSIONS - 1 {
        bytes.extend_from_slice(&[2, 0]);
    }
    bytes.extend_from_slice(&[0, 0]);
    assert!(Packet::decode(&bytes).is_ok());
    bytes[HEADER_LEN + 2 * (MAX_EXTENSIONS - 1)] = 2;
    bytes.extend_from_slice(&[0, 0]);
    assert_eq!(Packet::decode(&bytes), Err(Error::ExtensionLimit));
}

#[test]
fn sequence_and_timestamp_wrap() {
    assert!(seq_less(0xffff, 0));
    assert!(!seq_less(0, 0xffff));
    assert!(!seq_less(0, 0));
    assert!(seq_less(12, 13));
    assert_eq!(seq_diff(0, 0xffff), 1);
    assert_eq!(seq_diff(0xffff, 0), -1);
    assert_eq!(seq_diff(10, 8), 2);
    assert_eq!(timestamp_diff(7, u32::MAX - 2), 10);
    assert_eq!(timestamp_diff(10, 10), 0);
}
