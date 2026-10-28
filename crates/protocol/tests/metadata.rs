use protocol::{
    bencode::Limits,
    peer::metadata::{Error, Kind, Message, PIECE_LENGTH, piece_range},
};

const MAX_SIZE: u32 = 64 * 1024 * 1024;
fn parse(input: &[u8]) -> Result<Message<'_>, Error> {
    Message::parse(input, Limits::default(), MAX_SIZE)
}

#[test]
fn piece_ranges_and_exact_final_lengths() {
    assert_eq!(PIECE_LENGTH, 16384);
    for size in [1u32, 2, 16383, 16384, 16385, 32767, 32768, 32769, u32::MAX] {
        let pieces = size.div_ceil(PIECE_LENGTH);
        for piece in [0, pieces - 1] {
            let range = piece_range(piece, size, u32::MAX).unwrap();
            assert_eq!(range.start, u64::from(piece) * u64::from(PIECE_LENGTH));
            let expected = (u64::from(size) - range.start).min(u64::from(PIECE_LENGTH)) as usize;
            assert_eq!(range.end - range.start, expected as u64);
            let body = vec![b'e'; expected];
            let message = Message::new(
                Kind::Data {
                    piece,
                    total_size: size,
                },
                &body,
                u32::MAX,
            )
            .unwrap();
            let encoded = message.encode(Limits::default()).unwrap();
            assert_eq!(
                Message::parse(&encoded, Limits::default(), u32::MAX)
                    .unwrap()
                    .payload(),
                body
            );
            for actual in [expected - 1, expected + 1] {
                let wrong = vec![0; actual];
                assert_eq!(
                    Message::new(
                        Kind::Data {
                            piece,
                            total_size: size
                        },
                        &wrong,
                        u32::MAX
                    )
                    .unwrap_err(),
                    Error::PieceLength { expected, actual }
                );
            }
        }
        assert_eq!(
            piece_range(pieces, size, u32::MAX),
            Err(Error::PieceIndex(pieces))
        );
        assert_eq!(
            piece_range(u32::MAX, size, u32::MAX),
            Err(Error::PieceIndex(u32::MAX))
        );
    }
    assert_eq!(
        piece_range(0, 0, u32::MAX),
        Err(Error::MetadataSize {
            size: 0,
            limit: u32::MAX
        })
    );
    assert_eq!(
        piece_range(0, 16385, 16384),
        Err(Error::MetadataSize {
            size: 16385,
            limit: 16384
        })
    );
    assert_eq!(
        piece_range(0, 1, 0),
        Err(Error::MetadataSize { size: 1, limit: 0 })
    );
}

#[test]
fn truncations_and_deterministic_mutations_do_not_panic() {
    for bytes in [
        b"d8:msg_typei0e5:piecei0ee".as_slice(),
        b"d8:msg_typei1e5:piecei0e10:total_sizei4ee\0\xffde",
    ] {
        for end in 0..bytes.len() {
            assert!(parse(&bytes[..end]).is_err(), "at {end}");
        }
        for index in 0..bytes.len() {
            for byte in 0..=255 {
                let mut input = bytes.to_vec();
                input[index] = byte;
                if let Ok(message) = parse(&input) {
                    let encoded = message.encode(Limits::default()).unwrap();
                    let reparsed = parse(&encoded).unwrap();
                    assert_eq!(reparsed.kind(), message.kind());
                    assert_eq!(reparsed.payload(), message.payload());
                    assert_eq!(reparsed.header(), message.header());
                }
            }
        }
    }
}
