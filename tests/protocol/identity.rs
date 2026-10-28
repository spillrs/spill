use spill::protocol::{
    bencode::{Limits, decode, encode},
    identity::InfoHashV1,
};

#[test]
fn sha1_matches_standard_vectors() {
    for (input, expected) in [
        (b"".as_slice(), "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
        (b"abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
        (
            b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
        ),
    ] {
        assert_eq!(InfoHashV1::from_info_bytes(input).to_string(), expected);
    }
}

#[test]
fn identity_uses_original_info_span_not_normalized_or_outer_bytes() {
    let torrent = b"d4:infod1:bi2e1:ai1eee";
    let root = decode(torrent, Limits::default()).unwrap();
    let info = root.get(b"info").unwrap();
    let original = info.raw_bytes().unwrap();
    let hash = InfoHashV1::from_info_bytes(original);
    assert_eq!(hash, InfoHashV1::from_info_bytes(b"d1:bi2e1:ai1ee"));
    assert_ne!(hash, InfoHashV1::from_info_bytes(torrent));
    assert_ne!(
        hash,
        InfoHashV1::from_info_bytes(&encode(info, Limits::default()).unwrap())
    );
    // This is a hashing regression fixture, not acceptance of invalid metainfo.
}
