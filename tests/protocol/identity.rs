use spill::protocol::{
    bencode::{Limits, decode, encode},
    identity::{InfoHashV1, InfoHashV2, TorrentIdentity},
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
    let hash_v2 = InfoHashV2::from_info_bytes(original);
    assert_ne!(hash_v2, InfoHashV2::from_info_bytes(torrent));
    assert_ne!(
        hash_v2,
        InfoHashV2::from_info_bytes(&encode(info, Limits::default()).unwrap())
    );
    assert!(TorrentIdentity::Hybrid(hash, hash_v2).matches_info_bytes(original));
    // This is a hashing regression fixture, not acceptance of invalid metainfo.
}

#[test]
fn sha256_matches_standard_vectors_and_keeps_full_identity() {
    for (input, expected) in [
        (
            b"".as_slice(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        (
            b"abc",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        ),
        (
            b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
        ),
    ] {
        let hash = InfoHashV2::from_info_bytes(input);
        assert_eq!(hash.to_string(), expected);
        assert_eq!(hash.wire_key(), hash.as_bytes()[..20]);
        assert_eq!(InfoHashV2::from_bytes(*hash.as_bytes()), hash);
    }
    let bytes = b"d4:name1:xe";
    let v1 = InfoHashV1::from_info_bytes(bytes);
    let v2 = InfoHashV2::from_info_bytes(bytes);
    for identity in [
        TorrentIdentity::V1(v1),
        TorrentIdentity::V2(v2),
        TorrentIdentity::Hybrid(v1, v2),
    ] {
        assert!(identity.matches_info_bytes(bytes));
        assert!(!identity.matches_info_bytes(b"d4:name1:ye"));
    }
    assert_eq!(TorrentIdentity::V1(v1).v2(), None);
    assert_eq!(TorrentIdentity::V2(v2).v1(), None);
    assert_eq!(TorrentIdentity::Hybrid(v1, v2).v1(), Some(v1));
    assert_eq!(TorrentIdentity::Hybrid(v1, v2).v2(), Some(v2));
    let mut other = *v2.as_bytes();
    other[31] ^= 1;
    let other = InfoHashV2::from_bytes(other);
    assert_eq!(other.wire_key(), v2.wire_key());
    assert!(!TorrentIdentity::V2(other).matches_info_bytes(bytes));
    assert!(!TorrentIdentity::Hybrid(v1, other).matches_info_bytes(bytes));
    assert!(
        !TorrentIdentity::Hybrid(InfoHashV1::from_bytes([0; 20]), v2).matches_info_bytes(bytes)
    );
}
