use protocol::{
    bencode::Limits,
    metainfo::OwnedMetainfo,
    resume::{self, ResumeProgress, decode, encode, resume_progress},
};

#[test]
fn file_backend_fingerprints_fit_at_the_file_limit() {
    let meta = OwnedMetainfo::parse(
        b"d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
        Limits::default(),
    )
    .unwrap();
    let mut fingerprint = vec![0; 40 * resume::MAX_FILES];
    let bytes = resume::encode(&meta, &[0x80], &fingerprint).unwrap();
    let decoded = resume::decode(&meta, &bytes).unwrap();
    assert_eq!(decoded.verified, [0x80]);
    assert_eq!(decoded.fingerprint, fingerprint);
    fingerprint.push(0);
    assert!(resume::encode(&meta, &[0x80], &fingerprint).is_err());
}

#[test]
fn display_counts_include_the_short_final_piece_and_require_a_valid_record() {
    let meta = OwnedMetainfo::parse(
        b"d4:infod6:lengthi5e4:name1:x12:piece lengthi4e6:pieces40:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaee",
        Limits::default(),
    )
    .unwrap();
    for (bits, bytes, pieces) in [(0, 0, 0), (0x80, 4, 1), (0x40, 1, 1), (0xc0, 5, 2)] {
        let blob = encode(&meta, &[bits], b"fingerprint").unwrap();
        assert_eq!(
            resume_progress(&meta, &blob).unwrap(),
            ResumeProgress {
                verified_bytes: bytes,
                verified_pieces: pieces
            }
        );
        assert!(resume_progress(&meta, &blob[..blob.len() - 1]).is_err());
    }
}

#[test]
fn roundtrip_and_reject_corruption_identity_shape_and_trailing_bytes() {
    let meta = OwnedMetainfo::parse(
        b"d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
        Limits::default(),
    )
    .unwrap();
    let bytes = encode(&meta, &[0x80], b"fingerprint").unwrap();
    let record = decode(&meta, &bytes).unwrap();
    assert_eq!(record.verified, [0x80]);
    assert_eq!(record.fingerprint, b"fingerprint");
    for end in 0..bytes.len() {
        assert!(decode(&meta, &bytes[..end]).is_err());
    }
    for index in 0..bytes.len() {
        let mut bad = bytes.clone();
        bad[index] ^= 1;
        assert!(decode(&meta, &bad).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(decode(&meta, &trailing).is_err());
    assert!(encode(&meta, &[0x81], b"").is_err());
    assert!(encode(&meta, &[], b"").is_err());
}
