use super::*;

pub(super) fn ready(peer: &PeerState) -> bool {
    peer.established
        && !peer.retiring
        && peer.interested
        && (!peer.choked || peer.fast && peer.allowed_fast.iter().any(|&byte| byte != 0))
}
pub(super) fn eligible(peer: &PeerState, index: u32) -> bool {
    ready(peer) && has(&peer.pieces, index) && (!peer.choked || has(&peer.allowed_fast, index))
}
// First set bit in `present & !excluded`, at/after start. Both wire-order
// bitmaps have equal length and zero spare bits. Scan 64 bits at a time without
// proportional scratch allocation; cache the result until a relevant event.
pub(super) fn first_difference(present: &[u8], excluded: &[u8], start: u32) -> Option<u32> {
    assert_eq!(present.len(), excluded.len());
    let mut byte = usize::try_from(start / 8).expect("bitmap addressable");
    if byte >= present.len() {
        return None;
    }
    let mut skip = start % 8;
    for (a, b) in present[byte..].chunks(8).zip(excluded[byte..].chunks(8)) {
        let mut left = [0; 8];
        let mut right = [0; 8];
        left[..a.len()].copy_from_slice(a);
        right[..b.len()].copy_from_slice(b);
        let word = u64::from_be_bytes(left) & !u64::from_be_bytes(right) & (u64::MAX >> skip);
        if word != 0 {
            return Some((byte * 8 + word.leading_zeros() as usize) as u32);
        }
        byte += a.len();
        skip = 0;
    }
    None
}
