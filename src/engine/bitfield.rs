//! MSB-first piece bitmaps shared by core, storage and the runtime.
use crate::protocol::metainfo::Layout;

pub(in crate::engine) fn has(bits: &[u8], index: u32) -> bool {
    bits[index as usize / 8] & (0x80 >> (index % 8)) != 0
}

pub(in crate::engine) fn set(bits: &mut [u8], index: u32) {
    bits[index as usize / 8] |= 0x80 >> (index % 8);
}

pub(in crate::engine) fn clear(bits: &mut [u8], index: u32) {
    bits[index as usize / 8] &= !(0x80 >> (index % 8));
}

pub(in crate::engine) fn verified_bytes(layout: &Layout<'_>, bits: &[u8]) -> u64 {
    (0..layout.piece_count())
        .filter(|&index| has(bits, index))
        .map(|index| {
            let range = layout.piece_range(index).expect("piece");
            range.end - range.start
        })
        .sum()
}
