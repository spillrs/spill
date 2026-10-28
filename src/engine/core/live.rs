//! Shared live-download blocks, reserved against the picker's unfinished budget.
//! A new attempt has a distinct identity; no failed attempt can fill its gaps.
use super::SharedBlock;
use crate::protocol::metainfo::{FileKind, OwnedMetainfo};
use sha1::{Digest, Sha1};
use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    sync::{Arc, Weak},
};

pub(in crate::engine) const BUDGET: u64 = crate::engine::core::PIECE_BUDGET;
struct Piece {
    epoch: Weak<()>,
    range: Range<u64>,
    blocks: BTreeMap<u32, SharedBlock>,
}
pub(in crate::engine) struct Buffered {
    length: u64,
    blocks: BTreeMap<u32, SharedBlock>,
}
impl Buffered {
    pub(in crate::engine) fn digest(self) -> Vec<u8> {
        let mut hash = Sha1::new();
        let mut offset = 0;
        for (begin, block) in self.blocks {
            zeros(&mut hash, u64::from(begin) - offset);
            hash.update(&*block);
            offset = u64::from(begin) + block.len() as u64;
        }
        zeros(&mut hash, self.length - offset);
        hash.finalize().to_vec()
    }
}
fn zeros(hash: &mut Sha1, mut length: u64) {
    const ZERO: [u8; 16384] = [0; 16384];
    while length != 0 {
        let n = length.min(ZERO.len() as u64) as usize;
        hash.update(&ZERO[..n]);
        length -= n as u64;
    }
}
pub(in crate::engine) struct Live {
    pieces: HashMap<u32, Piece>,
    hashing: HashMap<u32, u64>,
    reserved: u64,
    data: Vec<Range<u64>>,
}
impl Live {
    pub(in crate::engine) fn new(meta: &OwnedMetainfo) -> Self {
        let mut offset = 0;
        let data = meta
            .layout()
            .files()
            .iter()
            .filter_map(|file| {
                let range = offset..offset + file.length();
                offset = range.end;
                (file.kind() == FileKind::Data && !range.is_empty()).then_some(range)
            })
            .collect();
        Self {
            pieces: HashMap::new(),
            hashing: HashMap::new(),
            reserved: 0,
            data,
        }
    }
    pub(in crate::engine) fn prune(&mut self) {
        self.pieces.retain(|_, piece| {
            if piece.epoch.strong_count() != 0 {
                return true;
            }
            self.reserved -= piece.range.end - piece.range.start;
            false
        });
    }
    pub(in crate::engine) fn discard(&mut self, index: u32) {
        if let Some(piece) = self.pieces.remove(&index) {
            self.reserved -= piece.range.end - piece.range.start;
        }
    }
    pub(in crate::engine) fn record(
        &mut self,
        meta: &OwnedMetainfo,
        index: u32,
        offset: u32,
        epoch: &Arc<()>,
        block: SharedBlock,
    ) {
        if self
            .pieces
            .get(&index)
            .is_some_and(|p| !p.epoch.ptr_eq(&Arc::downgrade(epoch)))
        {
            self.discard(index);
        }
        let range = meta
            .layout()
            .piece_range(index)
            .expect("validated live piece");
        let length = range.end - range.start;
        if !self.pieces.contains_key(&index) {
            if length > BUDGET - self.reserved || self.hashing.contains_key(&index) {
                // Over-budget or already-hashing pieces are verified by disk read-back.
                return;
            }
            self.reserved += length;
            self.pieces.insert(
                index,
                Piece {
                    epoch: Arc::downgrade(epoch),
                    range,
                    blocks: BTreeMap::new(),
                },
            );
        }
        self.pieces
            .get_mut(&index)
            .expect("live piece")
            .blocks
            .insert(offset, block);
    }
    fn padding(&self, start: u64, end: u64) -> bool {
        if start == end {
            return true;
        }
        let first = self.data.partition_point(|range| range.end <= start);
        self.data.get(first).is_none_or(|range| range.start >= end)
    }
    pub(in crate::engine) fn take(
        &mut self,
        meta: &OwnedMetainfo,
        index: u32,
        epoch: &Arc<()>,
    ) -> Option<Buffered> {
        let range = meta.layout().piece_range(index)?;
        let length = range.end - range.start;
        let Some(piece) = self.pieces.get(&index) else {
            if length <= BUDGET - self.reserved && self.padding(range.start, range.end) {
                self.reserved += length;
                self.hashing.insert(index, length);
                return Some(Buffered {
                    length,
                    blocks: BTreeMap::new(),
                });
            }
            return None;
        };
        if !piece.epoch.ptr_eq(&Arc::downgrade(epoch)) {
            return None;
        }
        let mut offset = 0;
        for (&begin, block) in &piece.blocks {
            let begin = u64::from(begin);
            if begin < offset
                || begin + block.len() as u64 > length
                || !self.padding(range.start + offset, range.start + begin)
            {
                return None;
            }
            offset = begin + block.len() as u64;
        }
        if !self.padding(range.start + offset, range.end) {
            return None;
        }
        let piece = self.pieces.remove(&index).expect("live piece");
        self.hashing.insert(index, length);
        Some(Buffered {
            length,
            blocks: piece.blocks,
        })
    }
    pub(in crate::engine) fn complete(&mut self, index: u32) {
        if let Some(length) = self.hashing.remove(&index) {
            self.reserved -= length;
        }
    }
}
