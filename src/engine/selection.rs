//! File selection projected onto v1's indivisible, hash-verified pieces.
//! This is a scheduling plan, not storage authority or a durable checkpoint.
use crate::protocol::metainfo::{FileKind, Layout};
use std::io;

/// Priorities are ordered for scheduling; a boundary piece takes the maximum
/// priority of the data files it overlaps. Padding has no selectable content.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FilePriority {
    Skip,
    #[default]
    Normal,
    High,
}

/// Immutable, bounded by the metainfo's file and piece counts. Rebuild after a
/// selection change, then replace only at an owner scheduling boundary.
#[derive(Clone, Debug)]
pub struct FileSelection {
    files: Vec<FilePriority>,
    pieces: Vec<FilePriority>,
    wanted_bytes: u64,
}
impl FileSelection {
    /// Indices follow `Layout::files`, including padding entries (which must be
    /// Skip). Reject a mismatched manifest rather than silently selecting files.
    pub fn new(layout: &Layout<'_>, priorities: &[FilePriority]) -> io::Result<Self> {
        if priorities.len() != layout.files().len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file priority count does not match the manifest",
            ));
        }
        let mut selection = Self {
            files: priorities.to_vec(),
            pieces: vec![FilePriority::Skip; layout.piece_count() as usize],
            wanted_bytes: 0,
        };
        let piece_length = u64::from(layout.piece_length());
        for (file, &priority) in layout.files().iter().zip(priorities) {
            if file.kind() == FileKind::Padding {
                if priority != FilePriority::Skip {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "padding files cannot be selected",
                    ));
                }
                continue;
            }
            if priority == FilePriority::Skip {
                continue;
            }
            selection.wanted_bytes += file.length();
            if file.length() == 0 {
                continue;
            }
            let first = (file.offset() / piece_length) as usize;
            let end = (file.offset() + file.length()).div_ceil(piece_length) as usize;
            for piece in &mut selection.pieces[first..end] {
                *piece = (*piece).max(priority);
            }
        }
        Ok(selection)
    }

    pub fn all(layout: &Layout<'_>) -> Self {
        let priorities: Vec<_> = layout
            .files()
            .iter()
            .map(|file| match file.kind() {
                FileKind::Data => FilePriority::Normal,
                FileKind::Padding => FilePriority::Skip,
            })
            .collect();
        Self::new(layout, &priorities).expect("matching manifest and padding priorities")
    }

    pub fn file_priorities(&self) -> &[FilePriority] {
        &self.files
    }

    pub(in crate::engine) fn piece_priorities(&self) -> &[FilePriority] {
        &self.pieces
    }

    /// Selected data bytes only, excluding skipped boundary bytes and padding.
    pub(in crate::engine) fn wanted_bytes(&self) -> u64 {
        self.wanted_bytes
    }

    /// Selected data bytes covered by a particular piece, excluding boundary
    /// bytes belonging to skipped files and synthesized padding.
    pub(in crate::engine) fn piece_wanted_bytes(&self, layout: &Layout<'_>, index: u32) -> u64 {
        layout
            .piece_range(index)
            .and_then(|range| layout.segments(range))
            .into_iter()
            .flatten()
            .filter(|segment| {
                segment.kind == FileKind::Data
                    && self.files[segment.file_index] != FilePriority::Skip
            })
            .map(|segment| segment.length)
            .sum()
    }

    pub(in crate::engine) fn verified_bytes(&self, layout: &Layout<'_>, verified: &[u8]) -> u64 {
        self.pieces
            .iter()
            .enumerate()
            .filter(|(index, _)| verified_piece(verified, *index))
            .map(|(index, _)| self.piece_wanted_bytes(layout, index as u32))
            .sum()
    }

    /// All selected content is covered by verified pieces. Bits are MSB-first,
    /// just like peer bitfields and resume checkpoints; missing bytes are false.
    /// This never fabricates membership for a skipped piece.
    pub(in crate::engine) fn is_complete(&self, verified: &[u8]) -> bool {
        self.pieces.iter().enumerate().all(|(index, priority)| {
            *priority == FilePriority::Skip || verified_piece(verified, index)
        })
    }
}

fn verified_piece(verified: &[u8], index: usize) -> bool {
    verified
        .get(index / 8)
        .is_some_and(|byte| byte & (0x80 >> (index % 8)) != 0)
}
