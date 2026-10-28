//! Pure incremental file-completion geometry; the writer performs the resulting renames.
use protocol::metainfo::OwnedMetainfo;
/// Per-file count of overlapping pieces not yet verified. A hashed piece only
/// touches the files it overlaps, so completion tracking stays O(files per
/// piece) instead of rescanning every file or every piece of a large file.
pub(crate) struct FileProgress {
    /// Start offset of each file; files are contiguous in metainfo order.
    starts: Vec<u64>,
    pub(crate) remaining: Vec<u32>,
    finished: Vec<bool>,
}
impl FileProgress {
    pub(crate) fn new(meta: &OwnedMetainfo, verified: &[u8]) -> Self {
        let length = u64::from(meta.layout().piece_length());
        let mut starts = Vec::with_capacity(meta.layout().files().len());
        let mut remaining = Vec::with_capacity(meta.layout().files().len());
        let mut offset = 0;
        for entry in meta.layout().files() {
            starts.push(offset);
            let end = offset + entry.length();
            remaining.push(if entry.length() == 0 {
                0
            } else {
                (offset / length..=(end - 1) / length)
                    .filter(|&i| !has(verified, i as u32))
                    .count() as u32
            });
            offset = end;
        }
        let finished = vec![false; starts.len()];
        Self {
            starts,
            remaining,
            finished,
        }
    }
    /// Files that are complete and not yet reported; marks them reported.
    pub(crate) fn complete(&mut self) -> Vec<u32> {
        let mut done = Vec::new();
        for (index, finished) in self.finished.iter_mut().enumerate() {
            if !*finished && self.remaining[index] == 0 {
                *finished = true;
                done.push(index as u32);
            }
        }
        done
    }
    /// Apply one piece's verification; returns newly completed files.
    pub(crate) fn changed(&mut self, meta: &OwnedMetainfo, piece: u32) -> Vec<u32> {
        let range = meta.layout().piece_range(piece).expect("validated piece");
        // First file ending after the piece start, through the last file
        // starting before the piece end. Zero-length files have no pieces.
        let first = self
            .starts
            .partition_point(|&start| start <= range.start)
            .max(1)
            - 1;
        let mut done = Vec::new();
        for index in first..self.starts.len() {
            if self.starts[index] >= range.end {
                break;
            }
            let length = meta.layout().files()[index].length();
            if length == 0 || self.starts[index] + length <= range.start {
                continue;
            }
            self.remaining[index] -= 1;
            if self.remaining[index] == 0 && !self.finished[index] {
                self.finished[index] = true;
                done.push(index as u32);
            }
        }
        done
    }
}
fn has(bits: &[u8], index: u32) -> bool {
    super::has(bits, index)
}
