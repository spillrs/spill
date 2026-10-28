//! Byte-bounded LRU upload windows. Availability is owned by the downloader;
//! callers request cache loads only for verified pieces. Uploads do not rehash.
use std::{
    collections::VecDeque,
    io,
    ops::Range,
    sync::{Arc, RwLock},
};

pub(crate) const MAX_WINDOW: usize = 256 * 1024;

/// A bounded read-ahead window containing the entire request, including an
/// unaligned block crossing the normal window boundary or the piece's tail.
pub(crate) fn window(
    piece_len: usize,
    budget: usize,
    offset: u32,
    length: u32,
) -> Option<Range<usize>> {
    let end = u64::from(offset) + u64::from(length);
    let size = budget.min(MAX_WINDOW).min(piece_len);
    if length == 0 || length as usize > size || end > piece_len as u64 {
        return None;
    }
    let mut start = offset as usize / size * size;
    if end as usize > start + size {
        start = offset as usize;
    }
    Some(start..start + size.min(piece_len - start))
}

type Entries = VecDeque<(u32, u32, Arc<Vec<u8>>)>;
pub(crate) struct CachedBlock {
    pub(crate) bytes: Arc<Vec<u8>>,
    pub(crate) range: Range<usize>,
}
#[derive(Clone, Default)]
pub(crate) struct CacheView(Arc<RwLock<Entries>>);
impl CacheView {
    pub(crate) fn block(
        &self,
        index: u32,
        offset: u32,
        length: u32,
    ) -> io::Result<Option<CachedBlock>> {
        let end = u64::from(offset) + u64::from(length);
        let mut entries = self
            .0
            .write()
            .map_err(|_| io::Error::other("upload cache poisoned"))?;
        let Some(position) = entries.iter().position(|(i, start, bytes)| {
            *i == index && *start <= offset && end <= u64::from(*start) + bytes.len() as u64
        }) else {
            return Ok(None);
        };
        let entry = entries.remove(position).expect("cache hit");
        let begin = (offset - entry.1) as usize;
        let hit = CachedBlock {
            bytes: entry.2.clone(),
            range: begin..begin + length as usize,
        };
        entries.push_back(entry);
        Ok(Some(hit))
    }
    pub(crate) fn insert(
        &self,
        index: u32,
        offset: u32,
        bytes: Vec<u8>,
        budget: usize,
    ) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > budget {
            return Err(io::Error::other("invalid upload cache window"));
        }
        let mut entries = self
            .0
            .write()
            .map_err(|_| io::Error::other("upload cache poisoned"))?;
        entries.retain(|(i, start, _)| *i != index || *start != offset);
        let mut used = entries.iter().map(|(_, _, b)| b.len()).sum::<usize>();
        while used > budget - bytes.len() {
            used -= entries.pop_front().expect("cache eviction").2.len();
        }
        entries.push_back((index, offset, Arc::new(bytes)));
        Ok(())
    }
}
