//! Confined filesystem block storage. Sparse file creation and path authority are
//! supplied by provisioning. Operations are synchronous; Writer dispatches them to
//! the blocking pool. Only the completion barrier syncs payload data.
use super::FileStorage;
use crate::engine::provisioning::paths::PayloadPaths;
use crate::engine::{core::SharedBlock, storage::PieceSet};
use crate::protocol::metainfo::{FileKind, OwnedMetainfo};
use std::{
    fs::File,
    io,
    os::unix::fs::{FileExt, MetadataExt},
};

const FINGERPRINT_BYTES: usize = 40;

struct Extent {
    offset: u64,
    length: u64,
    file: Option<File>,
}

pub(super) struct Files {
    entries: Vec<Extent>,
    payload_root: Option<File>,
    paths: Option<PayloadPaths>,
}
impl Files {
    /// Entries overlapping `[start, end)`. Entries are contiguous and sorted by
    /// offset, so a block touches a handful of files even in huge torrents.
    fn span(&self, start: u64, end: u64) -> std::ops::Range<usize> {
        let first = self
            .entries
            .partition_point(|e| e.offset + e.length <= start);
        let last = first + self.entries[first..].partition_point(|e| e.offset < end);
        first..last
    }
}
impl FileStorage {
    pub(in crate::engine) fn with_binding(
        mut self,
        binding: crate::engine::provisioning::DestinationBinding,
        resume: Option<Vec<u8>>,
    ) -> Self {
        self.binding = Some(binding);
        self.verified_resume = resume;
        self
    }
    pub(in crate::engine) fn binding(&self) -> &crate::engine::provisioning::DestinationBinding {
        self.binding.as_ref().expect("provisioned destination")
    }
    pub(in crate::engine) fn verified_resume(&self) -> Option<&[u8]> {
        self.verified_resume.as_deref()
    }
    /// Caller-opened exact-sized, regular files, in metainfo order (None for
    /// padding). Hardlinks are allowed. Fingerprints detect content changes at
    /// startup/resume; callers must not modify payloads during active use.
    pub(super) fn read_only(meta: OwnedMetainfo, files: Vec<Option<File>>) -> io::Result<Self> {
        Self::new(meta, files, true)
    }
    pub(in crate::engine) fn named(
        meta: OwnedMetainfo,
        files: Vec<Option<File>>,
        root: File,
    ) -> io::Result<Self> {
        let paths = PayloadPaths::new(&meta)?;
        let mut storage = Self::new(meta, files, false)?;
        storage.files.payload_root = Some(root);
        storage.files.paths = Some(paths);
        Ok(storage)
    }
    pub(in crate::engine) fn new(
        meta: OwnedMetainfo,
        files: Vec<Option<File>>,
        read_only: bool,
    ) -> io::Result<Self> {
        if files.len() != meta.layout().files().len() {
            return Err(invalid("file count mismatch"));
        }
        let mut entries = Vec::with_capacity(files.len());
        let mut offset = 0;
        for (entry, file) in meta.layout().files().iter().zip(files) {
            if file.is_some() != (entry.kind() == FileKind::Data) {
                return Err(invalid("file/padding mismatch"));
            }
            if let Some(file) = &file {
                let stat = file.metadata()?;
                if !stat.is_file() || stat.len() != entry.length() {
                    return Err(invalid("storage requires exact-sized regular files"));
                }
            }
            entries.push(Extent {
                offset,
                length: entry.length(),
                file,
            });
            offset += entry.length();
        }
        Ok(Self {
            meta,
            files: Files {
                entries,
                payload_root: None,
                paths: None,
            },
            read_only,
            binding: None,
            verified_resume: None,
        })
    }
    /// Same descriptor fingerprint used by resume, also captured around initial
    /// read-only adoption so the verified bytes and saved metadata agree.
    /// mtime/length track content changes; ctime/link count deliberately do not,
    /// so creating or removing a library hardlink does not invalidate the payload.
    pub(in crate::engine) fn fingerprint_files<'a>(
        files: impl Iterator<Item = Option<&'a File>>,
    ) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        for file in files {
            if let Some(file) = file {
                let m = file.metadata()?;
                out.extend_from_slice(&m.len().to_le_bytes());
                out.extend_from_slice(&m.mtime().to_le_bytes());
                out.extend_from_slice(&m.mtime_nsec().to_le_bytes());
                // Replacement files must be rechecked even when a copy tool
                // preserves their size and modification time.
                out.extend_from_slice(&m.dev().to_le_bytes());
                out.extend_from_slice(&m.ino().to_le_bytes());
            } else {
                out.extend_from_slice(&[0; FINGERPRINT_BYTES]);
            }
        }
        Ok(out)
    }
    fn range(&self, piece: u32, offset: u32, len: usize) -> io::Result<u64> {
        let range = self
            .meta
            .layout()
            .piece_range(piece)
            .ok_or_else(|| invalid("invalid piece"))?;
        if u64::from(offset)
            .checked_add(len as u64)
            .is_none_or(|end| end > range.end - range.start)
        {
            return Err(invalid("range crosses piece boundary"));
        }
        Ok(range.start + u64::from(offset))
    }
    pub(in crate::engine) fn write_batch(
        &self,
        blocks: Vec<(u32, u32, SharedBlock)>,
    ) -> io::Result<()> {
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "read-only storage",
            ));
        }
        let blocks = blocks
            .into_iter()
            .map(|(piece, offset, bytes)| {
                self.range(piece, offset, bytes.len())
                    .map(|start| (start, bytes))
            })
            .collect::<io::Result<Vec<_>>>()?;
        for (start, data) in blocks {
            let end = start + data.len() as u64;
            let span = self.files.span(start, end);
            // Validate the entire block's padding before physical writes.
            for e in &self.files.entries[span.clone()] {
                let a = start.max(e.offset);
                let b = end.min(e.offset + e.length);
                if a < b
                    && e.file.is_none()
                    && data[(a - start) as usize..(b - start) as usize]
                        .iter()
                        .any(|&b| b != 0)
                {
                    return Err(invalid("nonzero padding"));
                }
            }
            for e in &self.files.entries[span] {
                let a = start.max(e.offset);
                let b = end.min(e.offset + e.length);
                if a < b
                    && let Some(file) = &e.file
                {
                    file.write_all_at(
                        &data[(a - start) as usize..(b - start) as usize],
                        a - e.offset,
                    )?;
                }
            }
        }
        Ok(())
    }
    /// Positional reads share immutable descriptors with writes. Padding stays
    /// zero-filled, and independent upload jobs retain the storage's leases.
    pub(in crate::engine) fn read(
        &self,
        piece: u32,
        offset: u32,
        length: usize,
    ) -> io::Result<Vec<u8>> {
        let start = self.range(piece, offset, length)?;
        let end = start + length as u64;
        let mut bytes = vec![0; length];
        for e in &self.files.entries[self.files.span(start, end)] {
            let a = start.max(e.offset);
            let b = end.min(e.offset + e.length);
            if a < b
                && let Some(file) = &e.file
            {
                file.read_exact_at(
                    &mut bytes[(a - start) as usize..(b - start) as usize],
                    a - e.offset,
                )?;
            }
        }
        Ok(bytes)
    }
    pub(in crate::engine) fn finish_file(&self, file: u32) -> io::Result<()> {
        if file as usize >= self.meta.layout().files().len() {
            return Err(invalid("invalid file"));
        }
        if self.read_only || self.meta.layout().files()[file as usize].kind() != FileKind::Data {
            return Ok(());
        }
        if let Some(paths) = &self.files.paths
            && let Some(root) = &self.files.payload_root
        {
            paths.finish_at(root, file)?;
        }
        Ok(())
    }
    /// Completion-only durability barrier. Periodic and shutdown resume saves
    /// deliberately do not sync payload data; read-only adopted seeds need no sync.
    pub(in crate::engine) fn sync_data(&self) -> io::Result<()> {
        if !self.read_only {
            for entry in &self.files.entries {
                if let Some(file) = &entry.file {
                    file.sync_data()?;
                }
            }
        }
        Ok(())
    }
    pub(in crate::engine) fn fingerprint(&self) -> io::Result<Vec<u8>> {
        Self::fingerprint_files(self.files.entries.iter().map(|e| e.file.as_ref()))
    }
    pub(in crate::engine) fn changed_since(&self, old: &[u8]) -> io::Result<PieceSet> {
        let now = self.fingerprint()?;
        if old.len() != now.len() {
            return Ok((0..self.meta.layout().piece_count()).collect());
        }
        let mut changed = vec![false; self.meta.layout().piece_count() as usize];
        let mut offset = 0;
        for (i, file) in self.meta.layout().files().iter().enumerate() {
            let start = i * FINGERPRINT_BYTES;
            let end = start + FINGERPRINT_BYTES;
            if file.length() > 0 && now[start..end] != old[start..end] {
                let first = offset / u64::from(self.meta.layout().piece_length());
                let last =
                    (offset + file.length() - 1) / u64::from(self.meta.layout().piece_length());
                changed[first as usize..=last as usize].fill(true);
            }
            offset += file.length();
        }
        Ok(changed
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| b.then_some(i as u32))
            .collect())
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
