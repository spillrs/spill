//! Named payloads whose relative paths and resume are retained by the caller.
//! Payload leases last with the sink; removal is restricted to recorded paths.
use super::*;

impl NamedDestination {
    /// Record writable paths before creating payloads so interrupted creation
    /// can be reopened for recovery. Complete existing seeds stay read-only.
    pub(in crate::engine) fn open_cancellable(
        root: File,
        meta: &OwnedMetainfo,
        record: impl FnOnce(&DestinationBinding) -> io::Result<()>,
        canceled: impl Fn() -> bool,
    ) -> io::Result<FileStorage> {
        if let Some(sink) = Self::adopt_complete_cancellable(root.try_clone()?, meta, &canceled)? {
            record(sink.binding())?;
            return Ok(sink);
        }
        let binding = DestinationBinding::new(meta, false)?;
        record(&binding)?;
        Self::open_bound(root, meta, None, true, &canceled)
    }
    pub(in crate::engine) fn create(
        root: File,
        meta: &OwnedMetainfo,
        record: impl FnOnce(&DestinationBinding) -> io::Result<()>,
        canceled: impl Fn() -> bool,
    ) -> io::Result<FileStorage> {
        check_canceled(&canceled)?;
        PayloadPaths::new(meta)?.reject_existing(&root)?;
        record(&DestinationBinding::new(meta, false)?)?;
        Self::open_bound(root, meta, None, true, &canceled)
    }
    /// Probe final files without creating payloads, then verify once under the
    /// leases retained by the returned sink.
    pub(in crate::engine) fn adopt_complete_cancellable(
        root: File,
        meta: &OwnedMetainfo,
        canceled: impl Fn() -> bool,
    ) -> io::Result<Option<FileStorage>> {
        check_canceled(&canceled)?;
        if !root.metadata()?.is_dir() {
            return Err(invalid("output is not a directory"));
        }
        let paths = PayloadPaths::new(meta)?;
        let Some(files) = paths.complete_files(&root, meta.layout())? else {
            paths.reject_existing(&root)?;
            return Ok(None);
        };
        Self::finish_read_only(root, meta, files, None, true, &canceled).map(Some)
    }
    /// Reopen an existing binding without hashing. This does NOT grant piece
    /// availability: the caller must run fingerprint-based storage recovery (or
    /// explicit recheck) before advertising/uploading.
    pub(in crate::engine) fn reopen_for_recovery(
        root: File,
        meta: &OwnedMetainfo,
        binding: &DestinationBinding,
        canceled: impl Fn() -> bool,
    ) -> io::Result<FileStorage> {
        Self::open_bound(root, meta, Some(binding), false, &canceled)
    }
    fn open_bound(
        root: File,
        meta: &OwnedMetainfo,
        binding: Option<&DestinationBinding>,
        verify: bool,
        canceled: &dyn Fn() -> bool,
    ) -> io::Result<FileStorage> {
        check_canceled(canceled)?;
        if !root.metadata()?.is_dir() {
            return Err(invalid("output is not a directory"));
        }
        let paths = PayloadPaths::new(meta)?;
        if binding.is_some_and(DestinationBinding::is_read_only) {
            let files = paths
                .complete_files(&root, meta.layout())?
                .ok_or_else(|| invalid("seed payload is incomplete"))?;
            return Self::finish_read_only(root, meta, files, binding, verify, canceled);
        }
        let record = Self::check_binding(meta, binding, false)?;
        let files = paths.files_at(&root, meta.layout(), binding.is_none())?;
        Ok(FileStorage::named(meta.clone(), files, root)?.with_binding(record, None))
    }
    fn finish_read_only(
        _root: File,
        meta: &OwnedMetainfo,
        mut files: Vec<Option<File>>,
        binding: Option<&DestinationBinding>,
        verify: bool,
        canceled: &dyn Fn() -> bool,
    ) -> io::Result<FileStorage> {
        for file in files.iter().flatten() {
            check_canceled(canceled)?;
            lock(file)?;
        }
        let record = Self::check_binding(meta, binding, true)?;
        let verified_resume = if verify {
            let before = FileStorage::fingerprint_files(files.iter().map(Option::as_ref))?;
            verify_content_cancellable(meta, &mut files, canceled)?;
            check_canceled(canceled)?;
            let after = FileStorage::fingerprint_files(files.iter().map(Option::as_ref))?;
            if before != after {
                return Err(invalid("seed payload changed during verification"));
            }
            let count = meta.layout().piece_count();
            let mut bits = vec![255; count.div_ceil(8) as usize];
            if !count.is_multiple_of(8)
                && let Some(last) = bits.last_mut()
            {
                *last <<= 8 - count % 8;
            }
            let resume = crate::protocol::resume::encode(meta, &bits, &after)?;
            Some(resume)
        } else {
            None
        };
        Ok(FileStorage::read_only(meta.clone(), files)?.with_binding(record, verified_resume))
    }
    fn check_binding(
        meta: &OwnedMetainfo,
        binding: Option<&DestinationBinding>,
        read_only: bool,
    ) -> io::Result<DestinationBinding> {
        let record = DestinationBinding::new(meta, read_only)?;
        if binding.is_some_and(|binding| binding != &record) {
            return Err(invalid("download destination paths changed"));
        }
        Ok(record)
    }
    pub fn remove(root: File, binding: &DestinationBinding) -> io::Result<()> {
        if binding.is_read_only() {
            return Ok(());
        }
        binding.payload_paths()?.remove_at(&root)
    }
}
