//! Descriptor-relative destinations using validated relative torrent paths.
//! Named destinations never overwrite existing payloads during creation.

use protocol::metainfo::{FileKind, OwnedMetainfo};
use rustix::fs::{
    AtFlags, FileType, FlockOperation, Mode, OFlags, flock, openat, statat, unlinkat,
};
use std::{ffi::OsStr, fs::File, io};

mod verify;
use verify::{check_canceled, verify_content_cancellable};
mod file;
mod named;
pub(crate) mod paths;
use paths::PayloadPaths;

pub struct NamedDestination;

/// Opaque provisioned payload authority, consumed by torrent admission.
pub struct FileStorage {
    meta: OwnedMetainfo,
    files: file::Files,
    read_only: bool,
    binding: Option<DestinationBinding>,
    verified_resume: Option<Vec<u8>>,
}

/// Catalog record of payload paths and whether they were adopted read-only.
/// Paths are bytes so non-UTF-8 torrent names round-trip without loss.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DestinationBinding {
    read_only: bool,
    paths: Vec<Vec<u8>>,
}
impl DestinationBinding {
    pub fn new(meta: &OwnedMetainfo, read_only: bool) -> io::Result<Self> {
        let all = PayloadPaths::new(meta)?;
        Ok(Self {
            read_only,
            paths: all
                .paths
                .into_iter()
                .zip(meta.layout().files())
                .filter_map(|(path, entry)| (entry.kind() == FileKind::Data).then_some(path))
                .collect(),
        })
    }
    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only
    }
    fn payload_paths(&self) -> io::Result<PayloadPaths> {
        PayloadPaths::validated(self.paths.clone())
    }
}

fn open_directory(root: &File, name: impl AsRef<OsStr>) -> io::Result<File> {
    Ok(openat(
        root,
        name.as_ref(),
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?
    .into())
}
fn open_file_mode(
    directory: &File,
    name: impl AsRef<OsStr>,
    create: bool,
    mode: Mode,
) -> io::Result<File> {
    let name = name.as_ref();
    if !create {
        let stat = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Err(invalid("destination requires regular files"));
        }
    }
    let flags =
        OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOCTTY;
    let flags = if create {
        flags | OFlags::CREATE | OFlags::EXCL
    } else {
        flags
    };
    let file: File = openat(directory, name, flags, mode)?.into();
    if !file.metadata()?.is_file() {
        return Err(invalid("destination requires regular files"));
    }
    Ok(file)
}
fn remove_file(directory: &File, name: impl AsRef<OsStr>) -> io::Result<()> {
    match unlinkat(directory, name.as_ref(), AtFlags::empty()) {
        Ok(()) | Err(rustix::io::Errno::NOENT) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
fn check_file(file: &File, length: u64) -> io::Result<()> {
    let stat = file.metadata()?;
    if !stat.is_file() || stat.len() != length {
        return Err(invalid("destination type or length changed"));
    }
    Ok(())
}
fn lock(file: &File) -> io::Result<()> {
    flock(file, FlockOperation::NonBlockingLockExclusive).map_err(Into::into)
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
