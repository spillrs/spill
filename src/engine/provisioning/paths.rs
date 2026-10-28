//! Validated byte paths, opened one component at a time beneath a caller-owned directory.
use super::{check_file, invalid, open_directory, open_file_mode, remove_file};
use crate::protocol::metainfo::{FileKind, Layout, OwnedMetainfo};
use rustix::fs::{AtFlags, Mode, RenameFlags, mkdirat, renameat_with, statat, unlinkat};
use std::{collections::BTreeSet, ffi::OsStr, fs::File, io, os::unix::ffi::OsStrExt};

const MAX_PATH: usize = 4096;
const MAX_DEPTH: usize = 32;
// Conservative component budget, including room for payload `.part` names.
const NAME_BYTES: usize = 244;

pub(super) fn safe_name(raw: &[u8]) -> io::Result<Vec<u8>> {
    use sha1::{Digest, Sha1};
    if raw.is_empty() || raw == b"." || raw == b".." || raw.iter().any(|b| matches!(b, 0 | b'/')) {
        return Err(invalid("unsafe payload path component"));
    }
    let mut name: Vec<_> = raw
        .iter()
        .map(|&b| if matches!(b, b':' | b'\\') { b'_' } else { b })
        .collect();
    if name != raw || name.len() > NAME_BYTES {
        // A digest distinguishes replacement/truncation aliases, independent of
        // file order. The normal collision checks still reject deliberate aliases.
        let boundary = |mut end: usize| {
            if let Ok(text) = std::str::from_utf8(&name) {
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
            }
            end
        };
        let dot = name.iter().rposition(|&b| b == b'.').filter(|&i| i > 0);
        // Pathological extensions can themselves exceed the component budget.
        // Retain as much of those as fits, too, on a character boundary.
        let extension = dot.map_or_else(Vec::new, |start| {
            name[start..boundary((start + NAME_BYTES - 17).min(name.len()))].to_vec()
        });
        let end = boundary(
            dot.unwrap_or(name.len())
                .min(NAME_BYTES - 17 - extension.len()),
        );
        name.truncate(end);
        name.push(b'~');
        for byte in &Sha1::digest(raw)[..8] {
            name.extend_from_slice(format!("{byte:02x}").as_bytes());
        }
        name.extend_from_slice(&extension);
    }
    Ok(name)
}

/// Validated on-disk paths, built once and indexed for file observations.
#[derive(PartialEq, Eq)]
pub(in crate::engine) struct PayloadPaths {
    pub(super) paths: Vec<Vec<u8>>,
}
impl PayloadPaths {
    pub(in crate::engine) fn new(meta: &OwnedMetainfo) -> io::Result<Self> {
        let root_name = safe_name(meta.name())?;
        let mut paths = Vec::with_capacity(meta.layout().files().len());
        for entry in meta.layout().files() {
            let mut path = Vec::new();
            if meta.layout().is_multi_file() {
                path.extend_from_slice(&root_name);
            }
            for part in entry.path() {
                let name = safe_name(part)?;
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(&name);
                if path.len() > MAX_PATH {
                    return Err(invalid("payload path exceeds byte limit"));
                }
            }
            paths.push(path);
        }
        Self::validated(paths)
    }
    pub(super) fn validated(paths: Vec<Vec<u8>>) -> io::Result<Self> {
        let mut unique = BTreeSet::new();
        for path in &paths {
            if path.is_empty()
                || path.len() > MAX_PATH
                || path.split(|&b| b == b'/').count() > MAX_DEPTH
            {
                return Err(invalid("payload path exceeds length/depth policy"));
            }
            for part in path.split(|&b| b == b'/') {
                component(part)?;
            }
            if !unique.insert(path.as_slice()) {
                return Err(invalid("duplicate payload path"));
            }
        }
        let partials: Vec<Vec<u8>> = paths
            .iter()
            .map(|p| [p.as_slice(), b".part"].concat())
            .collect();
        for partial in &partials {
            component(filename(partial).as_bytes())?;
            if partial.len() > MAX_PATH || !unique.insert(partial.as_slice()) {
                return Err(invalid("partial payload path collision"));
            }
        }
        for path in paths.iter().chain(&partials) {
            for (index, &byte) in path.iter().enumerate() {
                if byte == b'/' && unique.contains(&path[..index]) {
                    return Err(invalid("payload file/directory collision"));
                }
            }
        }
        Ok(Self { paths })
    }
    pub(in crate::engine) fn files_at(
        &self,
        payload: &File,
        layout: &Layout<'_>,
        create: bool,
    ) -> io::Result<Vec<Option<File>>> {
        let mut files = Vec::with_capacity(self.paths.len());
        for (path, entry) in self.paths.iter().zip(layout.files()) {
            let opened = (|| {
                if entry.kind() != FileKind::Data {
                    return Ok(None);
                }
                // A crash may lose unsynced directory entries. Bound downloads
                // can recreate missing files; their new fingerprint forces recheck.
                let parent = parent(payload, path, true)?;
                let existing = stored_name(&parent, path)?;
                if create && existing.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "file already exists; choose another output directory",
                    ));
                }
                let missing = existing.is_none();
                let name = existing.unwrap_or_else(|| {
                    if entry.length() > 0 {
                        partial_name(path)
                    } else {
                        filename(path).to_owned()
                    }
                });
                let file = open_file_mode(&parent, &name, missing, Mode::from_raw_mode(0o666))?;
                super::lock(&file)?;
                if missing {
                    file.set_len(entry.length())?;
                } else {
                    check_file(&file, entry.length())?;
                }
                Ok(Some(file))
            })();
            files.push(opened.map_err(|error: io::Error| {
                io::Error::new(
                    error.kind(),
                    format!("payload {}: {error}", OsStr::from_bytes(path).display()),
                )
            })?);
        }
        Ok(files)
    }
    pub(in crate::engine) fn reject_existing(&self, root: &File) -> io::Result<()> {
        for path in &self.paths {
            if let Some(parent) = optional_parent(root, path)?
                && stored_name(&parent, path)?.is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "payload {}: file already exists; choose another output directory",
                        OsStr::from_bytes(path).display()
                    ),
                ));
            }
        }
        Ok(())
    }
    pub(in crate::engine) fn complete_files(
        &self,
        root: &File,
        layout: &Layout<'_>,
    ) -> io::Result<Option<Vec<Option<File>>>> {
        let mut files = Vec::with_capacity(self.paths.len());
        for (path, entry) in self.paths.iter().zip(layout.files()) {
            if entry.kind() != FileKind::Data {
                files.push(None);
                continue;
            }
            let parent = match parent(root, path, false) {
                Ok(parent) => parent,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            };
            if stored_name(&parent, path)?.as_deref() != Some(filename(path)) {
                return Ok(None);
            }
            let file = read_only_file(&parent, filename(path))?;
            if check_file(&file, entry.length()).is_err() {
                return Ok(None);
            }
            files.push(Some(file));
        }
        Ok(Some(files))
    }
    pub(in crate::engine) fn finish_at(&self, payload: &File, index: u32) -> io::Result<()> {
        let path = self
            .paths
            .get(index as usize)
            .ok_or_else(|| invalid("invalid file"))?;
        let parent = parent(payload, path, false)?;
        if stored_name(&parent, path)?.as_deref() == Some(partial_name(path).as_os_str()) {
            renameat_with(
                &parent,
                partial_name(path),
                &parent,
                filename(path),
                RenameFlags::NOREPLACE,
            )?;
        }
        Ok(())
    }
    pub(in crate::engine) fn remove_at(&self, payload: &File) -> io::Result<()> {
        let mut leases = Vec::new();
        // Validate all surviving payloads before any deletion. Missing parents
        // are permitted only for interrupted cleanup; symlinks are never followed.
        for path in &self.paths {
            if let Some(parent) = optional_parent(payload, path)? {
                let Some(name) = stored_name(&parent, path)? else {
                    continue;
                };
                match read_only_file(&parent, &name) {
                    Ok(file) => {
                        super::lock(&file)?;
                        leases.push(file);
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        let mut directories = BTreeSet::new();
        for path in &self.paths {
            if let Some(parent) = optional_parent(payload, path)?
                && let Some(name) = stored_name(&parent, path)?
            {
                remove_file(&parent, name)?;
            }
            for (index, &byte) in path.iter().enumerate() {
                if byte == b'/' {
                    directories.insert(&path[..index]);
                }
            }
        }
        // Lexicographic reverse order visits descendants before their parents.
        for path in directories.into_iter().rev() {
            if let Some(parent) = optional_parent(payload, path)? {
                match unlinkat(&parent, filename(path), AtFlags::REMOVEDIR) {
                    Ok(()) | Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTEMPTY) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(())
    }
}
fn read_only_file(parent: &File, name: &OsStr) -> io::Result<File> {
    let file: File = rustix::fs::openat(
        parent,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        Mode::empty(),
    )?
    .into();
    check_file(&file, file.metadata()?.len())?;
    Ok(file)
}
fn partial_name(path: &[u8]) -> std::ffi::OsString {
    let mut name = filename(path).to_owned();
    name.push(".part");
    name
}
fn stored_name(parent: &File, path: &[u8]) -> io::Result<Option<std::ffi::OsString>> {
    let mut found = None;
    for name in [filename(path).to_owned(), partial_name(path)] {
        match statat(parent, &name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(_) if found.is_some() => {
                return Err(invalid("both final and partial payload names exist"));
            }
            Ok(_) => found = Some(name),
            Err(rustix::io::Errno::NOENT) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(found)
}
fn component(bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty()
        || bytes.len() > 255
        || bytes == b"."
        || bytes == b".."
        || bytes.iter().any(|b| matches!(b, 0 | b'/' | b'\\' | b':'))
    {
        return Err(invalid("unsafe payload path component"));
    }
    Ok(())
}
fn filename(path: &[u8]) -> &OsStr {
    OsStr::from_bytes(path.rsplit(|&b| b == b'/').next().expect("validated path"))
}
fn parent(directory: &File, path: &[u8], create: bool) -> io::Result<File> {
    let mut parent = directory.try_clone()?;
    let mut components = path.split(|&b| b == b'/').peekable();
    while let Some(part) = components.next() {
        if components.peek().is_none() {
            break;
        }
        let name = OsStr::from_bytes(part);
        if create {
            match mkdirat(&parent, name, Mode::from_raw_mode(0o777)) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let child = open_directory(&parent, name)?;
        parent = child;
    }
    Ok(parent)
}
fn optional_parent(directory: &File, path: &[u8]) -> io::Result<Option<File>> {
    match parent(directory, path, false) {
        Ok(parent) => Ok(Some(parent)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}
