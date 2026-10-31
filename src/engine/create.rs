//! Read-only BitTorrent v1 metainfo creation, independent of sessions and networking.

use crate::protocol::{
    bencode::{self, Limits, Node, Value},
    identity::InfoHashV1,
};
use sha1::{Digest, Sha1};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::{self, File, Metadata, OpenOptions},
    io,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use super::profile::{MAX_FILES, MAX_META, MAX_PIECES};

/// Options shared by the offline CLI and embedded callers.
#[derive(Clone, Debug)]
pub struct Options {
    /// Bytes per piece: a power of two from 32 KiB through 256 MiB.
    pub piece_length: u32,
    /// Hashing workers; must be nonzero. Each uses a 64 KiB read buffer.
    pub threads: usize,
    /// Torrent root name; defaults to the source's basename.
    pub name: Option<OsString>,
    /// Tracker tiers. URLs within a tier are alternatives; later tiers are backups.
    pub announce: Vec<Vec<String>>,
    pub web_seeds: Vec<String>,
    pub comment: Option<String>,
    /// Stored inside `info`, so changing this changes the info hash.
    pub source: Option<String>,
    pub private: bool,
    /// Unix epoch seconds; `None` omits the date for reproducible output.
    pub creation_date: Option<u64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            piece_length: 1 << 18,
            threads: std::thread::available_parallelism().map_or(1, usize::from),
            name: None,
            announce: Vec::new(),
            web_seeds: Vec::new(),
            comment: None,
            source: None,
            private: false,
            creation_date: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock after epoch")
                    .as_secs(),
            ),
        }
    }
}

/// A complete torrent document. Creation never writes or modifies the payload.
#[derive(Debug)]
pub struct CreatedTorrent {
    pub metainfo: Vec<u8>,
    pub info_hash: InfoHashV1,
    pub name: OsString,
    pub total_length: u64,
    pub piece_count: u32,
}

/// Hash a file or directory and encode its v1 `.torrent` document.
///
/// This is blocking work; asynchronous callers should use `spawn_blocking`.
/// Paths are byte-sorted, hidden and empty files are included, and empty
/// directories are omitted. An entirely empty directory is rejected. Symlinks
/// are followed, including directory links; ancestor cycles and special files
/// are rejected. Keep the source unchanged until this function returns: observed
/// identity, size or modification-time changes fail creation, but this is not a
/// filesystem snapshot.
///
/// `progress` runs on the calling thread, initially at zero and then after each
/// piece. Returning an error cancels and joins the workers before returning it.
/// The output obeys spill's file, piece and metainfo-size limits. No tracker or
/// web seed is required. Network URLs are checked syntactically, not contacted.
///
/// ```no_run
/// use spill::engine::create::{self, Options};
/// let torrent = create::create(
///     std::path::Path::new("payload"),
///     &Options { creation_date: None, ..Options::default() },
///     &mut |_, _| Ok(()),
/// )?;
/// std::fs::write("payload.torrent", torrent.metainfo)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn create(
    source: &Path,
    options: &Options,
    progress: &mut dyn FnMut(u32, u32) -> io::Result<()>,
) -> io::Result<CreatedTorrent> {
    validate_options(options)?;
    let resolved = fs::canonicalize(source)?;
    let name = options
        .name
        .clone()
        .or_else(|| {
            source
                .file_name()
                .or_else(|| resolved.file_name())
                .map(OsString::from)
        })
        .ok_or_else(|| invalid("source has no basename; provide a name"))?;
    let source = resolved;
    component(name.as_bytes())?;
    let metadata = fs::metadata(&source)?;
    let multi_file = metadata.is_dir();
    let mut files = Vec::new();
    scan(&source, &source, metadata, &mut BTreeSet::new(), &mut files)?;
    if files.is_empty() {
        return Err(invalid("source directory contains no files"));
    }
    // Compare complete path bytes: '/' sorts after '-' but before '0'.
    files.sort_unstable_by(|a, b| {
        a.source
            .as_os_str()
            .as_bytes()
            .cmp(b.source.as_os_str().as_bytes())
    });
    let total: u128 = files.iter().map(|f| u128::from(f.metadata.len())).sum();
    if total > i64::MAX as u128 {
        return Err(invalid("payload is too large for v1 metainfo"));
    }
    let total_length = total as u64;
    let mut offset = 0;
    for file in &mut files {
        file.offset = offset;
        offset += file.metadata.len();
    }
    let count = total_length.div_ceil(u64::from(options.piece_length));
    if count > u64::from(MAX_PIECES) || count * 20 > MAX_META as u64 {
        return Err(invalid("too many pieces; choose a larger piece length"));
    }
    let piece_count = count as u32;
    let hashes = hash(&files, total_length, piece_count, options, progress)?;
    for file in &files {
        unchanged(&file.metadata, &fs::metadata(&file.source)?)?;
    }
    let mut info = BTreeMap::new();
    info.insert(b"name".as_slice(), string(name.as_bytes()));
    info.insert(
        b"piece length".as_slice(),
        integer(i64::from(options.piece_length)),
    );
    info.insert(b"pieces".as_slice(), string(&hashes));
    if multi_file {
        info.insert(
            b"files".as_slice(),
            Node::new(Value::List(
                files
                    .iter()
                    .map(|file| {
                        Node::new(Value::Dictionary(BTreeMap::from([
                            (b"length".as_slice(), integer(file.metadata.len() as i64)),
                            (
                                b"path".as_slice(),
                                Node::new(Value::List(
                                    file.path.iter().map(|p| string(p)).collect(),
                                )),
                            ),
                        ])))
                    })
                    .collect(),
            )),
        );
    } else {
        info.insert(b"length".as_slice(), integer(total_length as i64));
    }
    if options.private {
        info.insert(b"private".as_slice(), integer(1));
    }
    if let Some(source) = &options.source {
        info.insert(b"source".as_slice(), string(source.as_bytes()));
    }
    let limits = Limits {
        max_output_len: MAX_META,
        max_string_len: MAX_META,
        max_nodes: 2_000_000,
        max_depth: 64,
        ..Limits::default()
    };
    let info = Node::new(Value::Dictionary(info));
    let info_hash = InfoHashV1::from_info_bytes(&bencode::encode(&info, limits).map_err(invalid)?);
    let mut document = BTreeMap::from([
        (b"info".as_slice(), info),
        (
            b"created by".as_slice(),
            string(concat!("spill ", env!("CARGO_PKG_VERSION")).as_bytes()),
        ),
    ]);
    if let Some(first) = options.announce.first() {
        document.insert(b"announce".as_slice(), string(first[0].as_bytes()));
        if options.announce.len() > 1 || first.len() > 1 {
            document.insert(
                b"announce-list".as_slice(),
                Node::new(Value::List(
                    options
                        .announce
                        .iter()
                        .map(|tier| {
                            Node::new(Value::List(
                                tier.iter().map(|url| string(url.as_bytes())).collect(),
                            ))
                        })
                        .collect(),
                )),
            );
        }
    }
    if let Some(comment) = &options.comment {
        document.insert(b"comment".as_slice(), string(comment.as_bytes()));
    }
    if let Some(date) = options.creation_date {
        document.insert(b"creation date".as_slice(), integer(date as i64));
    }
    match options.web_seeds.as_slice() {
        [] => {}
        [url] => {
            document.insert(b"url-list".as_slice(), string(url.as_bytes()));
        }
        urls => {
            document.insert(
                b"url-list".as_slice(),
                Node::new(Value::List(
                    urls.iter().map(|u| string(u.as_bytes())).collect(),
                )),
            );
        }
    }
    let metainfo =
        bencode::encode(&Node::new(Value::Dictionary(document)), limits).map_err(invalid)?;
    Ok(CreatedTorrent {
        metainfo,
        info_hash,
        name,
        total_length,
        piece_count,
    })
}

struct Entry {
    source: PathBuf,
    path: Vec<Vec<u8>>,
    metadata: Metadata,
    offset: u64,
}

fn scan(
    root: &Path,
    path: &Path,
    metadata: Metadata,
    ancestors: &mut BTreeSet<(u64, u64)>,
    files: &mut Vec<Entry>,
) -> io::Result<()> {
    if metadata.is_dir() {
        let id = (metadata.dev(), metadata.ino());
        if !ancestors.insert(id) {
            return Err(invalid(format!(
                "directory symlink cycle: {}",
                path.display()
            )));
        }
        // Bound recursion before descending, including trees containing no files.
        if ancestors.len() > 32 {
            return Err(invalid("source directory exceeds 32 levels"));
        }
        for entry in fs::read_dir(path)? {
            let path = entry?.path();
            scan(root, &path, fs::metadata(&path)?, ancestors, files)?;
        }
        ancestors.remove(&id);
    } else if metadata.is_file() {
        if files.len() == MAX_FILES {
            return Err(invalid(format!("too many files (maximum {MAX_FILES})")));
        }
        let relative = path.strip_prefix(root).expect("scanned source descendant");
        let components = relative
            .iter()
            .map(|part| part.as_bytes().to_vec())
            .collect::<Vec<_>>();
        if components.len() >= 32 {
            return Err(invalid(
                "torrent path exceeds 32 levels including the root name",
            ));
        }
        files.push(Entry {
            source: path.to_owned(),
            path: components,
            metadata,
            offset: 0,
        });
    } else {
        return Err(invalid(format!(
            "not a regular file or directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn hash(
    files: &[Entry],
    total: u64,
    count: u32,
    options: &Options,
    progress: &mut dyn FnMut(u32, u32) -> io::Result<()>,
) -> io::Result<Vec<u8>> {
    progress(0, count)?;
    let next = AtomicU32::new(0);
    let canceled = AtomicBool::new(false);
    let mut hashes = vec![0; count as usize * 20];
    std::thread::scope(|scope| -> io::Result<()> {
        let concurrency = options.threads.min(count as usize);
        let (tx, rx) = mpsc::sync_channel(concurrency);
        let mut workers = Vec::new();
        let mut result = Ok(());
        for _ in 0..concurrency {
            let tx = tx.clone();
            let next = &next;
            let canceled = &canceled;
            let worker = std::thread::Builder::new().spawn_scoped(scope, move || {
                let result = (|| -> io::Result<()> {
                    let mut buffer = vec![0; 64 * 1024];
                    let mut opened: Option<(usize, File)> = None;
                    while !canceled.load(Ordering::Relaxed) {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= count {
                            break;
                        }
                        let mut position = u64::from(index) * u64::from(options.piece_length);
                        let end = (position + u64::from(options.piece_length)).min(total);
                        let mut file_index = files
                            .partition_point(|file| file.offset + file.metadata.len() <= position);
                        let mut hasher = Sha1::new();
                        while position < end {
                            let entry = &files[file_index];
                            if opened.as_ref().map(|(i, _)| *i) != Some(file_index) {
                                // A source replaced by a FIFO must not block an offline command.
                                let file = OpenOptions::new()
                                    .read(true)
                                    .custom_flags(libc::O_NONBLOCK)
                                    .open(&entry.source)?;
                                unchanged(&entry.metadata, &file.metadata()?)?;
                                opened = Some((file_index, file));
                            }
                            let file = &opened.as_ref().expect("opened source").1;
                            let stop = end.min(entry.offset + entry.metadata.len());
                            while position < stop {
                                if canceled.load(Ordering::Relaxed) {
                                    return Ok(());
                                }
                                let length = (stop - position).min(buffer.len() as u64) as usize;
                                file.read_exact_at(&mut buffer[..length], position - entry.offset)?;
                                hasher.update(&buffer[..length]);
                                position += length as u64;
                            }
                            file_index += 1;
                        }
                        tx.send((index, <[u8; 20]>::from(hasher.finalize())))
                            .expect("hash receiver retained until workers join");
                    }
                    Ok(())
                })();
                if result.is_err() {
                    canceled.store(true, Ordering::Relaxed);
                }
                result
            });
            match worker {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    canceled.store(true, Ordering::Relaxed);
                    result = Err(error);
                    break;
                }
            }
        }
        drop(tx);
        for (completed, (index, digest)) in rx.iter().enumerate() {
            let start = index as usize * 20;
            hashes[start..start + 20].copy_from_slice(&digest);
            if result.is_ok() {
                result = progress(completed as u32 + 1, count);
                if result.is_err() {
                    canceled.store(true, Ordering::Relaxed);
                }
            }
        }
        for worker in workers {
            result = result.and(worker.join().expect("hash worker panicked"));
        }
        result
    })?;
    Ok(hashes)
}

fn unchanged(before: &Metadata, after: &Metadata) -> io::Result<()> {
    if !after.is_file()
        || before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
        || before.modified()? != after.modified()?
    {
        return Err(invalid(
            "source changed while creating torrent; keep the payload unchanged",
        ));
    }
    Ok(())
}

fn validate_options(options: &Options) -> io::Result<()> {
    if !options.piece_length.is_power_of_two()
        || !(1 << 15..=1 << 28).contains(&options.piece_length)
    {
        return Err(invalid(
            "piece length must be a power of two from 32 KiB through 256 MiB",
        ));
    }
    if options.threads == 0 {
        return Err(invalid("thread count must be nonzero"));
    }
    if options
        .creation_date
        .is_some_and(|date| date > i64::MAX as u64)
    {
        return Err(invalid("creation date exceeds the bencode integer range"));
    }
    if options.announce.iter().any(Vec::is_empty) {
        return Err(invalid("tracker tiers must not be empty"));
    }
    for url in options.announce.iter().flatten() {
        validate_url(url, true)?;
    }
    for url in &options.web_seeds {
        validate_url(url, false)?;
    }
    Ok(())
}

fn validate_url(text: &str, tracker: bool) -> io::Result<()> {
    let url = url::Url::parse(text).map_err(invalid)?;
    if url.host_str().is_none()
        || !matches!(url.scheme(), "http" | "https") && !(tracker && url.scheme() == "udp")
    {
        return Err(invalid(
            "expected an HTTP(S) web seed or HTTP(S)/UDP tracker URL",
        ));
    }
    Ok(())
}

fn component(bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty()
        || bytes == b"."
        || bytes == b".."
        || bytes.contains(&b'/')
        || bytes.contains(&0)
    {
        return Err(invalid("invalid torrent path component"));
    }
    Ok(())
}

fn string(bytes: &[u8]) -> Node<'_> {
    Node::new(Value::Bytes(bytes))
}
fn integer(value: i64) -> Node<'static> {
    Node::new(Value::Integer(value))
}
fn invalid(message: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
