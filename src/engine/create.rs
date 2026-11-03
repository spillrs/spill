//! Read-only v1, v2 and hybrid metainfo creation, independent of sessions and networking.

use crate::protocol::{
    bencode::{self, Limits, Node, Value},
    identity::TorrentIdentity,
    merkle,
    metainfo::hash::PieceAccumulator,
};
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

use super::profile::{AdmissionError, AdmissionLimit, MAX_FILES, MAX_META, MAX_PIECES, Profile};

/// Metainfo format. V1 preserves mktorrent-compatible file ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    V1,
    V2,
    Hybrid,
}

/// Options shared by the offline CLI and embedded callers.
#[derive(Clone, Debug)]
pub struct Options {
    /// Torrent format; defaults to v1. V2 and hybrid use BEP 52 file-tree ordering.
    pub format: Format,
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
            format: Format::V1,
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
    /// Parsed identity: SHA-1 for v1, SHA-256 for v2, both for hybrid.
    pub info_hash: TorrentIdentity,
    pub name: OsString,
    /// Logical payload size, including v2/hybrid alignment padding.
    pub total_length: u64,
    pub piece_count: u32,
}

/// Hash a file or directory and encode a v1, v2 or hybrid `.torrent` document.
///
/// This is blocking work; asynchronous callers should use `spawn_blocking`.
/// V1 sorts whole path bytes; v2 and hybrid sort byte components. Hidden and empty
/// files are included, and empty
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
    if options.format == Format::Hybrid
        && multi_file
        && files.iter().any(|file| file.path[0] == b".pad")
    {
        return Err(invalid(
            "hybrid torrents cannot contain a top-level .pad entry; it is reserved for BEP 47 padding",
        ));
    }
    if files.is_empty() {
        return Err(invalid("source directory contains no files"));
    }
    files.sort_unstable_by(|a, b| match options.format {
        // Whole-path ordering preserves mktorrent output: '/' sorts after '-'.
        Format::V1 => a
            .source
            .as_os_str()
            .as_bytes()
            .cmp(b.source.as_os_str().as_bytes()),
        Format::V2 | Format::Hybrid => a.path.cmp(&b.path),
    });
    let last_nonempty = files.iter().rposition(|f| f.metadata.len() != 0);
    let mut total = 0u128;
    let p = u128::from(options.piece_length);
    for (index, file) in files.iter_mut().enumerate() {
        file.offset = total as u64;
        total += u128::from(file.metadata.len());
        if options.format != Format::V1 && file.metadata.len() != 0 && Some(index) != last_nonempty
        {
            file.padding = ((p - total % p) % p) as u64;
            total += u128::from(file.padding);
        }
    }
    if total > i64::MAX as u128 {
        return Err(invalid("payload is too large for metainfo"));
    }
    let total_length = total as u64;
    let count = total_length.div_ceil(u64::from(options.piece_length));
    let path_bytes: u64 = if multi_file {
        files
            .iter()
            .flat_map(|f| &f.path)
            .map(|p| p.len() as u64)
            .sum()
    } else {
        name.as_bytes().len() as u64
    };
    if count > u64::from(MAX_PIECES)
        || (options.format != Format::V2 && count * 20 > MAX_META as u64)
        || (options.format != Format::V1 && count * 64 + path_bytes > MAX_META as u64)
    {
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
    let padding_names: Vec<_> = files.iter().map(|file| file.padding.to_string()).collect();
    if options.format != Format::V2 {
        info.insert(b"pieces".as_slice(), string(&hashes.0));
        if multi_file {
            info.insert(
                b"files".as_slice(),
                Node::new(Value::List(
                    files
                        .iter()
                        .enumerate()
                        .flat_map(|(index, file)| {
                            let real = Node::new(Value::Dictionary(BTreeMap::from([
                                (b"length".as_slice(), integer(file.metadata.len() as i64)),
                                (
                                    b"path".as_slice(),
                                    Node::new(Value::List(
                                        file.path.iter().map(|p| string(p)).collect(),
                                    )),
                                ),
                            ])));
                            let padding = (file.padding != 0).then(|| {
                                Node::new(Value::Dictionary(BTreeMap::from([
                                    (b"attr".as_slice(), string(b"p")),
                                    (b"length".as_slice(), integer(file.padding as i64)),
                                    (
                                        b"path".as_slice(),
                                        Node::new(Value::List(vec![
                                            string(b".pad"),
                                            string(padding_names[index].as_bytes()),
                                        ])),
                                    ),
                                ])))
                            });
                            std::iter::once(real).chain(padding)
                        })
                        .collect(),
                )),
            );
        } else {
            info.insert(b"length".as_slice(), integer(total_length as i64));
        }
    }
    let mut roots = Vec::new();
    let mut layers = BTreeMap::new();
    if options.format != Format::V1 {
        for file in &files {
            let start = (file.offset / u64::from(options.piece_length)) as usize;
            let count = file
                .metadata
                .len()
                .div_ceil(u64::from(options.piece_length)) as usize;
            let layer = &hashes.1[start..start + count];
            roots.push(if count == 0 {
                None
            } else if count == 1 {
                Some(layer[0])
            } else {
                Some(merkle::root_from_layer(
                    layer,
                    (options.piece_length / merkle::BLOCK_LENGTH as u32).ilog2(),
                ))
            });
        }
        let tree = if multi_file {
            file_tree(&files, &roots, 0)
        } else {
            Node::new(Value::Dictionary(BTreeMap::from([(
                name.as_bytes(),
                file_leaf(files[0].metadata.len(), roots[0].as_ref()),
            )])))
        };
        for (file, root) in files.iter().zip(&roots) {
            if file.metadata.len() > u64::from(options.piece_length) {
                let start = (file.offset / u64::from(options.piece_length)) as usize;
                let count = file
                    .metadata
                    .len()
                    .div_ceil(u64::from(options.piece_length)) as usize;
                layers.insert(
                    root.as_ref().expect("nonempty root").as_slice(),
                    string(hashes.1[start..start + count].as_flattened()),
                );
            }
        }
        info.insert(b"meta version".as_slice(), integer(2));
        info.insert(b"file tree".as_slice(), tree);
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
    let mut document = BTreeMap::from([
        (b"info".as_slice(), info),
        (
            b"created by".as_slice(),
            string(concat!("spill ", env!("CARGO_PKG_VERSION")).as_bytes()),
        ),
    ]);
    if options.format != Format::V1 {
        document.insert(
            b"piece layers".as_slice(),
            Node::new(Value::Dictionary(layers)),
        );
    }
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
    let parsed = Profile::default()
        .parse_owned(&metainfo)
        .map_err(|error| match error {
            AdmissionError::ResourceLimit(AdmissionLimit::Pieces)
            | AdmissionError::Metainfo(crate::protocol::metainfo::Error::MetadataLimit) => {
                invalid(format!("{error}; choose a larger piece length"))
            }
            _ => invalid(error),
        })?;
    let info_hash = parsed.info_hash();
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
    padding: u64,
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
            padding: 0,
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
) -> io::Result<(Vec<u8>, Vec<[u8; 32]>)> {
    progress(0, count)?;
    let next = AtomicU32::new(0);
    let canceled = AtomicBool::new(false);
    let mut hashes = vec![
        0;
        if options.format != Format::V2 {
            count as usize * 20
        } else {
            0
        }
    ];
    let mut v2_hashes = vec![
        [0; 32];
        if options.format != Format::V1 {
            count as usize
        } else {
            0
        }
    ];
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
                        let entry = &files[file_index];
                        let data_end = end.min(entry.offset + entry.metadata.len());
                        let mut hasher = PieceAccumulator::new(
                            (data_end - position) as usize,
                            options.format != Format::V2,
                            options.format != Format::V1,
                        );
                        let blocks = entry
                            .metadata
                            .len()
                            .min(u64::from(options.piece_length))
                            .div_ceil(merkle::BLOCK_LENGTH as u64)
                            .next_power_of_two() as u32;
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
                            if options.format != Format::V1 {
                                buffer.fill(0);
                                while position < end {
                                    let length = (end - position).min(buffer.len() as u64) as usize;
                                    hasher.update(&buffer[..length]);
                                    position += length as u64;
                                }
                            }
                            file_index += 1;
                        }
                        let (sha1, leaves) = hasher.finish();
                        let v2 = (options.format != Format::V1).then(|| {
                            merkle::piece_hash(&leaves, blocks * merkle::BLOCK_LENGTH as u32)
                        });
                        tx.send((index, sha1, v2))
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
        for (completed, (index, sha1, v2)) in rx.iter().enumerate() {
            if let Some(digest) = sha1 {
                let start = index as usize * 20;
                hashes[start..start + 20].copy_from_slice(&digest);
            }
            if let Some(digest) = v2 {
                v2_hashes[index as usize] = digest;
            }
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
    Ok((hashes, v2_hashes))
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

fn file_leaf(length: u64, root: Option<&[u8; 32]>) -> Node<'_> {
    let mut leaf = BTreeMap::from([(b"length".as_slice(), integer(length as i64))]);
    if let Some(root) = root {
        leaf.insert(b"pieces root".as_slice(), string(root));
    }
    Node::new(Value::Dictionary(BTreeMap::from([(
        b"".as_slice(),
        Node::new(Value::Dictionary(leaf)),
    )])))
}

fn file_tree<'a>(files: &'a [Entry], roots: &'a [Option<[u8; 32]>], depth: usize) -> Node<'a> {
    let mut tree = BTreeMap::new();
    let mut start = 0;
    while start < files.len() {
        let key = files[start].path[depth].as_slice();
        let end = start + files[start..].partition_point(|file| file.path[depth].as_slice() == key);
        let child = if files[start].path.len() == depth + 1 {
            file_leaf(files[start].metadata.len(), roots[start].as_ref())
        } else {
            file_tree(&files[start..end], &roots[start..end], depth + 1)
        };
        tree.insert(key, child);
        start = end;
    }
    Node::new(Value::Dictionary(tree))
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
