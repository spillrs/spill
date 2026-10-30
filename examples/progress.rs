//! cargo run --no-default-features --example progress -- OUTPUT RECOVERY FILE.torrent...
//! Rerunning after an interruption resumes partial downloads from RECOVERY.
use spill::engine::{
    profile::Profile,
    provisioning::DestinationBinding,
    session::{DhtOptions, Limits, NetworkOptions, Session, dht::default_routers},
    torrent::{Destination, RecoveryState, RunPolicy, TorrentOptions, TorrentSource},
};
use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};

/// Replace atomically so an interrupted write never leaves a torn record.
/// Callbacks run on blocking threads, so synchronous I/O is fine here; each
/// record keeps its own temporary file because they can be saved concurrently.
fn save(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.with_added_extension("tmp");
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)
}

async fn load(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() < 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: progress OUTPUT RECOVERY FILE.torrent...",
        ));
    }
    // A binding authorizes writes only within the directory it was recorded for.
    let output = tokio::fs::canonicalize(&args[0]).await?;
    let recovery = PathBuf::from(&args[1]);
    let mut sources = Vec::new();
    for path in &args[2..] {
        sources.push(Profile::default().parse_owned(&tokio::fs::read(path).await?)?);
    }
    tokio::fs::create_dir_all(&recovery).await?;
    let session = Session::bind(
        NetworkOptions {
            dht: Some(DhtOptions {
                saved: None,
                routers: default_routers(),
            }),
            ..Default::default()
        },
        Limits::default(),
    )
    .await?;
    let result = async {
        let mut handles = Vec::new();
        for meta in sources {
            let record = recovery.join(meta.info_hash().to_string());
            let binding = record.with_extension("binding");
            let resume = record.with_extension("resume");
            let announce = record.with_extension("announce");
            // Only a recorded binding authorizes reopening partial payload;
            // without one, existing files are adopted only if already complete.
            let destination = match load(&binding).await? {
                Some(bytes) => {
                    let (root, binding): (PathBuf, DestinationBinding) =
                        serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                    if root != output {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "{} records a download in {}",
                                record.display(),
                                root.display()
                            ),
                        ));
                    }
                    let completed_announce = match load(&announce).await? {
                        Some(bytes) => {
                            Some(serde_json::from_slice(&bytes).map_err(io::Error::other)?)
                        }
                        None => None,
                    };
                    Destination::Recover {
                        root,
                        state: RecoveryState {
                            binding,
                            resume: load(&resume).await?,
                            completed_announce,
                        },
                    }
                }
                None => Destination::Open(output.clone()),
            };
            let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta), destination);
            options.run = RunPolicy::UntilComplete;
            let root = output.clone();
            options.persistence.binding = Some(Box::new(move |record| {
                save(
                    &binding,
                    &serde_json::to_vec(&(&root, record)).map_err(io::Error::other)?,
                )
            }));
            options.persistence.checkpoint = Some(Box::new(move |bytes| save(&resume, &bytes)));
            options.persistence.completion_announce = Some(Box::new(move |delivered| {
                save(
                    &announce,
                    &serde_json::to_vec(&delivered).map_err(io::Error::other)?,
                )
            }));
            handles.push(session.admit(options)?);
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        while handles.iter().any(|h| !h.is_finished()) {
            tick.tick().await;
            for handle in &handles {
                let p = handle.snapshot().progress;
                println!(
                    "{} {:?} {}/{} wanted bytes",
                    handle.info_hash(),
                    p.phase,
                    p.wanted_verified_bytes,
                    p.wanted_bytes
                );
            }
        }
        for handle in handles {
            handle.join().await?;
            handle.wait_complete().await?;
        }
        Ok(())
    }
    .await;
    let shutdown = session.shutdown().await;
    result.and(shutdown)
}
