//! cargo run --no-default-features --example progress -- OUTPUT RECOVERY FILE.torrent...
//! Rerunning after an interruption resumes partial downloads from RECOVERY.
use spill::engine::{
    Destination, DhtOptions, Limits, NetworkOptions, RecoveryState, RunPolicy, Session,
    TorrentOptions, TorrentSource, parse_owned,
};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

/// Replace atomically so an interrupted write never leaves a torn record.
/// Callbacks run on blocking threads, so synchronous I/O is fine here.
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
    tokio::fs::create_dir_all(&args[0]).await?;
    let output = tokio::fs::canonicalize(&args[0]).await?;
    let recovery = PathBuf::from(&args[1]);
    let mut sources = Vec::new();
    for path in &args[2..] {
        sources.push(parse_owned(&tokio::fs::read(path).await?)?);
    }
    tokio::fs::create_dir_all(&recovery).await?;
    let session = Session::bind(
        NetworkOptions {
            dht: Some(DhtOptions::default()),
            ..Default::default()
        },
        Limits::default(),
    )
    .await?;
    let result = async {
        let mut handles = Vec::new();
        for mut meta in sources {
            let record = recovery.join(meta.info_hash().to_string());
            let state_path = record.with_extension("recovery");
            let meta_path = record.with_extension("torrent");
            if let Some(saved_meta) = load(&meta_path).await? {
                meta = parse_owned(&saved_meta)?;
            }
            // Only a recorded binding authorizes reopening partial payload;
            // without one, existing files are adopted only if already complete.
            let existing: Option<(PathBuf, RecoveryState)> = match load(&state_path).await? {
                Some(bytes) => Some(serde_json::from_slice(&bytes).map_err(io::Error::other)?),
                None => None,
            };
            let destination = match &existing {
                Some((root, state)) => {
                    if *root != output {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "{} records a download in {}",
                                record.display(),
                                root.display()
                            ),
                        ));
                    }
                    Destination::Recover {
                        root: root.clone(),
                        state: state.clone(),
                    }
                }
                None => Destination::Open(output.clone()),
            };
            let persisted = Arc::new(Mutex::new(existing.map(|(_, state)| state)));
            let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta), destination);
            options.run = RunPolicy::UntilComplete;
            options.persistence.resolved = Some(Box::new(move |resolved| {
                save(&meta_path, resolved.source_bytes())
            }));
            let save_state = {
                let output = output.clone();
                let state_path = state_path.clone();
                Arc::new(move |state: &RecoveryState| {
                    save(
                        &state_path,
                        &serde_json::to_vec(&(&output, state)).map_err(io::Error::other)?,
                    )
                })
            };
            let state = persisted.clone();
            let writer = save_state.clone();
            options.persistence.binding = Some(Box::new(move |binding| {
                let mut guard = state.lock().unwrap();
                let current = guard.insert(RecoveryState {
                    binding,
                    resume: None,
                    completed_announces: Vec::new(),
                });
                writer(current)
            }));
            let state = persisted.clone();
            let writer = save_state.clone();
            options.persistence.checkpoint = Some(Box::new(move |bytes| {
                let mut guard = state.lock().unwrap();
                let current = guard.as_mut().expect("binding recorded");
                current.resume = Some(bytes);
                writer(current)
            }));
            let state = persisted;
            let writer = save_state;
            options.persistence.completion_announce = Some(Box::new(move |hash, delivered| {
                let mut guard = state.lock().unwrap();
                let current = guard.as_mut().expect("binding recorded");
                current.record_announce(hash, delivered);
                writer(current)
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
            handle.outcome().await.require_complete()?;
        }
        Ok(())
    }
    .await;
    let shutdown = session.shutdown().await;
    result.and(shutdown)
}
