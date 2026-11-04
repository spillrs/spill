//! Process adapter for opt-in interop harnesses; uses only the public engine API.
use serde::Deserialize;
use serde_json::json;
use spill::engine::{
    profile::Profile,
    provisioning::DestinationBinding,
    selection::FilePriority,
    session::{Limits, NetworkOptions, PeerEncryption, Session},
    torrent::{Destination, RecoveryState, RunPolicy, TorrentOptions, TorrentSource},
};
use std::{
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    torrent: PathBuf,
    output: PathBuf,
    recovery: PathBuf,
    stop: PathBuf,
    listen: SocketAddr,
    peers: Vec<SocketAddr>,
    mode: String,
    upload_limit: u64,
    download_limit: u64,
    file_priorities: Vec<FilePriority>,
    #[serde(default)]
    web_seeds: bool,
}

fn save(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.with_added_extension("tmp");
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)
}
fn load(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let path = std::env::args_os().nth(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: interop-engine CONFIG.json",
        )
    })?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?).map_err(io::Error::other)?;
    if !matches!(config.mode.as_str(), "download" | "seed" | "recheck") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mode must be download, seed or recheck",
        ));
    }
    let root = std::fs::canonicalize(&config.output)?;
    std::fs::create_dir_all(&config.recovery)?;
    let binding = config.recovery.join("binding.json");
    let resume = config.recovery.join("resume.bin");
    let destination = if let Some(bytes) = load(&binding)? {
        let (saved_root, binding): (PathBuf, DestinationBinding) =
            serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        if saved_root != root {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery belongs to another output directory",
            ));
        }
        Destination::Recover {
            root: root.clone(),
            state: RecoveryState {
                binding,
                resume: load(&resume)?,
                completed_announces: Vec::new(),
            },
        }
    } else if config.mode == "seed" {
        Destination::Adopt(root.clone())
    } else {
        Destination::Open(root.clone())
    };
    let meta = Profile::default().parse_owned(&std::fs::read(&config.torrent)?)?;
    let mut options = TorrentOptions::new(TorrentSource::Metainfo(meta), destination);
    options.peers = config.peers;
    options.diagnostics = true;
    options.settings.trackers = false;
    options.settings.dht = false;
    options.settings.pex = false;
    options.settings.web_seeds = config.web_seeds;
    options.settings.upload_limit = config.upload_limit;
    options.settings.download_limit = config.download_limit;
    options.settings.file_priorities = config.file_priorities;
    options.no_progress_timeout = Duration::from_secs(300);
    options.persistence.binding = Some(Box::new(move |record| {
        save(
            &binding,
            &serde_json::to_vec(&(&root, record)).map_err(io::Error::other)?,
        )
    }));
    options.persistence.checkpoint = Some(Box::new(move |bytes| save(&resume, &bytes)));
    let session = if config.mode == "recheck" {
        options.run = RunPolicy::Recheck;
        Session::offline(Limits::default())
    } else {
        Session::bind(
            NetworkOptions {
                listen: config.listen,
                utp: false,
                peer_encryption: PeerEncryption::Allowed,
                allow_local_network: true,
                ..Default::default()
            },
            Limits::default(),
        )
        .await?
    };
    let handle = session.admit(options)?;
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    loop {
        timer.tick().await;
        let status = handle.snapshot();
        let p = &status.progress;
        let peers: Vec<_> = status.diagnostics.as_ref().into_iter().flat_map(|d| &d.peers).map(|peer| json!({
            "endpoint": peer.endpoint, "client": peer.client, "transport": peer.transport,
            "fast": peer.detail.fast_extension, "choking": peer.detail.local_choking,
            "upload_slot": peer.detail.upload_slot, "authorized": peer.detail.authorized_requests,
            "assigned": peer.detail.assigned_requests, "retiring": peer.detail.retiring,
        })).collect();
        println!(
            "{}",
            json!({
                "event": "status", "identity": handle.info_hash().to_string(), "listen": session.local_addr(),
                "phase": format!("{:?}", p.phase), "ready": p.ready, "recovered": p.recovered,
                "completed": p.completed, "full_complete": p.full_complete, "joined": p.joined,
                "verified_bytes": p.verified_bytes, "verified_pieces": p.verified_pieces,
                "wanted_bytes": p.wanted_bytes, "wanted_verified_bytes": p.wanted_verified_bytes,
                "received_bytes": p.processed_payload_bytes, "uploaded_bytes": p.written_payload_bytes,
                "hash_failed_pieces": p.hash_failed_pieces, "peers": peers,
            })
        );
        if config.stop.exists() || handle.is_finished() {
            break;
        }
    }
    let shutdown = session.shutdown().await;
    let outcome = handle.outcome().await;
    println!(
        "{}",
        json!({"event": "outcome", "termination": format!("{:?}", outcome.termination),
        "cleanup_complete": outcome.cleanup_complete, "full_complete": outcome.full_complete,
        "recheck_finished": outcome.recheck_finished, "error": outcome.error.as_ref().map(ToString::to_string)})
    );
    shutdown.and(handle.join().await)
}
