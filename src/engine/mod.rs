//! Bounded torrent tasks with explicit caller consent for networking.
//! Identity comes from original metainfo bytes; only hash-verified pieces are served.
//! Verification, durable storage, socket writes and remote receipt are distinct.
//! Shutdown joins peer tasks and storage work before releasing a destination.

#![warn(unreachable_pub)]

pub(in crate::engine) mod bandwidth;
pub(in crate::engine) mod core;
pub(in crate::engine) mod dht;
pub(in crate::engine) mod discovery;
mod identity;
mod network;
pub(in crate::engine) mod peer_io;
pub mod profile;
#[cfg(unix)]
pub mod provisioning;
pub mod selection;
pub mod session;
pub mod settings;
pub(in crate::engine) mod storage;
pub mod torrent;
mod transfer_stats;
mod webseed;

/// Run blocking work owned by `slot`: if this future is dropped, the running job
/// stays in the slot so cleanup can still join it with `join_blocking`.
pub(in crate::engine) async fn run_blocking<T: Send + 'static>(
    slot: &mut Option<tokio::task::JoinHandle<std::io::Result<T>>>,
    job: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
    let running = slot.insert(tokio::task::spawn_blocking(job));
    let result = running.await.map_err(std::io::Error::other).and_then(|r| r);
    *slot = None;
    result
}

pub(in crate::engine) async fn join_blocking<T>(
    slot: &mut Option<tokio::task::JoinHandle<std::io::Result<T>>>,
) -> std::io::Result<()> {
    let Some(running) = slot else {
        return Ok(());
    };
    let result = running.await.map_err(std::io::Error::other).and_then(|r| r);
    *slot = None;
    result.map(drop)
}

/// Download wanted content and join cleanup in a caller-owned Tokio runtime.
/// Uses an ephemeral IPv4 listener, default DHT routers, no port mapping and no
/// LAN permission. Dropping this future requests stop but cannot join cleanup;
/// use an explicit Session when cleanup proof is needed after a timeout.
pub async fn download(
    source: torrent::TorrentSource,
    destination: torrent::Destination,
) -> std::io::Result<()> {
    let session = session::Session::bind(
        session::NetworkOptions {
            dht: Some(session::DhtOptions {
                saved: None,
                routers: session::dht::default_routers(),
            }),
            ..Default::default()
        },
        session::Limits::default(),
    )
    .await?;
    let result = async {
        let mut options = torrent::TorrentOptions::new(source, destination);
        options.run = torrent::RunPolicy::UntilComplete;
        let handle = session.admit(options)?;
        handle.join().await?;
        handle.wait_complete().await
    }
    .await;
    let shutdown = session.shutdown().await;
    result.and(shutdown)
}
