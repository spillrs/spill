//! Bounded torrent tasks with explicit caller consent for networking.
//! Identity comes from original metainfo bytes; only hash-verified pieces are served.
//! Verification, durable storage, socket writes and remote receipt are distinct.
//! Shutdown joins peer tasks and storage work before releasing a destination.

#![warn(unreachable_pub)]

pub(crate) mod bandwidth;
pub(crate) mod core;
pub(crate) mod dht;
pub(crate) mod discovery;
mod identity;
mod network;
pub(crate) mod peer_io;
pub mod profile;
#[cfg(unix)]
pub mod provisioning;
pub mod selection;
pub mod session;
pub mod settings;
pub(crate) mod storage;
pub mod torrent;
mod transfer_stats;
mod webseed;
