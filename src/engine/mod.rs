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
