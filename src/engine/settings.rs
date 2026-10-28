//! The durable per-torrent settings shared by the core, daemon and clients.
use crate::engine::{bandwidth, selection::FilePriority};
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TorrentSettings {
    /// Bytes/second; zero means unlimited.
    pub download_limit: u64,
    pub upload_limit: u64,
    /// Zero disables the corresponding seed stop rule; time is seconds.
    pub seed_ratio: f64,
    pub seed_time: u64,
    /// Empty selects every non-padding file at normal priority.
    pub file_priorities: Vec<FilePriority>,
    pub trackers: bool,
    pub dht: bool,
    pub pex: bool,
    pub web_seeds: bool,
}
impl Default for TorrentSettings {
    fn default() -> Self {
        Self {
            download_limit: 0,
            upload_limit: 0,
            seed_ratio: 0.0,
            seed_time: 0,
            file_priorities: Vec::new(),
            trackers: true,
            dht: true,
            pex: true,
            web_seeds: true,
        }
    }
}
impl TorrentSettings {
    pub fn validate(&self) -> io::Result<()> {
        if !self.seed_ratio.is_finite() || self.seed_ratio < 0.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seed_ratio must be finite and nonnegative",
            ));
        }
        Ok(())
    }
    pub(in crate::engine) fn bandwidth(&self) -> bandwidth::Limits {
        bandwidth::Limits {
            down: self.download_limit,
            up: self.upload_limit,
        }
    }
}
