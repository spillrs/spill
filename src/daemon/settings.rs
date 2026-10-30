use serde::{Deserialize, Serialize};
use spill::engine::{selection::FilePriority, settings::TorrentSettings};
use std::io;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub download_limit: u64,
    pub upload_limit: u64,
    pub seed_ratio: f64,
    pub seed_time: u64,
    pub file_priorities: Vec<FilePriority>,
    pub trackers: bool,
    pub dht: bool,
    pub pex: bool,
    pub web_seeds: bool,
}
impl Default for Settings {
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
impl Settings {
    pub fn transfer_settings(&self) -> TorrentSettings {
        TorrentSettings {
            download_limit: self.download_limit,
            upload_limit: self.upload_limit,
            file_priorities: self.file_priorities.clone(),
            trackers: self.trackers,
            dht: self.dht,
            pex: self.pex,
            web_seeds: self.web_seeds,
        }
    }
    pub fn validate(&self) -> io::Result<()> {
        if !self.seed_ratio.is_finite() || self.seed_ratio < 0.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seed_ratio must be finite and nonnegative",
            ));
        }
        Ok(())
    }
}
