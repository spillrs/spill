//! Single-user Linux and macOS configuration. Torrent defaults use the engine's settings schema.
use engine::{profile::Profile, settings::TorrentSettings};
use serde::Deserialize;
use std::{
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub download_dir: PathBuf,
    pub control_socket: PathBuf,
    pub listen: SocketAddr,
    pub web_listen: Option<SocketAddr>,
    pub allow_local_network: bool,
    pub utp: bool,
    pub port_mapping: bool,
    #[serde(deserialize_with = "mapping_gateway")]
    pub port_mapping_gateway: Option<engine::session::port_map::Gateway>,
    pub peers: usize,
    pub cache_bytes: usize,
    pub write_queue_bytes: usize,
    pub active_downloads: usize,
    pub active_seeds: usize,
    pub dht_bootstrap: Vec<String>,
    pub defaults: TorrentSettings,
}
fn mapping_gateway<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<engine::session::port_map::Gateway>, D::Error> {
    let value = String::deserialize(deserializer)?;
    engine::session::port_map::Gateway::parse(&value)
        .map(Some)
        .map_err(serde::de::Error::custom)
}
pub fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}
#[cfg(target_os = "macos")]
fn state_dir() -> PathBuf {
    home().join("Library/Application Support/spill")
}
#[cfg(not(target_os = "macos"))]
fn state_dir() -> PathBuf {
    home().join(".local/state/spill")
}
#[cfg(target_os = "macos")]
pub fn socket() -> PathBuf {
    state_dir().join("spill.sock")
}
#[cfg(not(target_os = "macos"))]
pub fn socket() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".local/state/spill"))
        .join("spill.sock")
}
impl Default for Config {
    fn default() -> Self {
        Self {
            state_dir: state_dir(),
            download_dir: home().join("Downloads"),
            control_socket: socket(),
            listen: "0.0.0.0:51413".parse().unwrap(),
            web_listen: None,
            allow_local_network: false,
            utp: true,
            port_mapping: true,
            port_mapping_gateway: None,
            peers: Profile::DEFAULT.peers,
            cache_bytes: Profile::DEFAULT.cache_bytes,
            write_queue_bytes: Profile::DEFAULT.write_queue_bytes,
            active_downloads: 4,
            active_seeds: 16,
            dht_bootstrap: engine::session::dht::default_routers(),
            defaults: Default::default(),
        }
    }
}
impl Config {
    pub fn load(path: Option<&Path>) -> io::Result<Self> {
        let config: Self = match path {
            Some(path) => {
                let bytes = std::fs::read(path)?;
                if bytes.len() > 64 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "config exceeds 64 KiB",
                    ));
                }
                toml::from_str(std::str::from_utf8(&bytes).map_err(io::Error::other)?)
                    .map_err(io::Error::other)?
            }
            None => Self::default(),
        };
        config.defaults.validate()?;
        engine::session::dht::validate_routers(&config.dht_bootstrap)?;
        let buffer_range = engine::profile::BLOCK_LEN as usize..=1024 * 1024 * 1024;
        if !(1..=4096).contains(&config.peers)
            || !buffer_range.contains(&config.cache_bytes)
            || !buffer_range.contains(&config.write_queue_bytes)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid torrent resource limits",
            ));
        }
        if config.active_downloads > 64
            || config.active_seeds > 64
            || config.active_downloads + config.active_seeds == 0
            || config.active_downloads + config.active_seeds > 64
            || (config.defaults.dht && !config.listen.is_ipv4())
            || config.web_listen.is_some_and(|a| !a.ip().is_loopback())
            || !config.state_dir.is_absolute()
            || !config.download_dir.is_absolute()
            || !config.control_socket.is_absolute()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "absolute paths and a nonzero active cap are required",
            ));
        }
        Ok(config)
    }
    pub fn profile(&self) -> Profile {
        Profile {
            peers: self.peers,
            cache_bytes: self.cache_bytes,
            write_queue_bytes: self.write_queue_bytes,
        }
    }
}
