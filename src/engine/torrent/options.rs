use super::{Destination, TorrentOptions, TorrentSource};
use std::io;

impl TorrentSource {
    /// Parse and validate `.torrent` bytes under spill's admission limits.
    pub fn from_metainfo_bytes(
        bytes: &[u8],
    ) -> Result<Self, crate::engine::profile::AdmissionError> {
        crate::engine::profile::parse_owned(bytes).map(Self::Metainfo)
    }

    /// Parse a magnet URI using default protocol limits.
    pub fn from_magnet(uri: &str) -> io::Result<Self> {
        crate::protocol::magnet::Magnet::parse(uri, Default::default())
            .map(Self::Magnet)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    }

    pub(in crate::engine) fn info_hash(&self) -> crate::protocol::identity::TorrentIdentity {
        match self {
            Self::Metainfo(meta) => meta.info_hash(),
            Self::Magnet(magnet) => magnet.info_hash(),
        }
    }
}

impl TorrentOptions {
    pub fn new(source: TorrentSource, destination: Destination) -> Self {
        Self {
            source,
            destination,
            profile: Default::default(),
            settings: Default::default(),
            run: super::RunPolicy::Persistent,
            initial_peers: Vec::new(),
            diagnostics: false,
            log_peer_endpoints: false,
            no_progress_timeout: None,
            persistence: Default::default(),
        }
    }
    pub(in crate::engine) fn validate(&self) -> io::Result<()> {
        self.profile.validate()?;
        if let TorrentSource::Metainfo(meta) = &self.source {
            crate::engine::profile::validate_metainfo(meta.view())?;
            if meta.is_private() && !self.initial_peers.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "direct peers are disabled for private torrents",
                ));
            }
            crate::engine::selection::FileSelection::from_priorities(
                meta.layout(),
                &self.settings.file_priorities,
            )?;
        }
        Ok(())
    }
}
