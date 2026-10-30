use super::{Destination, TorrentOptions, TorrentSource};
use std::{io, time::Duration};

impl TorrentSource {
    pub(in crate::engine) fn info_hash(&self) -> crate::protocol::identity::InfoHashV1 {
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
            peers: Vec::new(),
            diagnostics: false,
            log_peer_endpoints: false,
            no_progress_timeout: Duration::from_secs(60),
            persistence: Default::default(),
        }
    }
    pub(in crate::engine) fn validate(&self) -> io::Result<()> {
        self.profile.validate()?;
        if let TorrentSource::Metainfo(meta) = &self.source {
            self.profile.validate_layout(meta.layout())?;
            selection(meta, &self.settings)?;
        }
        Ok(())
    }
}

pub(super) fn selection(
    meta: &crate::protocol::metainfo::OwnedMetainfo,
    settings: &crate::engine::settings::TorrentSettings,
) -> io::Result<crate::engine::selection::FileSelection> {
    if settings.file_priorities.is_empty() {
        Ok(crate::engine::selection::FileSelection::all(meta.layout()))
    } else {
        crate::engine::selection::FileSelection::new(meta.layout(), &settings.file_priorities)
    }
}
