//! Metainfo parsing and fixed admission policy.
use super::{AdmissionError, AdmissionLimit, Profile};
use crate::protocol::{
    bencode,
    metainfo::{Layout, Metainfo, MetainfoView, OwnedMetainfo},
};

impl Profile {
    fn metainfo_limits(&self) -> bencode::Limits {
        bencode::Limits {
            max_input_len: super::MAX_META,
            max_string_len: super::MAX_META,
            max_output_len: super::MAX_META,
            max_nodes: 2_000_000,
            max_depth: 64,
        }
    }
    pub fn parse<'a>(&self, bytes: &'a [u8]) -> Result<Metainfo<'a>, AdmissionError> {
        let meta =
            Metainfo::parse(bytes, self.metainfo_limits()).map_err(AdmissionError::Metainfo)?;
        self.validate_metainfo(meta.view())?;
        Ok(meta)
    }
    pub fn parse_owned(&self, bytes: &[u8]) -> Result<OwnedMetainfo, AdmissionError> {
        let meta = OwnedMetainfo::parse(bytes, self.metainfo_limits())
            .map_err(AdmissionError::Metainfo)?;
        self.validate_metainfo(meta.view())?;
        Ok(meta)
    }
    pub(in crate::engine) fn parse_info(
        &self,
        bytes: &[u8],
    ) -> Result<crate::protocol::metainfo::InfoMetainfo, AdmissionError> {
        let info = crate::protocol::metainfo::InfoMetainfo::parse(bytes, self.metainfo_limits())
            .map_err(AdmissionError::Metainfo)?;
        self.validate_layout(info.layout())?;
        Ok(info)
    }
    pub(in crate::engine) fn validate_metainfo(
        &self,
        meta: MetainfoView<'_>,
    ) -> Result<(), AdmissionError> {
        if meta.v2_hashes().len() * 32 > super::MAX_META {
            return Err(AdmissionError::Metainfo(
                crate::protocol::metainfo::Error::MetadataLimit,
            ));
        }
        self.validate_layout(meta.layout())
    }
    pub(in crate::engine) fn validate_layout(
        &self,
        layout: &Layout<'_>,
    ) -> Result<(), AdmissionError> {
        for (exceeded, resource) in [
            (
                layout.piece_count() > super::MAX_PIECES,
                AdmissionLimit::Pieces,
            ),
            (
                layout.piece_length() > super::MAX_PIECE_LEN,
                AdmissionLimit::PieceBytes,
            ),
            (
                layout
                    .files()
                    .iter()
                    .filter(|file| file.kind() == crate::protocol::metainfo::FileKind::Data)
                    .count()
                    > super::MAX_FILES,
                AdmissionLimit::Files,
            ),
        ] {
            if exceeded {
                return Err(AdmissionError::ResourceLimit(resource));
            }
        }
        Ok(())
    }
}
