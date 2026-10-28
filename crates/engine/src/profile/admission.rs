//! Metainfo parsing and fixed admission policy.
use super::{AdmissionError, AdmissionLimit, Profile};
use protocol::{
    bencode,
    metainfo::{Layout, Metainfo, OwnedMetainfo},
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
        self.validate(meta.layout())?;
        Ok(meta)
    }
    pub fn parse_owned(&self, bytes: &[u8]) -> Result<OwnedMetainfo, AdmissionError> {
        let meta = OwnedMetainfo::parse(bytes, self.metainfo_limits())
            .map_err(AdmissionError::Metainfo)?;
        self.validate(meta.layout())?;
        Ok(meta)
    }
    fn validate(&self, layout: &Layout<'_>) -> Result<(), AdmissionError> {
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
                layout.files().len() > super::MAX_FILES,
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
