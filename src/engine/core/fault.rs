//! Peer closures are recoverable; local failures terminate the torrent.
use std::io;

#[derive(Debug, thiserror::Error)]
pub(in crate::engine) enum PeerError {
    #[error("{0}")]
    Remote(#[source] io::Error),
    #[error("{0}")]
    Local(#[source] io::Error),
}

pub(in crate::engine) fn is_local(error: &io::Error) -> bool {
    !matches!(
        error.get_ref().and_then(|e| e.downcast_ref::<PeerError>()),
        Some(PeerError::Remote(_))
    )
}
