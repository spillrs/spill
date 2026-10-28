//! Peer closures are recoverable; local failures terminate the torrent.
use std::io;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PeerError {
    #[error("{0}")]
    Remote(#[source] io::Error),
    #[error("{0}")]
    Local(#[source] io::Error),
}

pub(crate) fn is_local(error: &io::Error) -> bool {
    !matches!(
        error.get_ref().and_then(|e| e.downcast_ref::<PeerError>()),
        Some(PeerError::Remote(_))
    )
}

pub(crate) mod peer {
    use super::*;
    use tokio::task::JoinError;

    fn remote(error: io::Error) -> io::Error {
        io::Error::new(error.kind(), PeerError::Remote(error))
    }
    fn local(error: io::Error) -> io::Error {
        io::Error::new(error.kind(), PeerError::Local(error))
    }
    pub(crate) fn handshake_expired() -> io::Error {
        remote(io::Error::new(
            io::ErrorKind::TimedOut,
            "peer handshake deadline",
        ))
    }
    pub(crate) fn io_deadline() -> io::Error {
        remote(io::Error::new(io::ErrorKind::TimedOut, "peer I/O deadline"))
    }
    pub(crate) fn invalid(message: &'static str) -> io::Error {
        local(io::Error::new(io::ErrorKind::InvalidInput, message))
    }
    pub(crate) fn busy(message: &'static str) -> io::Error {
        remote(io::Error::new(io::ErrorKind::WouldBlock, message))
    }
    pub(crate) fn task_failed(error: JoinError) -> io::Error {
        local(io::Error::other(error))
    }
    pub(crate) fn event_receiver_closed() -> io::Error {
        local(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "peer event receiver closed",
        ))
    }
    pub(crate) fn socket_failure(error: io::Error) -> io::Error {
        match error.kind() {
            io::ErrorKind::InvalidInput | io::ErrorKind::Other | io::ErrorKind::OutOfMemory => {
                local(error)
            }
            _ => remote(error),
        }
    }
    pub(crate) fn protocol(message: &'static str) -> io::Error {
        remote(io::Error::new(io::ErrorKind::InvalidData, message))
    }
    pub(crate) fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
        remote(io::Error::new(io::ErrorKind::InvalidData, error))
    }
    pub(crate) fn invalid_message(
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> io::Error {
        local(io::Error::new(io::ErrorKind::InvalidData, error))
    }
}
