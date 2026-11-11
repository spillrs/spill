//! Transport failures retain the core's local/remote classification.
use crate::engine::core::fault::PeerError;
use std::io;
use tokio::task::JoinError;

fn remote(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), PeerError::Remote(error))
}
fn local(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), PeerError::Local(error))
}
pub(super) fn handshake_expired() -> io::Error {
    remote(io::Error::new(
        io::ErrorKind::TimedOut,
        "peer handshake deadline",
    ))
}
pub(super) fn io_deadline() -> io::Error {
    remote(io::Error::new(io::ErrorKind::TimedOut, "peer I/O deadline"))
}
pub(super) fn invalid(message: &'static str) -> io::Error {
    local(io::Error::new(io::ErrorKind::InvalidInput, message))
}
pub(super) fn busy(message: &'static str) -> io::Error {
    remote(io::Error::new(io::ErrorKind::WouldBlock, message))
}
pub(super) fn task_failed(error: JoinError) -> io::Error {
    local(io::Error::other(error))
}
pub(super) fn event_receiver_closed() -> io::Error {
    local(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "peer event receiver closed",
    ))
}
pub(super) fn socket_failure(error: io::Error) -> io::Error {
    match error.kind() {
        io::ErrorKind::InvalidInput | io::ErrorKind::Other | io::ErrorKind::OutOfMemory => {
            local(error)
        }
        _ => remote(error),
    }
}
pub(super) fn protocol(message: &'static str) -> io::Error {
    remote(io::Error::new(io::ErrorKind::InvalidData, message))
}
pub(super) fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    remote(io::Error::new(io::ErrorKind::InvalidData, error))
}
pub(super) fn invalid_message(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    local(io::Error::new(io::ErrorKind::InvalidData, error))
}
