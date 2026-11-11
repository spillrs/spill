use std::io;

pub(super) fn request_timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "block request deadline")
}
pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
pub(super) fn invalid_data_text(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
pub(super) fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
