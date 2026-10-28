//! Shared accept retries. State survives canceled waits; no work is spawned.
use std::{future::Future, io, time::Duration};
use tokio::time::Instant;
use tracing::warn;

const INITIAL_DELAY: Duration = Duration::from_millis(50);
const MAX_DELAY: Duration = Duration::from_secs(2);

pub(in crate::engine) struct AcceptBackoff {
    listener: &'static str,
    failures: u8,
    retry_at: Instant,
}
impl AcceptBackoff {
    pub(in crate::engine) fn new(listener: &'static str) -> Self {
        Self {
            listener,
            failures: 0,
            retry_at: Instant::now(),
        }
    }

    /// Recoverable errors retry indefinitely with a capped delay; only a
    /// successful accept resets the backoff. Dropping this future retains its
    /// deadline and already charged failures.
    pub(in crate::engine) async fn accept<T, F>(
        &mut self,
        mut accept: impl FnMut() -> F,
    ) -> io::Result<T>
    where
        F: Future<Output = io::Result<T>>,
    {
        loop {
            tokio::time::sleep_until(self.retry_at).await;
            match accept().await {
                Ok(socket) => {
                    self.failures = 0;
                    self.retry_at = Instant::now();
                    return Ok(socket);
                }
                Err(error) => self.failed(error)?,
            }
        }
    }

    fn failed(&mut self, error: io::Error) -> io::Result<()> {
        if !retryable(&error) {
            warn!(
                listener = self.listener,
                kind = ?error.kind(),
                os_code = error.raw_os_error(),
                retries = self.failures,
                "service_accept_failed"
            );
            return Err(error);
        }
        // Six doublings already exceed MAX_DELAY. Bound the shift as well as
        // the counter so prolonged exhaustion cannot overflow or start spinning.
        let delay = (INITIAL_DELAY * (1u32 << self.failures.min(6))).min(MAX_DELAY);
        self.failures = self.failures.saturating_add(1);
        self.retry_at = Instant::now() + delay;
        warn!(
            listener = self.listener,
            kind = ?error.kind(),
            os_code = error.raw_os_error(),
            retry = self.failures,
            delay_ms = delay.as_millis() as u64,
            "service_accept_retry"
        );
        Ok(())
    }
}

// Unknown OS errors fail closed. Platform codes supplement ErrorKind for
// resource exhaustion and Linux pending-connection network errors.
fn retryable(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::TimedOut
            | io::ErrorKind::OutOfMemory
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
    ) {
        return true;
    }
    match error.raw_os_error() {
        #[cfg(unix)]
        Some(libc::ENFILE | libc::EMFILE | libc::ENOBUFS) => true,
        #[cfg(target_os = "linux")]
        Some(
            libc::ENONET | libc::EPROTO | libc::ENOPROTOOPT | libc::EOPNOTSUPP | libc::EHOSTDOWN,
        ) => true,
        #[cfg(windows)]
        Some(10024 | 10055 | 10052 | 10064) => true,
        _ => false,
    }
}
