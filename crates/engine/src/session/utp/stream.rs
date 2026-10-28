use super::{Key, Mux};
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf};

pub(crate) struct Stream {
    mux: Arc<Mutex<Mux>>,
    key: Key,
}
impl Stream {
    pub(super) fn new(mux: Arc<Mutex<Mux>>, key: Key) -> Self {
        Self { mux, key }
    }
    pub(crate) fn retransmits(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.mux.lock().unwrap().entries[&self.key]
            .connection
            .retransmits
            .clone()
    }
    pub(crate) fn peer_addr(&self) -> std::net::SocketAddr {
        self.key.address
    }
    pub(crate) fn split(self) -> (ReadHalf<Self>, WriteHalf<Self>) {
        tokio::io::split(self)
    }
    /// Wait for the requested prefix, or EOF, without consuming any bytes.
    pub(crate) async fn peek(&self, bytes: &mut [u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| {
            let mut mux = self.mux.lock().unwrap();
            let e = mux
                .entries
                .get_mut(&self.key)
                .expect("owned uTP connection");
            if e.connection.failed {
                return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
            }
            if e.connection.received.len() >= bytes.len() || e.connection.eof() {
                let n = e.connection.received.len().min(bytes.len());
                for (out, &byte) in bytes[..n].iter_mut().zip(&e.connection.received) {
                    *out = byte;
                }
                return Poll::Ready(Ok(n));
            }
            e.reader = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
    pub(crate) fn reset(&self) {
        let mut mux = self.mux.lock().unwrap();
        let stamp = mux.stamp(Instant::now());
        mux.entries
            .get_mut(&self.key)
            .expect("owned uTP connection")
            .connection
            .abort(stamp);
        mux.output(self.key);
        mux.changed.notify_one();
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut mux = self.mux.lock().unwrap();
        let stamp = mux.stamp(Instant::now());
        let e = mux
            .entries
            .get_mut(&self.key)
            .expect("owned uTP connection");
        if e.connection.failed {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        if e.connection.received.is_empty() && !e.connection.eof() {
            e.reader = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = e.connection.read(stamp, buf.initialize_unfilled());
        buf.advance(n);
        if n != 0 {
            mux.output(self.key);
        }
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[io::IoSlice::new(bytes)])
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut mux = self.mux.lock().unwrap();
        let now = Instant::now();
        let stamp = mux.stamp(now);
        let e = mux
            .entries
            .get_mut(&self.key)
            .expect("owned uTP connection");
        if e.connection.failed || e.connection.finished {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if e.connection.writable() == 0 && buffers.iter().any(|b| !b.is_empty()) {
            e.writer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let mut n = 0;
        for bytes in buffers {
            let written = e.connection.write(now, bytes);
            n += written;
            if written < bytes.len() {
                break;
            }
        }
        e.connection.pump(now, stamp, false);
        mux.output(self.key);
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut mux = self.mux.lock().unwrap();
        let e = mux
            .entries
            .get_mut(&self.key)
            .expect("owned uTP connection");
        if e.connection.failed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if e.connection.flushed() {
            return Poll::Ready(Ok(()));
        }
        e.writer = Some(cx.waker().clone());
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut mux = self.mux.lock().unwrap();
        let now = Instant::now();
        let stamp = mux.stamp(now);
        let e = mux
            .entries
            .get_mut(&self.key)
            .expect("owned uTP connection");
        e.connection.close(now);
        e.connection.pump(now, stamp, false);
        if e.connection.failed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let done = e.connection.finished;
        if !done {
            e.writer = Some(cx.waker().clone());
        }
        mux.output(self.key);
        if done {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        let mut mux = self.mux.lock().unwrap();
        let now = Instant::now();
        let stamp = mux.stamp(now);
        if let Some(e) = mux.entries.get_mut(&self.key) {
            e.owned = false;
            e.connection.close(now);
            e.connection.pump(now, stamp, false);
            mux.output(self.key);
            mux.reap(self.key);
        }
    }
}
