use crate::protocol::peer::mse::{Cipher, Method, Methods, Negotiated, Negotiation};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

fn key_and_padding() -> io::Result<([u8; 20], Vec<u8>)> {
    let mut entropy = [0; 20 + 2 + 512];
    getrandom::fill(&mut entropy).map_err(io::Error::other)?;
    let length = u16::from_be_bytes([entropy[20], entropy[21]]) as usize % 513;
    Ok((
        entropy[..20].try_into().expect("private key size"),
        entropy[22..22 + length].to_vec(),
    ))
}

pub(in crate::engine) fn responder(methods: Methods) -> io::Result<Negotiation> {
    let (private, padding) = key_and_padding()?;
    Ok(Negotiation::responder(private, methods, &padding))
}

pub(super) async fn initiate<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    key: [u8; 20],
    methods: Methods,
    initial: &[u8],
) -> io::Result<Negotiated> {
    let (private, padding) = key_and_padding()?;
    let (codec, output) = Negotiation::initiator(private, &padding, key, methods, initial);
    exchange(socket, codec, output, |_| None).await
}

pub(in crate::engine) async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    mut codec: Negotiation,
    mut output: Vec<u8>,
    mut lookup: impl FnMut([u8; 20]) -> Option<[u8; 20]>,
) -> io::Result<Negotiated> {
    let mut bytes = [0; 4096];
    loop {
        socket.write_all(&output).await?;
        let count = socket.read(&mut bytes).await?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                crate::protocol::peer::mse::Error::Truncated,
            ));
        }
        let progress = codec
            .receive(&bytes[..count], &mut lookup)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        output = progress.output;
        if let Some(negotiated) = progress.negotiated {
            socket.write_all(&output).await?;
            return Ok(negotiated);
        }
    }
}

pub(super) struct Reader<R> {
    inner: R,
    cipher: Option<Cipher>,
    initial: Vec<u8>,
    offset: usize,
}

pub(super) struct Writer<W> {
    inner: W,
    cipher: Option<Cipher>,
    pending: Vec<u8>,
    offset: usize,
}

pub(super) fn wrap<R, W>(
    reader: R,
    writer: W,
    negotiated: Option<Box<Negotiated>>,
) -> (Reader<R>, Writer<W>) {
    let (decrypt, encrypt, initial) = match negotiated {
        Some(mut n) => {
            let encrypted = n.method == Method::Rc4;
            if encrypted {
                n.decrypt.apply(&mut n.surplus);
            }
            n.initial.extend(n.surplus);
            (
                encrypted.then_some(n.decrypt),
                encrypted.then_some(n.encrypt),
                n.initial,
            )
        }
        None => (None, None, Vec::new()),
    };
    (
        Reader {
            inner: reader,
            cipher: decrypt,
            initial,
            offset: 0,
        },
        Writer {
            inner: writer,
            cipher: encrypt,
            pending: Vec::new(),
            offset: 0,
        },
    )
}

impl<R: AsyncRead + Unpin> AsyncRead for Reader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.offset < this.initial.len() {
            let count = buffer.remaining().min(this.initial.len() - this.offset);
            buffer.put_slice(&this.initial[this.offset..this.offset + count]);
            this.offset += count;
            if this.offset == this.initial.len() {
                this.initial = Vec::new();
                this.offset = 0;
            }
            return Poll::Ready(Ok(()));
        }
        let before = buffer.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buffer) {
            Poll::Ready(Ok(())) => {
                if let Some(cipher) = &mut this.cipher {
                    cipher.apply(&mut buffer.filled_mut()[before..]);
                }
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Writer<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[io::IoSlice::new(buffer)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(cipher) = &mut this.cipher else {
            return Pin::new(&mut this.inner).poll_write_vectored(cx, buffers);
        };
        let length: usize = buffers.iter().map(|b| b.len()).sum();
        // Callers resubmit the same unaccepted plaintext; cancellation drops the connection.
        assert!(length >= this.pending.len() - this.offset);
        if this.pending.is_empty() {
            let mut remaining = length.min(64 * 1024);
            for buffer in buffers {
                let count = remaining.min(buffer.len());
                this.pending.extend_from_slice(&buffer[..count]);
                remaining -= count;
                if remaining == 0 {
                    break;
                }
            }
            cipher.apply(&mut this.pending);
        }
        match Pin::new(&mut this.inner).poll_write(cx, &this.pending[this.offset..]) {
            Poll::Ready(Ok(count)) => {
                this.offset += count;
                if this.offset == this.pending.len() {
                    this.pending.clear();
                    this.offset = 0;
                }
                Poll::Ready(Ok(count))
            }
            result => result,
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        assert!(this.pending.is_empty(), "unfinished encrypted write");
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        assert!(this.pending.is_empty(), "unfinished encrypted write");
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}
