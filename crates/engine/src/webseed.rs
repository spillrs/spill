//! BEP 19 HTTP(S) range sources, explicitly authorized independently of trackers.
//! A tracked peer adapter feeds the normal request, hash, retry and storage path.
//! Parallel mirrors share bounded request/storage budgets, pinned DNS and same-origin redirects.
use crate::peer_io::{ConnectionId, Frame, PeerEvent};
use protocol::{
    bencode::Value,
    metainfo::{FileKind, OwnedMetainfo},
    peer::message::{BlockRequest, Message},
};
use reqwest::{Client, header, redirect};
use std::{collections::VecDeque, io, net::SocketAddr};
use tokio::{net::lookup_host, sync::mpsc, time::timeout};
use url::Url;

pub(crate) const MAX_ACTIVE: usize = 16;

pub(crate) struct WebSeeds {
    allow_local_network: bool,
    urls: VecDeque<Url>,
    pub(crate) active: Vec<ConnectionId>,
}
impl WebSeeds {
    pub(crate) fn new(meta: &OwnedMetainfo, allow_local_network: bool) -> io::Result<Self> {
        let document = meta.decode_document();
        let values: Vec<_> = match document.get(b"url-list").map(|n| n.value()) {
            None => Vec::new(),
            Some(Value::Bytes(bytes)) => vec![*bytes],
            Some(Value::List(list)) => list
                .iter()
                .take(crate::profile::MAX_WEB_SEEDS)
                .filter_map(|n| match n.value() {
                    Value::Bytes(b) => Some(*b),
                    _ => None,
                })
                .collect(),
            _ => return Err(invalid("invalid web seed url-list")),
        };
        let mut urls = VecDeque::new();
        let mut bytes = 0usize;
        for raw in values.into_iter().take(crate::profile::MAX_WEB_SEEDS) {
            if raw.len() > crate::profile::MAX_URL_BYTES {
                continue;
            }
            bytes = bytes.saturating_add(raw.len());
            if bytes > crate::profile::MAX_TOTAL_URL_BYTES {
                break;
            }
            let Ok(text) = std::str::from_utf8(raw) else {
                continue;
            };
            let Ok(url) = Url::parse(text) else {
                continue;
            };
            if valid(&url)
                && (!meta.layout().is_multi_file() || url.path().ends_with('/'))
                && !urls.contains(&url)
            {
                urls.push_back(url);
            }
        }
        tracing::info!(sources = urls.len(), "web_seed_policy");
        Ok(Self {
            allow_local_network,
            urls,
            active: Vec::new(),
        })
    }
    pub(crate) fn next(&mut self, meta: OwnedMetainfo) -> Option<Seed> {
        Some(Seed {
            url: self.urls.pop_front()?,
            meta,
            allow_local_network: self.allow_local_network,
        })
    }
}

struct PinnedClient {
    client: Client,
    addresses: Vec<SocketAddr>,
}
pub(crate) struct Seed {
    url: Url,
    meta: OwnedMetainfo,
    allow_local_network: bool,
}
impl Seed {
    pub(crate) fn url(&self) -> &str {
        self.url.as_str()
    }
    pub(crate) async fn run(
        self,
        connection: ConnectionId,
        mut commands: mpsc::Receiver<crate::peer_io::Outbound>,
        events: &mpsc::Sender<PeerEvent>,
        capacity_changed: &tokio::sync::Notify,
    ) -> io::Result<()> {
        let client = self.client().await?;
        events
            .send(PeerEvent::Handshake {
                connection,
                reserved: [0; 8],
                peer_id: [0; 20],
            })
            .await
            .map_err(closed)?;
        let count = self.meta.layout().piece_count();
        let mut bits = vec![255; count.div_ceil(8) as usize];
        if count % 8 != 0
            && let Some(last) = bits.last_mut()
        {
            *last <<= 8 - count % 8;
        }
        self.emit(connection, events, Message::Bitfield(&bits))
            .await?;
        self.emit(connection, events, Message::Unchoke).await?;
        let mut pending = VecDeque::new();
        loop {
            let mut received = false;
            if pending.is_empty() {
                let Some(bytes) = commands.recv().await else {
                    return Ok(());
                };
                self.command(&bytes, &mut pending)?;
                received = true;
            }
            // Drain only the bounded transport queue; coalesce contiguous requests
            // from the same piece, never reading ahead beyond picker admission.
            for _ in 0..crate::profile::OUTBOUND_QUEUE {
                let Ok(bytes) = commands.try_recv() else {
                    break;
                };
                self.command(&bytes, &mut pending)?;
                received = true;
            }
            if received {
                capacity_changed.notify_one();
            }
            let Some(first) = pending.pop_front() else {
                continue;
            };
            let mut batch = vec![first];
            let mut length = first.length;
            while let Some(next) = pending.front() {
                if next.index != first.index || next.begin != first.begin + length {
                    break;
                }
                length = length
                    .checked_add(next.length)
                    .ok_or_else(|| invalid("web seed range overflow"))?;
                batch.push(pending.pop_front().expect("front"));
            }
            let range = self
                .meta
                .layout()
                .piece_range(first.index)
                .ok_or_else(|| invalid("invalid web seed piece"))?;
            let start = range.start + u64::from(first.begin);
            let body = timeout(
                crate::profile::REQUEST_TIMEOUT,
                self.fetch(&client, start, length),
            )
            .await
            .map_err(|_| invalid("web seed request deadline"))??;
            let mut at = 0;
            for request in batch {
                let end = at + request.length as usize;
                self.emit(
                    connection,
                    events,
                    Message::Piece {
                        index: request.index,
                        begin: request.begin,
                        block: &body[at..end],
                    },
                )
                .await?;
                at = end;
            }
        }
    }
    fn command(
        &self,
        bytes: &crate::peer_io::Outbound,
        pending: &mut VecDeque<BlockRequest>,
    ) -> io::Result<()> {
        let message = bytes
            .message(crate::profile::MAX_FRAME_LENGTH)
            .map_err(|_| invalid("invalid web seed command"))?;
        match message {
            Message::Request(request) => {
                message
                    .validate_layout(self.meta.layout(), crate::profile::BLOCK_LEN)
                    .map_err(|_| invalid("invalid web seed request"))?;
                if pending.len() >= crate::profile::MAX_REQUEST_QUEUE {
                    return Err(invalid("web seed request queue exhausted"));
                }
                if !pending.contains(&request) {
                    pending.push_back(request);
                }
            }
            Message::Cancel(request) => pending.retain(|r| *r != request),
            _ => {}
        }
        Ok(())
    }
    async fn emit(
        &self,
        connection: ConnectionId,
        events: &mpsc::Sender<PeerEvent>,
        message: Message<'_>,
    ) -> io::Result<()> {
        let frame = Frame::from_message(message, crate::profile::MAX_FRAME_LENGTH)?;
        events
            .send(PeerEvent::Frame { connection, frame })
            .await
            .map_err(closed)
    }
    async fn client(&self) -> io::Result<PinnedClient> {
        let host = self
            .url
            .host_str()
            .ok_or_else(|| invalid("invalid web seed host"))?;
        let port = self
            .url
            .port_or_known_default()
            .ok_or_else(|| invalid("invalid web seed port"))?;
        let addresses: Vec<SocketAddr> = match self.url.host() {
            Some(url::Host::Ipv4(ip)) => vec![SocketAddr::new(ip.into(), port)],
            Some(url::Host::Ipv6(ip)) => vec![SocketAddr::new(ip.into(), port)],
            _ => timeout(crate::profile::DNS_TIMEOUT, lookup_host((host, port)))
                .await
                .map_err(|_| invalid("web seed DNS deadline"))?
                .map_err(|_| invalid("web seed DNS failed"))?
                .take(crate::profile::MAX_DNS_ANSWERS)
                .filter(|a| crate::network::source(*a, self.allow_local_network))
                .collect(),
        };
        if addresses.is_empty()
            || addresses
                .iter()
                .any(|a| !crate::network::source(*a, self.allow_local_network))
        {
            return Err(invalid("no usable web seed address"));
        }
        let client = Client::builder()
            .use_rustls_tls()
            .http1_only()
            .no_proxy()
            .redirect(redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .referer(false)
            .connect_timeout(crate::profile::CONNECT_TIMEOUT)
            .timeout(crate::profile::REQUEST_TIMEOUT)
            .pool_max_idle_per_host(1)
            .user_agent(concat!("Spill/", env!("CARGO_PKG_VERSION")))
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(transport)?;
        Ok(PinnedClient { client, addresses })
    }
    async fn fetch(&self, client: &PinnedClient, start: u64, length: u32) -> io::Result<Vec<u8>> {
        if length > crate::profile::MAX_PIECE_LEN {
            return Err(invalid("web seed range budget"));
        }
        let mut body = Vec::with_capacity(length as usize);
        let segments = self
            .meta
            .layout()
            .segments(start..start + u64::from(length))
            .ok_or_else(|| invalid("invalid web seed range"))?;
        for segment in segments {
            if segment.kind == FileKind::Padding {
                body.resize(body.len() + segment.length as usize, 0);
                continue;
            }
            let file = &self.meta.layout().files()[segment.file_index];
            let url = file_url(&self.url, &self.meta, segment.file_index)?;
            let bytes = get_range(
                client,
                url,
                segment.file_offset,
                segment.length,
                file.length(),
                self.allow_local_network,
            )
            .await?;
            body.extend_from_slice(&bytes);
        }
        Ok(body)
    }
}
fn valid(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host().is_some()
        && url.port_or_known_default().is_some_and(|p| p != 0)
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url.as_str().len() <= crate::profile::MAX_URL_BYTES
}
fn file_url(base: &Url, meta: &OwnedMetainfo, file: usize) -> io::Result<Url> {
    let mut url = base.clone();
    if meta.layout().is_multi_file() || base.path().ends_with('/') {
        let mut path = base.path().to_owned();
        if meta.layout().is_multi_file() {
            component(&mut path, meta.name());
            path.push('/');
        }
        for (i, part) in meta.layout().files()[file].path().iter().enumerate() {
            if i != 0 {
                path.push('/');
            }
            component(&mut path, part);
        }
        url.set_path(&path);
    }
    if !valid(&url) {
        return Err(invalid("invalid or oversized web seed file URL"));
    }
    Ok(url)
}
fn component(path: &mut String, bytes: &[u8]) {
    const HEX: &[u8] = b"0123456789ABCDEF";
    for &b in bytes {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            path.push(char::from(b));
        } else {
            path.push('%');
            path.push(char::from(HEX[(b >> 4) as usize]));
            path.push(char::from(HEX[(b & 15) as usize]));
        }
    }
}
async fn get_range(
    client: &PinnedClient,
    mut url: Url,
    start: u64,
    length: u64,
    total: u64,
    allow_local_network: bool,
) -> io::Result<Vec<u8>> {
    let end = start
        .checked_add(length)
        .and_then(|n| n.checked_sub(1))
        .ok_or_else(|| invalid("invalid web seed range"))?;
    let origin = url.origin();
    for redirects in 0..=3 {
        // The same-origin restriction below preserves these pins on every hop.
        if client.addresses.is_empty()
            || client
                .addresses
                .iter()
                .any(|a| !crate::network::source(*a, allow_local_network))
        {
            return Err(invalid("web seed endpoint blocked by network policy"));
        }
        let mut response = client
            .client
            .get(url.clone())
            .header(header::RANGE, format!("bytes={start}-{end}"))
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(transport)?;
        if matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
            if redirects == 3 {
                return Err(invalid("web seed redirect limit"));
            }
            let location = response
                .headers()
                .get(header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .filter(|s| s.len() <= crate::profile::MAX_URL_BYTES)
                .ok_or_else(|| invalid("invalid web seed redirect"))?;
            let next = url
                .join(location)
                .map_err(|_| invalid("invalid web seed redirect"))?;
            if next.origin() != origin || !valid(&next) {
                return Err(invalid("web seed redirect must remain same-origin"));
            }
            url = next;
            continue;
        }
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(invalid("web seed requires HTTP 206 range response"));
        }
        let expected = format!("bytes {start}-{end}/{total}");
        if response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            != Some(expected.as_str())
            || response.content_length().is_some_and(|n| n != length)
            || response
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_some_and(|v| !v.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(invalid("invalid web seed range headers"));
        }
        let mut body = Vec::with_capacity(length as usize);
        while let Some(chunk) = response.chunk().await.map_err(transport)? {
            if chunk.len() as u64 > length - body.len() as u64 {
                return Err(invalid("web seed body exceeds requested range"));
            }
            body.extend_from_slice(&chunk);
        }
        if body.len() as u64 != length {
            return Err(invalid("short web seed range body"));
        }
        return Ok(body);
    }
    unreachable!("bounded redirect loop")
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn transport(error: reqwest::Error) -> io::Error {
    io::Error::new(
        if error.is_builder() {
            io::ErrorKind::Other
        } else if error.is_timeout() {
            io::ErrorKind::TimedOut
        } else {
            // Reqwest collapses TLS/reset/connect failures into its own error.
            // Keep them remote: raw `Other` means a fatal local fault to peer_io.
            io::ErrorKind::ConnectionAborted
        },
        "web seed HTTP transport failed",
    )
}
fn closed(_: impl std::fmt::Debug) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "web seed owner closed")
}
