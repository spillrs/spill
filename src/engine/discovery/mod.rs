//! Sequential BEP 12 announces over session-owned transports.
use crate::engine::{core::AnnounceEvent, torrent::telemetry::TrackerStatus};
use crate::protocol::{
    bencode::{self, Value},
    metainfo::OwnedMetainfo,
    tracker::{http, udp},
};
use std::{
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::time::{Instant, timeout};
use url::Url;

const MIN_INTERVAL: Duration = Duration::from_secs(300);
const BODY_CAP: usize = 64 * 1024;
const PEER_CAP: usize = 200;
const CODEC: udp::Limits = udp::Limits {
    max_packet_len: 65_507,
    max_peers: (65_507 - 20) / 6,
};
type Pending = Pin<Box<dyn Future<Output = io::Result<Reply>> + Send>>;
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid tracker reply or endpoint",
    )
}
fn tracker_failure(message: &[u8]) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        String::from_utf8_lossy(message).into_owned(),
    )
}
fn random() -> io::Result<u32> {
    let mut bytes = [0; 4];
    getrandom::fill(&mut bytes).map_err(io::Error::other)?;
    Ok(u32::from_ne_bytes(bytes))
}
#[derive(Clone, Copy)]
pub(in crate::engine) struct Counters {
    pub(in crate::engine) downloaded: u64,
    pub(in crate::engine) corrupt: u64,
    pub(in crate::engine) uploaded: u64,
    pub(in crate::engine) left: u64,
}
struct Reply {
    interval: Duration,
    min_interval: Duration,
    peers: Vec<SocketAddr>,
    seeders: Option<u64>,
    leechers: Option<u64>,
    tracker_id: Option<Vec<u8>>,
    warning: Option<String>,
    error: Option<io::Error>,
    disabled: bool,
}
struct Tracker {
    url: Option<Url>,
    started: bool,
    tracker_id: Option<Vec<u8>>,
    status: TrackerStatus,
    last_reply: Option<Instant>,
    next_allowed: Instant,
    next_announce: Instant,
    fails: u8,
    disabled: bool,
    pending: Option<(Option<AnnounceEvent>, Pending)>,
}
pub(in crate::engine) struct AnnounceOptions {
    pub(in crate::engine) hash: [u8; 20],
    pub(in crate::engine) peer: [u8; 20],
    pub(in crate::engine) port: tokio::sync::watch::Receiver<u16>,
    pub(in crate::engine) left: u64,
    pub(in crate::engine) allow_local_network: bool,
    pub(in crate::engine) client: reqwest::Client,
    pub(in crate::engine) udp: crate::engine::session::udp::Transport,
    pub(in crate::engine) completed_announce: Option<bool>,
    pub(in crate::engine) checkpoint: Arc<dyn Fn(bool) -> io::Result<()> + Send + Sync>,
}
pub(in crate::engine) struct Announcer {
    trackers: Vec<Tracker>,
    pub(in crate::engine) hash: [u8; 20],
    peer: [u8; 20],
    port: tokio::sync::watch::Receiver<u16>,
    key: u32,
    pub(in crate::engine) counters: Counters,
    allow_local_network: bool,
    client: reqwest::Client,
    udp: crate::engine::session::udp::Transport,
    completed_pending: bool,
    completed_acknowledged: bool,
    checkpoint: Arc<dyn Fn(bool) -> io::Result<()> + Send + Sync>,
    // A select! cancellation must not abandon an in-progress acknowledgement.
    checkpoint_job: Option<(bool, tokio::task::JoinHandle<io::Result<()>>)>,
}
impl Announcer {
    pub(in crate::engine) fn new(
        tiers: Vec<Vec<Vec<u8>>>,
        options: AnnounceOptions,
    ) -> io::Result<Option<Self>> {
        let AnnounceOptions {
            hash,
            peer,
            port,
            left,
            allow_local_network,
            client,
            udp,
            completed_announce,
            checkpoint,
        } = options;
        let mut parsed = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (tier, urls) in tiers.into_iter().enumerate() {
            let trackers: Vec<_> = urls
                .into_iter()
                .filter(|raw| seen.insert(raw.clone()))
                .enumerate()
                .map(|(slot, raw)| {
                    let url = std::str::from_utf8(&raw)
                        .ok()
                        .and_then(|s| Url::parse(s).ok())
                        .filter(|u| {
                            matches!(u.scheme(), "http" | "https" | "udp")
                                && u.host().is_some()
                                && u.username().is_empty()
                                && u.password().is_none()
                                && u.fragment().is_none()
                                && u.port_or_known_default().is_some_and(|p| p != 0)
                        });
                    Tracker {
                        url,
                        started: false,
                        tracker_id: None,
                        last_reply: None,
                        next_allowed: Instant::now(),
                        next_announce: Instant::now(),
                        fails: 0,
                        disabled: false,
                        pending: None,
                        status: TrackerStatus {
                            swarm: hash,
                            url: String::from_utf8_lossy(&raw).into_owned(),
                            tier,
                            slot,
                            state: "Idle",
                            exchanges: 0,
                            failures: 0,
                            last_reply_ago_ms: None,
                            next_announce_ms: Some(0),
                            seeders: None,
                            leechers: None,
                            peers_returned: None,
                            error: None,
                            warning: None,
                        },
                    }
                })
                .collect();
            parsed.extend(trackers);
        }
        if parsed.is_empty() {
            return Ok(None);
        }
        // Prefer UDP on the same host, without announcing to both transports.
        for i in 0..parsed.len() {
            let Some(url) = parsed[i].url.as_ref() else {
                continue;
            };
            if url.scheme() != "udp" {
                continue;
            }
            if let Some(j) = (0..i).find(|&j| {
                parsed[j].url.as_ref().is_some_and(|other| {
                    other.scheme() != "udp" && other.host_str() == url.host_str()
                })
            }) {
                parsed.swap(i, j);
            }
        }
        Ok(Some(Self {
            trackers: parsed,
            hash,
            peer,
            port,
            key: random()?,
            counters: Counters {
                downloaded: 0,
                corrupt: 0,
                uploaded: 0,
                left,
            },
            allow_local_network,
            client,
            udp,
            completed_pending: completed_announce == Some(false),
            completed_acknowledged: completed_announce == Some(true),
            checkpoint,
            checkpoint_job: None,
        }))
    }
    pub(in crate::engine) fn metainfo(meta: &OwnedMetainfo) -> Vec<Vec<Vec<u8>>> {
        let document = meta.decode_document();
        if let Some(list) = document.get(b"announce-list")
            && let Value::List(tiers) = list.value()
        {
            return tiers
                .iter()
                .filter_map(|tier| match tier.value() {
                    Value::List(urls) => Some(
                        urls.iter()
                            .filter_map(|url| match url.value() {
                                Value::Bytes(bytes) => Some(bytes.to_vec()),
                                _ => None,
                            })
                            .collect(),
                    ),
                    _ => None,
                })
                .collect();
        }
        document
            .get(b"announce")
            .and_then(|url| match url.value() {
                Value::Bytes(bytes) => Some(vec![vec![bytes.to_vec()]]),
                _ => None,
            })
            .unwrap_or_default()
    }
    pub(in crate::engine) async fn request(&mut self, event: AnnounceEvent) -> io::Result<()> {
        match event {
            AnnounceEvent::Completed if !self.completed_pending && !self.completed_acknowledged => {
                self.start_checkpoint(false);
                self.acknowledge().await?;
                self.completed_pending = true;
            }
            AnnounceEvent::Completed => return Ok(()),
            AnnounceEvent::Started => {}
            AnnounceEvent::Stopped => return Ok(()),
        }
        Ok(())
    }
    fn start_checkpoint(&mut self, delivered: bool) {
        assert!(self.checkpoint_job.is_none());
        let checkpoint = self.checkpoint.clone();
        self.checkpoint_job = Some((
            delivered,
            tokio::task::spawn_blocking(move || checkpoint(delivered)),
        ));
    }
    async fn join_checkpoint(&mut self) -> io::Result<Option<bool>> {
        let Some((delivered, job)) = &mut self.checkpoint_job else {
            return Ok(None);
        };
        let delivered = *delivered;
        let result = job.await.map_err(io::Error::other).and_then(|r| r);
        self.checkpoint_job = None;
        result.map(|()| Some(delivered))
    }
    async fn acknowledge(&mut self) -> io::Result<()> {
        if self.join_checkpoint().await? == Some(true) {
            self.completed_pending = false;
            self.completed_acknowledged = true;
        }
        Ok(())
    }
    pub(in crate::engine) fn completion_pending(&self) -> bool {
        self.completed_pending
    }
    pub(in crate::engine) fn reannounce(&mut self) {
        for tracker in self
            .trackers
            .iter_mut()
            .filter(|t| t.fails == 0 && t.pending.is_none())
        {
            tracker.next_announce = Instant::now();
        }
    }
    fn next_due(&self) -> Instant {
        let mut earliest = None;
        for tracker in self.trackers.iter().filter(|t| !t.disabled) {
            if tracker.pending.is_none() {
                let due = tracker.next_announce.max(tracker.next_allowed);
                earliest = Some(earliest.map_or(due, |at: Instant| at.min(due)));
            }
            if tracker.fails == 0 {
                break;
            }
        }
        earliest.unwrap_or_else(|| Instant::now() + Duration::from_secs(60))
    }
    pub(in crate::engine) fn snapshots(&self) -> Vec<TrackerStatus> {
        self.trackers
            .iter()
            .map(|t| {
                let mut status = t.status.clone();
                status.last_reply_ago_ms = t.last_reply.map(|at| at.elapsed().as_millis() as u64);
                status.next_announce_ms = (!t.disabled).then(|| {
                    t.next_announce
                        .max(t.next_allowed)
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64
                });
                status
            })
            .collect()
    }
    fn exchange(&self, tracker: &Tracker, event: Option<AnnounceEvent>) -> Pending {
        let url = tracker.url.clone();
        let allow_local_network = self.allow_local_network;
        let tracker_id = tracker.tracker_id.clone();
        let corrupt = self.counters.corrupt;
        let client = self.client.clone();
        let udp = self.udp.clone();
        let a = udp::Announce {
            info_hash: self.hash,
            peer_id: self.peer,
            port: *self.port.borrow(),
            downloaded: self.counters.downloaded,
            uploaded: self.counters.uploaded,
            left: self.counters.left,
            event: match event {
                Some(AnnounceEvent::Started) => udp::EVENT_STARTED,
                Some(AnnounceEvent::Completed) => udp::EVENT_COMPLETED,
                Some(AnnounceEvent::Stopped) => udp::EVENT_STOPPED,
                None => udp::EVENT_NONE,
            },
            ip: Ipv4Addr::UNSPECIFIED,
            key: self.key,
            num_want: if matches!(event, Some(AnnounceEvent::Stopped)) {
                0
            } else {
                PEER_CAP as i32
            },
        };
        Box::pin(async move {
            let deadline = if matches!(event, Some(AnnounceEvent::Stopped)) {
                Duration::from_secs(5)
            } else {
                Duration::from_secs(30)
            };
            timeout(deadline, async {
                let url = url.ok_or_else(invalid)?;
                if url.scheme() == "udp" {
                    udp_announce(url, a, allow_local_network, udp).await
                } else {
                    http_announce(
                        url,
                        a,
                        corrupt,
                        allow_local_network,
                        tracker_id.as_deref(),
                        client,
                    )
                    .await
                }
            })
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
        })
    }
    /// Retain each exchange across select! cancellation; never restart network I/O on a tick.
    pub(in crate::engine) async fn next(&mut self) -> io::Result<Vec<SocketAddr>> {
        self.acknowledge().await?;
        loop {
            let now = Instant::now();
            for index in 0..self.trackers.len() {
                let tracker = &self.trackers[index];
                if tracker.disabled {
                    continue;
                }
                let working = tracker.fails == 0;
                let completing = self.completed_pending && working;
                if tracker.pending.is_none()
                    && (completing || now + Duration::from_secs(1) >= tracker.next_announce)
                    && (completing || now >= tracker.next_allowed)
                {
                    let event = if !tracker.started {
                        Some(AnnounceEvent::Started)
                    } else {
                        self.completed_pending.then_some(AnnounceEvent::Completed)
                    };
                    let exchange = self.exchange(tracker, event);
                    self.trackers[index].pending = Some((event, exchange));
                }
                // A known failure must not hold up the next working tracker.
                if working {
                    break;
                }
            }
            let due = self.next_due();
            let (index, result) = tokio::select! {
                reply = std::future::poll_fn(|cx| {
                    for (index, tracker) in self.trackers.iter_mut().enumerate() {
                        if let Some((_, exchange)) = &mut tracker.pending
                            && let std::task::Poll::Ready(reply) = exchange.as_mut().poll(cx)
                        {
                            return std::task::Poll::Ready((index, reply));
                        }
                    }
                    std::task::Poll::Pending
                }) => reply,
                _ = tokio::time::sleep_until(due) => continue,
            };
            let (sent_event, _) = self.trackers[index].pending.take().expect("exchange");
            let mut retry_interval = Duration::ZERO;
            let result = match result {
                Ok(mut reply) if reply.error.is_some() => {
                    retry_interval = reply.interval;
                    self.trackers[index].disabled = reply.disabled;
                    self.trackers[index].status.warning = reply.warning;
                    Err(reply.error.take().expect("tracker failure"))
                }
                result => result,
            };
            let tracker = &mut self.trackers[index];
            tracker.status.exchanges = tracker.status.exchanges.saturating_add(1);
            match result {
                Ok(reply) => {
                    if tracker.fails != 0 {
                        tracing::debug!(swarm=?self.hash,tier=tracker.status.tier,slot=tracker.status.slot,"tracker_recovered");
                    }
                    tracker.started = true;
                    tracker.last_reply = Some(Instant::now());
                    tracker.status.state = "Success";
                    tracker.status.error = None;
                    tracker.status.warning = reply.warning;
                    if let Some(id) = reply.tracker_id {
                        tracker.tracker_id = Some(id);
                    }
                    tracker.status.peers_returned = Some(reply.peers.len());
                    tracker.status.seeders = reply.seeders;
                    tracker.status.leechers = reply.leechers;
                    tracker.next_allowed = Instant::now() + reply.min_interval;
                    tracker.next_announce = Instant::now() + reply.interval;
                    tracker.fails = 0;
                    if matches!(sent_event, Some(AnnounceEvent::Completed)) {
                        self.start_checkpoint(true);
                        self.acknowledge().await?;
                    }
                    return Ok(reply.peers);
                }
                Err(error) => {
                    tracker.status.state = "Error";
                    tracker.status.failures = tracker.status.failures.saturating_add(1);
                    tracker.status.peers_returned = Some(0);
                    let message = error.to_string();
                    let changed = tracker.status.error.as_deref() != Some(message.as_str());
                    tracker.status.error = Some(message);
                    tracker.fails = (tracker.fails + 1).min(127);
                    let fails = u64::from(tracker.fails);
                    let delay = Duration::from_secs((5 + fails * fails * 5 * 250 / 100).min(3600))
                        .max(retry_interval);
                    tracker.next_announce = Instant::now() + delay;
                    if changed {
                        tracing::debug!(swarm=?self.hash,tier=tracker.status.tier,slot=tracker.status.slot,kind=?error.kind(),%error,retry_secs=delay.as_secs(),disabled=tracker.disabled,"tracker_failed");
                    }
                    let failed = self.trackers.remove(index);
                    let mut end = index;
                    while end < self.trackers.len()
                        && self.trackers[end].status.tier == failed.status.tier
                    {
                        end += 1;
                    }
                    self.trackers.insert(end, failed);
                }
            }
        }
    }
    pub(in crate::engine) async fn drain(&mut self) -> io::Result<()> {
        for tracker in &mut self.trackers {
            tracker.pending = None;
        }
        self.join_checkpoint().await.map(|_| ())
    }
    pub(in crate::engine) async fn shutdown(&mut self) -> io::Result<()> {
        let checkpoint = self.drain().await;
        let exchanges: Vec<_> = self
            .trackers
            .iter()
            .filter(|t| t.started && !t.disabled)
            .map(|tracker| self.exchange(tracker, Some(AnnounceEvent::Stopped)))
            .collect();
        let _ = timeout(Duration::from_secs(5), async move {
            for exchange in exchanges {
                if exchange.await.is_ok_and(|reply| reply.error.is_none()) {
                    break;
                }
            }
        })
        .await;
        checkpoint
    }
}

// Resolve afresh for each request, filtering the actual connection destinations.
// No pins, proxy, or secondary unfiltered resolution.
#[derive(Debug)]
struct Resolver(bool);
impl reqwest::dns::Resolve for Resolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let allow_local_network = self.0;
        Box::pin(async move {
            let addresses = timeout(
                crate::engine::profile::DNS_TIMEOUT,
                tokio::net::lookup_host((name.as_str(), 80)),
            )
            .await??;
            let allowed: Vec<_> = addresses
                .filter(|a| crate::engine::network::source(*a, allow_local_network))
                .map(|mut address| {
                    // A nonzero DNS port overrides the URL's default HTTPS port.
                    address.set_port(0);
                    address
                })
                .collect();
            if allowed.is_empty() {
                return Err(invalid().into());
            }
            Ok(Box::new(allowed.into_iter()) as reqwest::dns::Addrs)
        })
    }
}
pub(in crate::engine) fn http_client(allow_local_network: bool) -> io::Result<reqwest::Client> {
    crate::http::client()
        .user_agent(crate::engine::identity::CLIENT_NAME)
        .referer(false)
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            let url = attempt.url();
            if attempt.previous().len() > 5 {
                return attempt.error("tracker redirect limit");
            }
            if !matches!(url.scheme(), "http" | "https")
                || (url.scheme() == "http"
                    && attempt
                        .previous()
                        .last()
                        .is_some_and(|url| url.scheme() == "https"))
                || !tracker_destination_allowed(url, allow_local_network)
            {
                return attempt.error("tracker redirect destination not allowed");
            }
            attempt.follow()
        }))
        .retry(reqwest::retry::never())
        .pool_idle_timeout(Duration::from_secs(90))
        .dns_resolver(Arc::new(Resolver(allow_local_network)))
        .connect_timeout(crate::engine::profile::CONNECT_TIMEOUT)
        .build()
        .map_err(io::Error::other)
}
fn tracker_destination_allowed(url: &Url, allow_local_network: bool) -> bool {
    let Some(port) = url.port_or_known_default() else {
        return false;
    };
    if port == 0 {
        return false;
    }
    match url.host() {
        Some(url::Host::Ipv4(ip)) => {
            crate::engine::network::source(SocketAddr::new(ip.into(), port), allow_local_network)
        }
        Some(url::Host::Ipv6(ip)) => {
            crate::engine::network::source(SocketAddr::new(ip.into(), port), allow_local_network)
        }
        Some(url::Host::Domain(_)) => true,
        None => false,
    }
}

fn failed_reply(error: io::Error, interval: Duration, disabled: bool) -> Reply {
    Reply {
        interval,
        min_interval: Duration::ZERO,
        peers: Vec::new(),
        seeders: None,
        leechers: None,
        tracker_id: None,
        warning: None,
        error: Some(error),
        disabled,
    }
}

async fn http_announce(
    mut url: Url,
    a: udp::Announce,
    corrupt: u64,
    allow_local_network: bool,
    tracker_id: Option<&[u8]>,
    client: reqwest::Client,
) -> io::Result<Reply> {
    if !tracker_destination_allowed(&url, allow_local_network) {
        return Err(invalid());
    }
    let request = http::AnnounceRequest {
        info_hash: a.info_hash,
        peer_id: a.peer_id,
        port: a.port,
        uploaded: a.uploaded,
        downloaded: a.downloaded,
        corrupt,
        support_crypto: true,
        left: a.left,
        event: match a.event {
            udp::EVENT_STARTED => Some(http::AnnounceEvent::Started),
            udp::EVENT_COMPLETED => Some(http::AnnounceEvent::Completed),
            udp::EVENT_STOPPED => Some(http::AnnounceEvent::Stopped),
            _ => None,
        },
        numwant: Some(a.num_want as u32),
        key: Some(a.key),
        tracker_id,
    };
    let query = request
        .encode_query(url.query(), 16 * 1024)
        .map_err(|_| invalid())?;
    url.set_query(Some(&query));
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|e| io::Error::other(e.without_url()))?;
    if response.status() != reqwest::StatusCode::OK {
        return Ok(failed_reply(
            io::Error::other(format!("tracker HTTP status {}", response.status())),
            Duration::ZERO,
            response.status() == reqwest::StatusCode::GONE,
        ));
    }
    if response
        .content_length()
        .is_some_and(|n| n > BODY_CAP as u64)
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| io::Error::other(e.without_url()))?
    {
        if bytes.len() + chunk.len() > BODY_CAP {
            return Err(invalid());
        }
        bytes.extend_from_slice(&chunk);
    }
    // Validate the entire reply before retaining a bounded prefix of contacts.
    let limits = bencode::Limits {
        max_input_len: BODY_CAP,
        max_string_len: BODY_CAP,
        max_output_len: BODY_CAP,
        max_nodes: BODY_CAP,
        max_depth: 16,
    };
    let reply =
        http::AnnounceResponse::parse(&bytes, limits, BODY_CAP / 6).map_err(|_| invalid())?;
    let interval = Duration::from_secs(u64::from(
        u32::try_from(reply.interval_secs().unwrap_or(1800)).map_err(|_| invalid())?,
    ))
    .max(MIN_INTERVAL);
    let min_interval = Duration::from_secs(u64::from(
        u32::try_from(reply.min_interval_secs().unwrap_or(30)).map_err(|_| invalid())?,
    ));
    if let Some(reason) = reply.failure_reason() {
        let mut failure = failed_reply(tracker_failure(reason), interval, false);
        failure.warning = reply
            .warning_message()
            .map(|m| String::from_utf8_lossy(m).into_owned());
        return Ok(failure);
    }
    if !reply.peers_v4().remainder().is_empty() || !reply.peers_v6().remainder().is_empty() {
        return Err(invalid());
    }
    // The codec permits incomplete responses for inspection; an announcer must
    // require well-typed peer fields and validate every dictionary record too.
    let document = reply.document();
    match document.get(b"peers").map(|n| n.value()) {
        Some(Value::Bytes(_)) => {}
        Some(Value::List(entries))
            if reply.dictionary_peers().count() == entries.len()
                && reply.dictionary_peers().all(|p| p.socket_addr().is_some()) => {}
        None if document.get(b"peers6").is_some() || a.event == udp::EVENT_STOPPED => {}
        _ => return Err(invalid()),
    }
    if document
        .get(b"peers6")
        .is_some_and(|n| !matches!(n.value(), Value::Bytes(_)))
    {
        return Err(invalid());
    }
    let peers = reply
        .peers_v4()
        .iter()
        .map(SocketAddr::V4)
        .chain(reply.peers_v6().iter().map(SocketAddr::V6))
        .chain(reply.dictionary_peers().filter_map(|p| p.socket_addr()))
        .filter(|a| crate::engine::network::peer(*a, allow_local_network))
        .take(PEER_CAP)
        .collect();
    Ok(Reply {
        interval,
        min_interval,
        peers,
        seeders: reply.complete(),
        leechers: reply.incomplete(),
        tracker_id: reply.tracker_id().map(<[u8]>::to_vec),
        error: None,
        disabled: false,
        warning: reply
            .warning_message()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
    })
}
async fn udp_announce(
    url: Url,
    a: udp::Announce,
    allow_local_network: bool,
    transport: crate::engine::session::udp::Transport,
) -> io::Result<Reply> {
    let host = url.host_str().ok_or_else(invalid)?.trim_matches(['[', ']']);
    let addresses = timeout(
        crate::engine::profile::DNS_TIMEOUT,
        tokio::net::lookup_host((host, url.port().ok_or_else(invalid)?)),
    )
    .await
    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    let endpoint = addresses
        .into_iter()
        .find(|a| crate::engine::network::source(*a, allow_local_network))
        .ok_or_else(invalid)?;
    let endpoint = SocketAddr::new(endpoint.ip().to_canonical(), endpoint.port());
    if endpoint.is_ipv6() && transport.socket.local_addr()?.is_ipv4() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "IPv6 tracker requires an IPv6 or dual-stack session UDP socket",
        ));
    }
    let connect = udp::Request {
        connection_id: udp::PROTOCOL_ID,
        transaction_id: 0,
        kind: udp::RequestKind::Connect { extensions: &[] },
    };
    let bytes = udp_exchange(&transport, endpoint, connect).await?;
    let family = if endpoint.is_ipv4() {
        udp::Family::V4
    } else {
        udp::Family::V6
    };
    let connection_id = match udp::Response::decode(&bytes, family, CODEC)
        .map_err(|_| invalid())?
        .kind
    {
        udp::ResponseKind::Connect { connection_id, .. } => connection_id,
        udp::ResponseKind::Error(message) => return Err(tracker_failure(message)),
        _ => return Err(invalid()),
    };
    let request = udp::Request {
        connection_id,
        transaction_id: 0,
        kind: udp::RequestKind::Announce {
            announce: a,
            extensions: &[],
        },
    };
    let bytes = udp_exchange(&transport, endpoint, request).await?;
    match udp::Response::decode(&bytes, family, CODEC)
        .map_err(|_| invalid())?
        .kind
    {
        udp::ResponseKind::Announce {
            interval,
            seeders,
            leechers,
            peers,
        } => {
            let peers: Vec<_> = match peers {
                udp::Peers::V4(p) if p.remainder().is_empty() => {
                    p.iter().map(SocketAddr::V4).collect()
                }
                udp::Peers::V6(p) if p.remainder().is_empty() => {
                    p.iter().map(SocketAddr::V6).collect()
                }
                _ => return Err(invalid()),
            };
            Ok(Reply {
                interval: Duration::from_secs((interval as i32).max(300) as u64),
                min_interval: Duration::from_secs(60),
                peers: peers
                    .into_iter()
                    .filter(|a| crate::engine::network::peer(*a, allow_local_network))
                    .take(PEER_CAP)
                    .collect(),
                seeders: Some(seeders as u64),
                leechers: Some(leechers as u64),
                tracker_id: None,
                warning: None,
                error: None,
                disabled: false,
            })
        }
        udp::ResponseKind::Error(message) => Err(tracker_failure(message)),
        _ => Err(invalid()),
    }
}
async fn udp_exchange(
    transport: &crate::engine::session::udp::Transport,
    endpoint: SocketAddr,
    mut request: udp::Request<'_>,
) -> io::Result<Vec<u8>> {
    let (id, _registration, mut reply) = transport.register(endpoint)?;
    request.transaction_id = id;
    let packet = request.encode(CODEC).map_err(|_| invalid())?;
    let destination = match endpoint {
        SocketAddr::V4(v4) if transport.socket.local_addr()?.is_ipv6() => {
            SocketAddr::new(v4.ip().to_ipv6_mapped().into(), v4.port())
        }
        _ => endpoint,
    };
    transport.socket.send_to(&packet, destination).await?;
    timeout(Duration::from_secs(10), &mut reply)
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
        .map_err(io::Error::other)
}

pub(super) struct Discovery {
    pub(in crate::engine) lsd: Option<crate::engine::session::lsd::Registration>,
    pub(in crate::engine) dht: Vec<crate::engine::dht::Discovery>,
    pub(in crate::engine) announces: Vec<Announcer>,
    pub(in crate::engine) contacts: Option<crate::engine::session::dial::Contacts>,
    pub(in crate::engine) dials: Option<crate::engine::session::dial::Client>,
    pub(in crate::engine) web_seeds: Option<crate::engine::webseed::WebSeeds>,
}
impl Discovery {
    pub(in crate::engine) fn empty() -> Self {
        Self {
            lsd: None,
            dht: Vec::new(),
            announces: Vec::new(),
            contacts: None,
            dials: None,
            web_seeds: None,
        }
    }
    pub(in crate::engine) async fn setup(
        &mut self,
        options: &crate::engine::torrent::RuntimeOptions,
        session: &crate::engine::torrent::Network,
        left: u64,
    ) -> io::Result<()> {
        for hash in crate::engine::core::swarm_identity(&options.metainfo).wire_keys() {
            if let Some(handle) = &options.dht {
                self.dht.push(crate::engine::dht::Discovery::new(
                    handle.clone(),
                    hash,
                    session.advertised_port.clone(),
                ));
            }
        }
        self.lsd = session
            .lsd
            .as_ref()
            .filter(|_| options.settings.dht)
            .map(|lsd| lsd.register(crate::engine::core::swarm_identity(&options.metainfo)));
        self.web_seeds = if options.settings.web_seeds && left != 0 {
            Some(crate::engine::webseed::WebSeeds::new(
                &options.metainfo,
                session.allow_local_network,
            )?)
        } else {
            None
        };
        if options.settings.trackers {
            for hash in crate::engine::core::swarm_identity(&options.metainfo).wire_keys() {
                if let Some(announcer) = Announcer::new(
                    Announcer::metainfo(&options.metainfo),
                    AnnounceOptions {
                        hash,
                        peer: options.peer_id,
                        port: session.advertised_port.clone(),
                        left,
                        allow_local_network: session.allow_local_network,
                        client: session.tracker_http.clone(),
                        udp: session.tracker_udp.clone(),
                        completed_announce: options
                            .completed_announces
                            .iter()
                            .find(|(key, _)| *key == hash)
                            .map(|(_, delivered)| *delivered),
                        checkpoint: {
                            let checkpoint = options.announce_checkpoint.clone();
                            Arc::new(move |delivered| checkpoint(hash, delivered))
                        },
                    },
                )? {
                    self.announces.push(announcer);
                }
            }
        }
        let (mut contacts, dials) = session.dial_queue.client(options.profile);
        contacts.add_direct(options.metainfo.info_hash().wire_key(), &options.peers)?;
        for &(hash, address) in &options.lsd_peers {
            contacts.add_lsd(hash, &[address])?;
        }
        self.contacts = Some(contacts);
        self.dials = Some(dials);
        Ok(())
    }
    pub(in crate::engine) async fn shutdown(
        &mut self,
        _: crate::engine::torrent::RunPolicy,
    ) -> io::Result<()> {
        self.lsd = None;
        self.dht.clear();
        let mut result = Ok(());
        for a in &mut self.announces {
            result = result.and(a.shutdown().await);
        }
        result.and(self.drain().await)
    }
    pub(in crate::engine) async fn drain(&mut self) -> io::Result<()> {
        self.lsd = None;
        self.dht.clear();
        let mut announces = Ok(());
        for a in &mut self.announces {
            announces = announces.and(a.drain().await);
        }
        self.announces.clear();
        self.contacts = None;
        let dials = if let Some(d) = &mut self.dials {
            d.shutdown().await
        } else {
            Ok(())
        };
        self.dials = None;
        announces.and(dials)
    }
}
