//! Sequential BEP 12 announces over session-owned transports.
use crate::engine::{core::AnnounceEvent, torrent::telemetry::TrackerStatus};
use crate::protocol::{
    bencode::{self, Value},
    identity::InfoHashV1,
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

const MIN: Duration = Duration::from_secs(30);
const MAX: Duration = Duration::from_secs(3600);
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
}
struct Tracker {
    url: Option<Url>,
    started: bool,
    tracker_id: Option<Vec<u8>>,
    status: TrackerStatus,
    last_reply: Option<Instant>,
    next_allowed: Instant,
}
pub(in crate::engine) struct AnnounceOptions {
    pub(in crate::engine) hash: InfoHashV1,
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
    tiers: Vec<Vec<Tracker>>,
    hash: InfoHashV1,
    peer: [u8; 20],
    port: tokio::sync::watch::Receiver<u16>,
    key: u32,
    pub(in crate::engine) counters: Counters,
    allow_local_network: bool,
    client: reqwest::Client,
    udp: crate::engine::session::udp::Transport,
    due: Instant,
    backoff: Duration,
    completed_pending: bool,
    completed_acknowledged: bool,
    checkpoint: Arc<dyn Fn(bool) -> io::Result<()> + Send + Sync>,
    // A select! cancellation must not abandon an in-progress acknowledgement.
    checkpoint_job: Option<(bool, tokio::task::JoinHandle<io::Result<()>>)>,
    sent_event: Option<AnnounceEvent>,
    cursor: (usize, usize),
    pending: Option<Pending>,
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
        for (tier, urls) in tiers.into_iter().enumerate() {
            let mut trackers: Vec<_> = urls
                .into_iter()
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
                        status: TrackerStatus {
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
            // Fisher-Yates, independently within each tier (BEP 12).
            for i in (1..trackers.len()).rev() {
                trackers.swap(i, random()? as usize % (i + 1));
            }
            if !trackers.is_empty() {
                parsed.push(trackers);
            }
        }
        if parsed.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            tiers: parsed,
            hash,
            peer,
            port,
            key: random()?,
            counters: Counters {
                downloaded: 0,
                uploaded: 0,
                left,
            },
            allow_local_network,
            client,
            udp,
            due: Instant::now(),
            backoff: MIN,
            completed_pending: completed_announce == Some(false),
            completed_acknowledged: completed_announce == Some(true),
            checkpoint,
            checkpoint_job: None,
            sent_event: None,
            cursor: (0, 0),
            pending: None,
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
        if self.pending.is_none() {
            self.due = Instant::now();
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
        if self.pending.is_none() {
            let (tier, slot) = self.cursor;
            self.due = Instant::now().max(self.tiers[tier][slot].next_allowed);
        }
    }
    pub(in crate::engine) fn snapshots(&self) -> Vec<TrackerStatus> {
        self.tiers
            .iter()
            .flatten()
            .map(|t| {
                let mut status = t.status.clone();
                status.last_reply_ago_ms = t.last_reply.map(|at| at.elapsed().as_millis() as u64);
                status.next_announce_ms = Some(
                    self.due
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64,
                );
                status
            })
            .collect()
    }
    fn exchange(&self, tracker: &Tracker, event: Option<AnnounceEvent>) -> Pending {
        let url = tracker.url.clone();
        let allow_local_network = self.allow_local_network;
        let tracker_id = tracker.tracker_id.clone();
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
            num_want: PEER_CAP as i32,
        };
        Box::pin(async move {
            timeout(crate::engine::profile::TOTAL_TIMEOUT, async {
                let url = url.ok_or_else(invalid)?;
                if url.scheme() == "udp" {
                    udp_announce(url, a, allow_local_network, udp).await
                } else {
                    http_announce(url, a, allow_local_network, tracker_id.as_deref(), client).await
                }
            })
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
        })
    }
    /// Retain each exchange across select! cancellation; never restart network I/O on a tick.
    pub(in crate::engine) async fn next(&mut self) -> io::Result<Vec<SocketAddr>> {
        self.acknowledge().await?;
        tokio::time::sleep_until(self.due).await;
        loop {
            let (tier, slot) = self.cursor;
            let tracker = &self.tiers[tier][slot];
            if self.pending.is_none() {
                let event = if !tracker.started {
                    Some(AnnounceEvent::Started)
                } else {
                    self.completed_pending.then_some(AnnounceEvent::Completed)
                };
                if event.is_none() {
                    tokio::time::sleep_until(tracker.next_allowed).await;
                }
                self.sent_event = event;
                self.pending = Some(self.exchange(tracker, event));
            }
            let result = self.pending.as_mut().expect("exchange").await;
            self.pending = None;
            let tracker = &mut self.tiers[tier][slot];
            tracker.status.exchanges = tracker.status.exchanges.saturating_add(1);
            match result {
                Ok(reply) => {
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
                    self.due =
                        Instant::now() + reply.interval.clamp(MIN, MAX).max(reply.min_interval);
                    self.backoff = MIN;
                    let successful = self.tiers[tier].remove(slot);
                    self.tiers[tier].insert(0, successful);
                    self.cursor = (0, 0);
                    if matches!(self.sent_event, Some(AnnounceEvent::Completed)) {
                        self.start_checkpoint(true);
                        self.acknowledge().await?;
                    }
                    if self.completed_pending {
                        self.due = Instant::now();
                    }
                    return Ok(reply.peers);
                }
                Err(error) => {
                    tracker.status.state = "Error";
                    tracker.status.failures = tracker.status.failures.saturating_add(1);
                    tracker.status.peers_returned = Some(0);
                    tracker.status.error = Some(error.to_string());
                }
            }
            self.cursor.1 += 1;
            if self.cursor.1 == self.tiers[tier].len() {
                self.cursor = (tier + 1, 0);
            }
            if self.cursor.0 == self.tiers.len() {
                self.cursor = (0, 0);
                self.due = Instant::now() + self.backoff;
                self.backoff = (self.backoff * 2).min(Duration::from_secs(1800));
                return Ok(Vec::new());
            }
        }
    }
    pub(in crate::engine) async fn drain(&mut self) -> io::Result<()> {
        self.pending = None;
        self.join_checkpoint().await.map(|_| ())
    }
    pub(in crate::engine) async fn shutdown(&mut self) -> io::Result<()> {
        let checkpoint = self.drain().await;
        let exchanges: Vec<_> = self
            .tiers
            .iter()
            .flatten()
            .filter(|t| t.started)
            .map(|tracker| self.exchange(tracker, Some(AnnounceEvent::Stopped)))
            .collect();
        let _ = timeout(Duration::from_secs(5), async move {
            for exchange in exchanges {
                let _ = exchange.await;
            }
        })
        .await;
        checkpoint
    }
}

// Resolve afresh for each request, filtering the actual connection destinations.
// No pins, proxy, redirects, or secondary unfiltered resolution.
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
        .user_agent(concat!("Spill/", env!("CARGO_PKG_VERSION")))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .pool_idle_timeout(Duration::from_secs(90))
        .dns_resolver(Arc::new(Resolver(allow_local_network)))
        .connect_timeout(crate::engine::profile::CONNECT_TIMEOUT)
        .build()
        .map_err(io::Error::other)
}
async fn http_announce(
    mut url: Url,
    a: udp::Announce,
    allow_local_network: bool,
    tracker_id: Option<&[u8]>,
    client: reqwest::Client,
) -> io::Result<Reply> {
    if let Ok(ip) = url
        .host_str()
        .unwrap_or_default()
        .trim_matches(['[', ']'])
        .parse()
        && !crate::engine::network::source(
            SocketAddr::new(ip, url.port_or_known_default().ok_or_else(invalid)?),
            allow_local_network,
        )
    {
        return Err(invalid());
    }
    let request = http::AnnounceRequest {
        info_hash: a.info_hash,
        peer_id: a.peer_id,
        port: a.port,
        uploaded: a.uploaded,
        downloaded: a.downloaded,
        left: a.left,
        event: match a.event {
            udp::EVENT_STARTED => Some(http::AnnounceEvent::Started),
            udp::EVENT_COMPLETED => Some(http::AnnounceEvent::Completed),
            udp::EVENT_STOPPED => Some(http::AnnounceEvent::Stopped),
            _ => None,
        },
        numwant: Some(PEER_CAP as u32),
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
    if !response.status().is_success()
        || response
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
    if let Some(reason) = reply.failure_reason() {
        return Err(tracker_failure(reason));
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
        None if document.get(b"peers6").is_some() => {}
        _ => return Err(invalid()),
    }
    if document
        .get(b"peers6")
        .is_some_and(|n| !matches!(n.value(), Value::Bytes(_)))
    {
        return Err(invalid());
    }
    let interval = Duration::from_secs(reply.interval_secs().ok_or_else(invalid)?);
    let min_interval = Duration::from_secs(u64::from(
        u32::try_from(reply.min_interval_secs().unwrap_or(30)).map_err(|_| invalid())?,
    ))
    .max(MIN);
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
                interval: Duration::from_secs(interval as u64),
                min_interval: MIN,
                peers: peers
                    .into_iter()
                    .filter(|a| crate::engine::network::peer(*a, allow_local_network))
                    .take(PEER_CAP)
                    .collect(),
                seeders: Some(seeders as u64),
                leechers: Some(leechers as u64),
                tracker_id: None,
                warning: None,
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
    let mut delay = Duration::from_secs(15);
    loop {
        transport.socket.send_to(&packet, destination).await?;
        match timeout(delay, &mut reply).await {
            Ok(bytes) => return bytes.map_err(io::Error::other),
            Err(_) => delay *= 2,
        }
    }
}

pub(super) struct Discovery {
    pub(in crate::engine) dht: Option<crate::engine::dht::Discovery>,
    pub(in crate::engine) announces: Option<Announcer>,
    pub(in crate::engine) contacts: Option<crate::engine::session::dial::Contacts>,
    pub(in crate::engine) dials: Option<crate::engine::session::dial::Client>,
    pub(in crate::engine) web_seeds: Option<crate::engine::webseed::WebSeeds>,
}
impl Discovery {
    pub(in crate::engine) fn empty() -> Self {
        Self {
            dht: None,
            announces: None,
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
        self.dht = options
            .dht
            .clone()
            .map(|handle| {
                crate::engine::dht::Discovery::new(
                    handle,
                    options.metainfo.clone(),
                    session.advertised_port.clone(),
                )
            })
            .transpose()?;
        self.web_seeds = if options.settings.web_seeds && left != 0 {
            Some(crate::engine::webseed::WebSeeds::new(
                &options.metainfo,
                session.allow_local_network,
            )?)
        } else {
            None
        };
        self.announces = if options.settings.trackers {
            Announcer::new(
                Announcer::metainfo(&options.metainfo),
                AnnounceOptions {
                    hash: options.metainfo.info_hash(),
                    peer: options.peer_id,
                    port: session.advertised_port.clone(),
                    left,
                    allow_local_network: session.allow_local_network,
                    client: session.tracker_http.clone(),
                    udp: session.tracker_udp.clone(),
                    completed_announce: options.completed_announce,
                    checkpoint: options.announce_checkpoint.clone(),
                },
            )?
        } else {
            None
        };
        let (mut contacts, dials) = session.dial_queue.client(options.profile);
        contacts.add_direct(&options.peers)?;
        self.contacts = Some(contacts);
        self.dials = Some(dials);
        Ok(())
    }
    pub(in crate::engine) async fn shutdown(
        &mut self,
        _: crate::engine::torrent::RunPolicy,
    ) -> io::Result<()> {
        self.dht = None;
        let announces = if let Some(a) = &mut self.announces {
            a.shutdown().await
        } else {
            Ok(())
        };
        announces.and(self.drain().await)
    }
    pub(in crate::engine) async fn drain(&mut self) -> io::Result<()> {
        self.dht = None;
        let announces = if let Some(a) = &mut self.announces {
            a.drain().await
        } else {
            Ok(())
        };
        self.announces = None;
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
