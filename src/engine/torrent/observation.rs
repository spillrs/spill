//! Discovery handoff and latest-value presentation. Piece/protocol state lives in core.
use super::{
    RuntimeOptions, TorrentDiagnostics, TorrentExit, TorrentPhase, TorrentStatus, shell::Shell,
    telemetry,
};
use crate::engine::{
    core::Event,
    discovery::Discovery,
    peer_io::{ConnectionId, PeerSocket, Role},
    session::dial::{ConnectionEvent, DialId},
};
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{sync::watch, time::Instant};
use tracing::{debug, info, trace};
type Status = watch::Sender<TorrentStatus>;
struct Peer {
    address: Option<SocketAddr>,
    direction: &'static str,
    transport: &'static str,
    retransmits: Option<Arc<AtomicU64>>,
    source: &'static str,
    web_seed_url: Option<String>,
    sampled: Instant,
    downloaded: u64,
    uploaded: u64,
}
pub(in crate::engine) struct Progress {
    status: Status,
    seeding_since: Option<Instant>,
    pub(in crate::engine) active: usize,
    pub(in crate::engine) pending: usize,
    endpoints: HashMap<ConnectionId, Peer>,
    sources: HashMap<DialId, (&'static str, [u8; 20])>,
    history: HashMap<String, (Instant, Option<String>)>,
    map_count: Option<u32>,
    closed_retransmits: u64,
    sampled: Instant,
    downloaded: u64,
    uploaded: u64,
}
impl Drop for Progress {
    fn drop(&mut self) {
        if let Some(since) = self.seeding_since {
            self.status
                .send_modify(|s| s.progress.seeding_duration = since.elapsed());
        }
    }
}
impl Progress {
    pub(in crate::engine) fn new(status: Status) -> Self {
        Self {
            status,
            seeding_since: None,
            active: 0,
            pending: 0,
            endpoints: HashMap::new(),
            sources: HashMap::new(),
            history: HashMap::new(),
            map_count: None,
            closed_retransmits: 0,
            sampled: Instant::now(),
            downloaded: 0,
            uploaded: 0,
        }
    }
    pub(in crate::engine) fn retain_peers(&mut self, transfer: &Shell) {
        self.endpoints.retain(|id, _| transfer.has_peer(*id));
        self.active = self.endpoints.len();
    }
    pub(in crate::engine) fn publish_peers(&self, status: &Status) {
        status.send_modify(|s| {
            s.progress.active_peers = self.active;
        });
    }
    pub(in crate::engine) async fn update_counters(
        &self,
        transfer: &mut Shell,
        discovery: &mut Discovery,
    ) -> io::Result<()> {
        let events = transfer.take_announces();
        for manager in &mut discovery.announces {
            let p = transfer.progress()?;
            manager.counters = crate::engine::discovery::Counters {
                downloaded: p.received_payload_bytes(),
                uploaded: transfer.uploaded_bytes(),
                left: p.remaining_bytes(),
            };
            for event in &events {
                manager.request(*event).await?;
            }
        }
        Ok(())
    }
    fn remember(&mut self, id: ConnectionId, peer: Peer) {
        self.endpoints.insert(id, peer);
        self.active += 1;
    }
    fn retransmits(&self) -> u64 {
        self.closed_retransmits
            + self
                .endpoints
                .values()
                .filter_map(|p| p.retransmits.as_ref())
                .map(|n| n.load(Ordering::Relaxed))
                .sum::<u64>()
    }
    pub(in crate::engine) fn attach_peer(
        &mut self,
        socket: crate::engine::peer_io::Stream,
        role: Role,
        transfer: &mut Shell,
        source: &'static str,
        log_endpoint: bool,
        prepared: Option<crate::engine::peer_io::Prepared>,
    ) -> io::Result<bool> {
        let address = socket.peer_addr().ok();
        let transport = socket.transport().as_str();
        let retransmits = match &socket {
            crate::engine::peer_io::Stream::Utp(s) => Some(s.retransmits()),
            crate::engine::peer_io::Stream::Tcp(_) => None,
        };
        match transfer.attach_peer(socket, role, prepared) {
            Ok(id) => {
                trace!(connection=?id,source,transport,direction=if matches!(role,Role::Inbound){"inbound"}else{"outbound"},endpoint=?address.filter(|_|log_endpoint),"peer_admitted");
                self.remember(
                    id,
                    Peer {
                        address,
                        direction: if matches!(role, Role::Inbound) {
                            "inbound"
                        } else {
                            "outbound"
                        },
                        source,
                        transport,
                        retransmits,
                        web_seed_url: None,
                        sampled: Instant::now(),
                        downloaded: 0,
                        uploaded: 0,
                    },
                );
                Ok(true)
            }
            Err(error) => {
                if crate::engine::core::fault::is_local(&error) {
                    return Err(error);
                }
                trace!(kind=?error.kind(),"peer_rejected");
                Ok(false)
            }
        }
    }
    pub(in crate::engine) fn prepare_discovery(
        &mut self,
        transfer: &mut Shell,
        discovery: &mut Discovery,
        options: &RuntimeOptions,
    ) -> io::Result<()> {
        if let Some(seeds) = &mut discovery.web_seeds {
            seeds.active.retain(|id| transfer.has_peer(*id));
            let slots = options
                .profile
                .peers
                .min(crate::engine::webseed::MAX_ACTIVE);
            let web_slots = if options.settings.trackers
                || options.dht.is_some()
                || !options.peers.is_empty()
            {
                slots.div_ceil(2)
            } else {
                slots
            };
            while transfer.can_connect() && seeds.active.len() < web_slots {
                let Some(seed) = seeds.next(options.metainfo.clone()) else {
                    break;
                };
                let url = seed.url().to_owned();
                let id = transfer.attach_socket(PeerSocket::WebSeed(Box::new(seed)))?;
                seeds.active.push(id);
                self.remember(
                    id,
                    Peer {
                        address: None,
                        direction: "outbound",
                        source: "web_seed",
                        transport: "http",
                        retransmits: None,
                        web_seed_url: Some(url),
                        sampled: Instant::now(),
                        downloaded: 0,
                        uploaded: 0,
                    },
                );
                trace!(connection=?id,source="web_seed","web_seed_admitted");
            }
        }
        if let Some(queue) = &mut discovery.contacts {
            transfer.drain_pex(queue);
        }
        if let Some(dht) = &options.dht {
            transfer.drain_dht(dht);
        }
        Ok(())
    }
    pub(in crate::engine) fn prepare_dials(
        &mut self,
        transfer: &mut Shell,
        discovery: &mut Discovery,
        options: &RuntimeOptions,
    ) -> io::Result<()> {
        let (Some(queue), Some(dials)) = (&mut discovery.contacts, &mut discovery.dials) else {
            return Ok(());
        };
        while transfer.can_connect() && dials.ready() {
            let Some(entry) = queue.candidate(|a| transfer.rejects_contact(a)) else {
                dials.withdraw();
                return Ok(());
            };
            let Some(address) = transfer.contact(entry.address)? else {
                queue.dial_completed(entry.address, None, false);
                dials.withdraw();
                return Ok(());
            };
            let utp = queue.try_utp(address);
            match dials.admit(
                address,
                utp,
                queue.fallback(address),
                queue.methods(address),
            ) {
                Ok(id) => {
                    self.sources.insert(id, (entry.source, entry.hash));
                    self.pending += 1;
                    trace!(?id,source=entry.source,transport=dials.transport(utp),endpoint=?options.log_peer_endpoints.then_some(address),"peer_dial_started");
                }
                Err(_) => {
                    transfer.connect_failed(address)?;
                    queue.dial_completed(address, None, false);
                    return Ok(());
                }
            }
        }
        if !transfer.can_connect() {
            dials.withdraw();
        }
        Ok(())
    }
    pub(in crate::engine) fn direct_event(
        &mut self,
        event: Option<ConnectionEvent>,
        transfer: &mut Shell,
        options: &RuntimeOptions,
        contacts: &mut crate::engine::session::dial::Contacts,
    ) -> io::Result<()> {
        let event = event.ok_or_else(|| io::Error::other("unexpected dial completion"))?;
        self.pending = self.pending.saturating_sub(1);
        let (source, hash) = self.sources.remove(&event.id).expect("admitted dial");
        match event.result {
            Ok(socket) => {
                if !self.attach_peer(
                    socket,
                    Role::Outbound(hash, event.methods, event.address),
                    transfer,
                    source,
                    options.log_peer_endpoints,
                    None,
                )? {
                    contacts.negotiated(
                        event.address,
                        crate::engine::session::dial::NegotiationOutcome::Failed,
                    );
                }
            }
            Err(error) => {
                transfer.connect_failed(event.address)?;
                trace!(dial=?event.id,source,kind=?error.kind(),os_code=error.raw_os_error(),"dial_failed");
            }
        }
        Ok(())
    }
    pub(in crate::engine) fn event(
        &mut self,
        event: Event,
        transfer: &Shell,
        status: &Status,
        _options: &RuntimeOptions,
    ) -> io::Result<Option<TorrentExit>> {
        match event {
            Event::Complete => {
                self.seeding_since.get_or_insert_with(Instant::now);
                status.send_modify(|s| {
                    s.progress.completed = true;
                    s.progress.phase = TorrentPhase::Seeding;
                });
                self.publish_map(transfer, status);
                info!("seeding_started");
            }
            Event::Verified(index) => {
                trace!(index, "piece_verified");
            }
            Event::Block {
                index,
                begin,
                length,
            } => {
                trace!(index, begin, length, "block_accepted");
            }
            Event::PeerClosed { connection, result } => {
                self.active = self.active.saturating_sub(1);
                if let Some(peer) = self.endpoints.remove(&connection) {
                    if let Some(n) = peer.retransmits {
                        self.closed_retransmits += n.load(Ordering::Relaxed);
                    }
                    if let Some(url) = peer.web_seed_url {
                        self.history.insert(
                            url,
                            (
                                Instant::now(),
                                result.as_ref().err().map(|e| format!("{:?}", e.kind())),
                            ),
                        );
                    }
                }
                let retransmits = self.retransmits();
                status.send_modify(|s| {
                    if let Some(diagnostics) = &mut s.diagnostics {
                        let diagnostics = Arc::make_mut(diagnostics);
                        diagnostics.utp_retransmits = retransmits;
                        diagnostics.peers.retain(|p| p.connection != connection.0);
                    }
                });
                trace!(?connection,error_kind=?result.as_ref().err().map(|e|e.kind()),"peer_joined");
            }
            Event::Stalled(crate::engine::core::Stall::NoProgress) => {
                return Ok(Some(TorrentExit::NoProgress));
            }
            Event::Stalled(reason) => debug!(?reason, "download_stalled"),
            Event::PieceRetry { index, retry } => debug!(index, retry, "piece_hash_failed"),
            Event::PeerReady(connection) => trace!(?connection, "peer_ready"),
            Event::IgnoredBlock { index, begin } => trace!(index, begin, "block_ignored"),
        }
        let p = transfer.progress()?;
        status.send_modify(|s| {
            s.progress.verified_bytes = p.verified_bytes();
            s.progress.full_complete = p.verified_bytes() == p.total_bytes();
            s.progress.verified_pieces = transfer.verified_count();
            s.progress.wanted_bytes = p.wanted_bytes();
            s.progress.wanted_verified_bytes = p.wanted_verified_bytes();
            s.progress.processed_payload_bytes = p.received_payload_bytes();
            s.progress.hash_failed_bytes = p.hash_failed_bytes();
            s.progress.hash_failed_pieces = p.hash_failed_pieces();
        });
        Ok(None)
    }
    fn publish_map(&mut self, transfer: &Shell, status: &Status) {
        let count = transfer.verified_count();
        if self.map_count != Some(count) {
            let pieces = transfer.piece_map();
            status.send_modify(|s| s.progress.verified_piece_map = pieces);
            self.map_count = Some(count);
        }
    }
    pub(in crate::engine) fn report(
        &mut self,
        transfer: &Shell,
        discovery: &Discovery,
        status: &Status,
    ) -> io::Result<()> {
        if let Some(since) = self.seeding_since {
            status.send_modify(|s| s.progress.seeding_duration = since.elapsed());
        }
        let p = transfer.progress()?;
        let uploaded = transfer.uploaded_bytes();
        let elapsed = self.sampled.elapsed().as_secs_f64().max(0.001);
        let download_rate =
            p.received_payload_bytes().saturating_sub(self.downloaded) as f64 / elapsed;
        let upload_rate = uploaded.saturating_sub(self.uploaded) as f64 / elapsed;
        self.sampled = Instant::now();
        self.downloaded = p.received_payload_bytes();
        self.uploaded = uploaded;
        let diagnostics = if status.borrow().diagnostics.is_some() {
            let peers = transfer
                .peer_diagnostics()
                .filter_map(|detail| {
                    let context = self.endpoints.get_mut(&ConnectionId(detail.connection))?;
                    let elapsed = context.sampled.elapsed().as_secs_f64().max(0.001);
                    let uploaded = detail.uploaded_bytes.unwrap_or(context.uploaded);
                    let peer = telemetry::PeerStatus {
                        connection: detail.connection,
                        endpoint: context.address,
                        client: transfer
                            .peer_client(ConnectionId(detail.connection))
                            .map(str::to_owned),
                        direction: context.direction,
                        transport: context.transport,
                        encryption: transfer.peer_encryption(ConnectionId(detail.connection)),
                        utp_retransmits: context
                            .retransmits
                            .as_ref()
                            .map(|n| n.load(Ordering::Relaxed)),
                        source: context.source,
                        web_seed_url: context.web_seed_url.clone(),
                        download_bytes_per_second: detail
                            .downloaded_bytes
                            .saturating_sub(context.downloaded)
                            as f64
                            / elapsed,
                        upload_bytes_per_second: uploaded.saturating_sub(context.uploaded) as f64
                            / elapsed,
                        detail,
                    };
                    context.sampled = Instant::now();
                    context.downloaded = peer.detail.downloaded_bytes;
                    context.uploaded = uploaded;
                    Some(peer)
                })
                .collect();
            let web_seeds = self
                .history
                .iter()
                .map(|(url, (at, error))| telemetry::WebSeedStatus {
                    url: url.clone(),
                    state: if error.is_some() { "Error" } else { "Closed" },
                    last_event_ago_ms: at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    error: error.clone(),
                })
                .collect();
            Some(Arc::new(TorrentDiagnostics {
                utp_retransmits: self.retransmits(),
                pending_contacts: discovery.contacts.as_ref().map_or(0, |q| q.progress()),
                writer_free_slots: transfer.writer_free_slots(),
                upload_reads_in_flight: transfer.upload_reads_in_flight(),
                upload_reads_finished: transfer.upload_reads_finished(),
                peers,
                web_seeds,
                trackers: discovery
                    .announces
                    .iter()
                    .flat_map(|a| a.snapshots())
                    .collect(),
                dht: discovery.dht.iter().map(|d| d.snapshot()).collect(),
                retry: transfer.retry_status(),
            }))
        } else {
            None
        };
        self.publish_map(transfer, status);
        status.send_modify(|s| {
            s.diagnostics = diagnostics;
            s.progress.verified_bytes = p.verified_bytes();
            s.progress.total_bytes = p.total_bytes();
            s.progress.wanted_bytes = p.wanted_bytes();
            s.progress.wanted_verified_bytes = p.wanted_verified_bytes();
            s.progress.processed_payload_bytes = p.received_payload_bytes();
            s.progress.hash_failed_bytes = p.hash_failed_bytes();
            s.progress.hash_failed_pieces = p.hash_failed_pieces();
            s.progress.written_payload_bytes = uploaded;
            s.progress.download_bytes_per_second = download_rate;
            s.progress.upload_bytes_per_second = upload_rate;
            s.progress.verified_pieces = transfer.verified_count();
            s.progress.active_peers = self.active;
        });
        Ok(())
    }
}
