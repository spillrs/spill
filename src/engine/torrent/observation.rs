//! Latest-value status presentation; admission and protocol state live elsewhere.
use super::{
    TorrentDiagnostics, TorrentExit, TorrentPhase, TorrentStatus, discovery::Discovery,
    shell::Shell, telemetry,
};
use crate::engine::{core::Event, peer_io::ConnectionId};
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

pub(super) fn apply_progress(
    p: &crate::engine::core::Progress,
    target: &mut super::TorrentProgress,
) {
    target.verified_bytes = p.verified_bytes;
    target.wanted_bytes = p.wanted_bytes;
    target.wanted_verified_bytes = p.wanted_verified_bytes;
    target.processed_payload_bytes = p.received_payload_bytes;
    target.hash_failed_bytes = p.hash_failed_bytes;
    target.hash_failed_pieces = p.hash_failed_pieces;
}

pub(super) fn tracker_counters(transfer: &Shell) -> io::Result<crate::engine::discovery::Counters> {
    let p = transfer.progress()?;
    Ok(crate::engine::discovery::Counters {
        // Failed-piece bytes include padding synthesized without receiving it.
        downloaded: p.received_payload_bytes.saturating_sub(p.hash_failed_bytes),
        corrupt: p.hash_failed_bytes,
        uploaded: transfer.uploaded_bytes(),
        left: p.remaining_bytes(),
        partial_seed: p.partial_seed(),
    })
}
pub(super) struct Peer {
    pub(super) address: Option<SocketAddr>,
    pub(super) direction: &'static str,
    pub(super) transport: &'static str,
    pub(super) retransmits: Option<Arc<AtomicU64>>,
    pub(super) source: &'static str,
    pub(super) web_seed_url: Option<String>,
    pub(super) sampled: Instant,
    pub(super) downloaded: u64,
    pub(super) uploaded: u64,
}
pub(super) struct Observer {
    status: Status,
    seeding_since: Option<Instant>,
    pub(super) active: usize,
    endpoints: HashMap<ConnectionId, Peer>,
    history: HashMap<String, (Instant, Option<String>)>,
    map_count: Option<u32>,
    closed_retransmits: u64,
    sampled: Instant,
    downloaded: u64,
    uploaded: u64,
}
impl Drop for Observer {
    fn drop(&mut self) {
        if let Some(since) = self.seeding_since {
            self.status
                .send_modify(|s| s.progress.seeding_duration = since.elapsed());
        }
    }
}
impl Observer {
    pub(super) fn new(status: Status) -> Self {
        Self {
            status,
            seeding_since: None,
            active: 0,
            endpoints: HashMap::new(),
            history: HashMap::new(),
            map_count: None,
            closed_retransmits: 0,
            sampled: Instant::now(),
            downloaded: 0,
            uploaded: 0,
        }
    }
    pub(super) fn retain_peers(&mut self, transfer: &Shell) {
        self.endpoints.retain(|id, _| transfer.has_peer(*id));
        self.active = self.endpoints.len();
    }
    pub(super) fn publish_peers(&self, status: &Status) {
        status.send_modify(|s| {
            s.progress.active_peers = self.active;
        });
    }
    pub(super) fn remember(&mut self, id: ConnectionId, peer: Peer) {
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
    pub(super) fn event(
        &mut self,
        event: Event,
        transfer: &Shell,
        status: &Status,
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
                connection,
                index,
                begin,
                length,
            } => {
                trace!(?connection, index, begin, length, "block_accepted");
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
                                result.as_ref().err().map(ToString::to_string),
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
                if let Err(error) = &result
                    && matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::TimedOut
                    )
                {
                    debug!(?connection, kind=?error.kind(), %error, "peer_closed");
                } else {
                    trace!(?connection,error_kind=?result.as_ref().err().map(|e|e.kind()),error=%result.as_ref().err().map(ToString::to_string).unwrap_or_default(),"peer_closed");
                }
            }
            Event::Stalled(crate::engine::core::Stall::NoProgress) => {
                return Ok(Some(TorrentExit::NoProgress));
            }
            Event::Stalled(reason) => debug!(?reason, "download_stalled"),
            Event::PieceRetry {
                index,
                retry,
                corrupt,
                source,
            } => debug!(index, retry, corrupt, ?source, "piece_retry"),
            Event::PeerReplaced {
                connection,
                replacement,
                rank,
                replacement_rank,
            } => trace!(
                ?connection,
                ?replacement,
                rank,
                replacement_rank,
                "peer_replaced"
            ),
            Event::PieceWithdrawn { connection, index } => {
                trace!(?connection, index, "piece_withdrawn");
            }
            Event::PeerReady(connection) => trace!(?connection, "peer_ready"),
            Event::IgnoredBlock { index, begin } => trace!(index, begin, "block_ignored"),
        }
        let p = transfer.progress()?;
        status.send_modify(|s| {
            apply_progress(&p, &mut s.progress);
            s.progress.full_complete = p.verified_bytes == p.total_bytes;
            s.progress.verified_pieces = transfer.verified_count();
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
    pub(super) fn report(
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
            p.received_payload_bytes.saturating_sub(self.downloaded) as f64 / elapsed;
        let upload_rate = uploaded.saturating_sub(self.uploaded) as f64 / elapsed;
        self.sampled = Instant::now();
        self.downloaded = p.received_payload_bytes;
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
            let mut holepunch = transfer.holepunch_status();
            if let Some(contacts) = &discovery.contacts {
                holepunch.punches_queued = contacts.punches_queued;
                holepunch.punches_expired = contacts.punches_expired;
            }
            Some(Arc::new(TorrentDiagnostics {
                sampled_at: Some(Instant::now().into_std()),
                storage: transfer.storage_status(),
                holepunch,
                metadata_error: status
                    .borrow()
                    .diagnostics
                    .as_ref()
                    .and_then(|d| d.metadata_error.clone()),
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
            apply_progress(&p, &mut s.progress);
            s.progress.total_bytes = p.total_bytes;
            s.progress.written_payload_bytes = uploaded;
            s.progress.download_bytes_per_second = download_rate;
            s.progress.upload_bytes_per_second = upload_rate;
            s.progress.verified_pieces = transfer.verified_count();
            s.progress.active_peers = self.active;
        });
        Ok(())
    }
}
