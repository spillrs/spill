use super::{
    Network, Networking, RunPolicy, RuntimeOptions, TorrentExit, TorrentPhase, TorrentStatus,
    observation::{self, Progress},
};
use crate::engine::peer_io::Role;
use crate::engine::{discovery::Discovery, torrent::shell::Shell as Task};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};
use tracing::{info, warn};

type Status = watch::Sender<TorrentStatus>;

fn tracker_event(peers: Vec<std::net::SocketAddr>, discovery: &mut Discovery) {
    if let Some(queue) = &mut discovery.contacts {
        let _ = queue.add_tracker(&peers);
    }
}

async fn start_discovery(
    options: &RuntimeOptions,
    transfer: &mut Task,
    network: &mut Discovery,
    stop: &mut watch::Receiver<bool>,
    session: &Network,
) -> io::Result<Option<TorrentExit>> {
    let setup = network.setup(options, session, transfer.progress()?.remaining_bytes());
    tokio::select! {
        biased;
        _ = stop.changed() => return Ok(Some(TorrentExit::StopRequested)),
        result = setup => result?,
    }
    Ok(None)
}

async fn drive(
    options: &mut RuntimeOptions,
    session: Network,
    transfer: &mut Task,
    network: &mut Discovery,
    stop: &mut watch::Receiver<bool>,
    status: &Status,
) -> io::Result<TorrentExit> {
    transfer.set_bandwidth(options.bandwidth.clone());
    if *stop.borrow() {
        return Ok(TorrentExit::StopRequested);
    }
    if options.settings.dht
        && let Some(dht) = &session.dht
    {
        options.dht = Some(tokio::select! {
            biased;
            _ = stop.changed() => return Ok(TorrentExit::StopRequested),
            handle = dht.get() => handle?,
        });
    }
    let port = *session.advertised_port.borrow();
    transfer.set_no_progress_timeout(options.no_progress_timeout);
    transfer.set_advertised_port(session.advertised_port.clone());
    transfer.set_dht_port(options.dht.as_ref().map(|h| h.local_addr().port()));
    transfer.settings(options.settings.clone())?;
    let p = transfer.progress()?;
    status.send_modify(|s| {
        s.progress.phase = TorrentPhase::Downloading;
        s.progress.verified_bytes = p.verified_bytes();
        s.progress.total_bytes = p.total_bytes();
        s.progress.wanted_bytes = p.wanted_bytes();
        s.progress.wanted_verified_bytes = p.wanted_verified_bytes();
        s.progress.processed_payload_bytes = p.received_payload_bytes();
        s.progress.hash_failed_bytes = p.hash_failed_bytes();
        s.progress.hash_failed_pieces = p.hash_failed_pieces();
    });
    info!(listen_port = port, "transfer_listening");
    if let Some(exit) = start_discovery(options, transfer, network, stop, &session).await? {
        return Ok(exit);
    }
    status.send_modify(|s| {
        s.progress.ready = true;
    });
    let inbound = session.sockets;
    serve(options, transfer, network, stop, status, inbound).await
}

async fn serve(
    options: &RuntimeOptions,
    transfer: &mut Task,
    network: &mut Discovery,
    stop: &mut watch::Receiver<bool>,
    status: &Status,
    mut inbound: tokio::sync::mpsc::Receiver<crate::engine::session::Inbound>,
) -> io::Result<TorrentExit> {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut progress = Progress::new(status.clone());
    progress.restore_verified(transfer);
    let mut log_tick = tokio::time::interval_at(
        Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    log_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut dial_tick = tokio::time::interval(Duration::from_millis(250));
    dial_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut reannounce = options.reannounce.clone();
    let mut inbound_open = true;
    let mut discovery_dirty = true;
    let mut observation_dirty = true;
    let mut completion_deadline = None;
    loop {
        if *stop.borrow() {
            return Ok(TorrentExit::StopRequested);
        }
        if observation_dirty {
            if options.diagnostics {
                let retry = transfer.retry_status();
                status.send_if_modified(|s| {
                    let Some(diagnostics) = &mut s.diagnostics else {
                        return false;
                    };
                    if diagnostics.retry == retry {
                        return false;
                    }
                    std::sync::Arc::make_mut(diagnostics).retry = retry;
                    true
                });
            }
            observation_dirty = false;
        }
        progress.update_counters(transfer, network).await?;
        if options.run == RunPolicy::UntilComplete && status.borrow().progress.completed {
            if !network
                .announces
                .as_ref()
                .is_some_and(|a| a.completion_pending())
            {
                return Ok(TorrentExit::Complete);
            }
            let deadline =
                completion_deadline.get_or_insert_with(|| Instant::now() + Duration::from_secs(5));
            if Instant::now() >= *deadline {
                return Ok(TorrentExit::Complete);
            }
        }
        if discovery_dirty {
            progress.prepare_discovery(transfer, network, options)?;
            discovery_dirty = false;
        }
        progress.prepare_dials(transfer, network, options)?;
        progress.publish_peers(status);
        let announces = &mut network.announces;
        let tracker = async {
            let Some(a) = announces else {
                return std::future::pending().await;
            };
            a.next().await
        };
        let dht = async {
            let Some(dht) = &mut network.dht else {
                return std::future::pending().await;
            };
            dht.next_event().await
        };
        let dial_turn = network.dials.as_ref().map(|d| d.turn.clone());
        let direct = async {
            let Some(d) = &mut network.dials else {
                return std::future::pending().await;
            };
            d.next().await
        };
        tokio::select! {
            biased;
            _ = stop.changed() => return Ok(TorrentExit::StopRequested),
            _ = reannounce.changed() => {
                if let Some(a) = &mut network.announces {
                    a.reannounce();
                }
                discovery_dirty = true;
            },
            event = tracker => {
                tracker_event(event?, network);
                discovery_dirty = true;
            },
            result = dht => {
                discovery_dirty = true;
                match result {
                    Ok(crate::engine::dht::DiscoveryEvent::Peers(peers)) => {
                        if let Some(queue) = &mut network.contacts {
                            let peers: Vec<_> = peers.into_iter().map(std::net::SocketAddr::V4).collect();
                            let _ = queue.add_dht(options.metainfo.info_hash(), &peers);
                        }
                    }
                    Ok(crate::engine::dht::DiscoveryEvent::Finished(found)) => {
                        info!(queried = found.queried, announced = found.announced, peers = found.peers.len(), "dht_lookup_finished");
                        // Include peers produced in the lookup's final poll. The
                        // contact queue deduplicates earlier streamed results.
                        if let Some(queue) = &mut network.contacts {
                            let peers: Vec<_> = found.peers.into_iter().map(std::net::SocketAddr::V4).collect();
                            let _ = queue.add_dht(options.metainfo.info_hash(), &peers);
                        }
                    }
                    Err(error) => warn!(kind = ?error.kind(), "dht_lookup_failed"),
                }
            },
            event = direct, if progress.pending > 0 => {
                let event = event?;
                if let Some(event) = &event && let Some(q) = &mut network.contacts {
                    q.dial_completed(event.address, event.result.is_ok(), event.utp_failed);
                }
                progress.direct_event(event, transfer, options)?;
                discovery_dirty = true;
            }
            socket = inbound.recv(), if inbound_open => {
                if let Some(socket) = socket {
                    let socket = match socket {
                        crate::engine::session::Inbound::Tcp(socket) => match tokio::net::TcpStream::from_std(socket) {
                            Ok(socket) => crate::engine::peer_io::Stream::Tcp(socket),
                            Err(error) => { warn!(kind = ?error.kind(), "peer_register_failed"); continue; }
                        },
                        crate::engine::session::Inbound::Utp(socket) => crate::engine::peer_io::Stream::Utp(socket),
                    };
                    progress.attach_peer(socket, Role::Inbound, transfer, "inbound", options.log_peer_endpoints, None);
                    discovery_dirty = true;
                    progress.publish_peers(status);
                } else {
                    inbound_open = false;
                }
            }
            event = transfer.next() => {
                let Some(event) = event? else { discovery_dirty = true; observation_dirty = true; continue; };
                if !matches!(event, crate::engine::core::Event::Block { .. } | crate::engine::core::Event::IgnoredBlock { .. }) {
                    discovery_dirty = true;
                    observation_dirty = true;
                }
                if let Some(exit) = progress.event(event, transfer, status, options)? {
                    return Ok(exit);
                }
            }
            _ = async {
                if let Some(turn) = dial_turn { turn.changed().await; }
                else { std::future::pending::<()>().await; }
            } => {},
            _ = dial_tick.tick() => { discovery_dirty = true; observation_dirty = true; },
            _ = tick.tick() => progress.report(transfer, network, status)?,
            _ = log_tick.tick() => progress.log_summary(transfer, network)?,
            _ = async {
                if let Some(deadline) = completion_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => return Ok(TorrentExit::Complete),
        }
    }
}

async fn finish(
    transfer: &mut Task,
    network: &mut Discovery,
    status: &Status,
    run: RunPolicy,
    piece_count: u32,
    result: io::Result<TorrentExit>,
) -> io::Result<()> {
    status.send_modify(|s| {
        s.progress.phase = TorrentPhase::Stopping;
        s.progress.exit = result.as_ref().ok().copied();
    });
    observation::log_peers(transfer);
    let setup = network.shutdown(run).await;
    let peers = transfer.shutdown().await;
    let p = transfer.progress();
    let uploaded = transfer.uploaded_bytes();
    let pieces: std::sync::Arc<[bool]> = (0..piece_count).map(|i| transfer.contains(i)).collect();
    status.send_modify(|s| {
        s.progress.phase = TorrentPhase::Stopped;
        s.progress.verified_pieces = pieces.iter().filter(|&&p| p).count() as u32;
        s.progress.verified_piece_map = pieces;
        if let Ok(p) = &p {
            s.progress.verified_bytes = p.verified_bytes();
            s.progress.full_complete = p.verified_bytes() == p.total_bytes();
            s.progress.total_bytes = p.total_bytes();
            s.progress.wanted_bytes = p.wanted_bytes();
            s.progress.wanted_verified_bytes = p.wanted_verified_bytes();
            s.progress.processed_payload_bytes = p.received_payload_bytes();
            s.progress.hash_failed_bytes = p.hash_failed_bytes();
            s.progress.hash_failed_pieces = p.hash_failed_pieces();
        }
        s.progress.written_payload_bytes = uploaded;
        s.progress.download_bytes_per_second = 0.0;
        s.progress.upload_bytes_per_second = 0.0;
        s.progress.active_peers = 0;
        if let Some(diagnostics) = &mut s.diagnostics {
            let diagnostics = std::sync::Arc::make_mut(diagnostics);
            diagnostics.peers.clear();
            diagnostics.trackers.clear();
            diagnostics.web_seeds.clear();
            diagnostics.dht = None;
        }
    });
    info!(clean = result.is_ok(),
        exit = ?result.as_ref().ok(), error_kind = ?result.as_ref().err().map(|e| e.kind()),
        "torrent_runtime_joined");
    result.map(|_| ()).and(setup).and(peers).and(p.map(|_| ()))
}

pub(super) struct Resources {
    pub(super) writer: Option<crate::engine::storage::writer::Writer>,
    transfer: Option<Task>,
    network: Discovery,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            writer: None,
            transfer: None,
            network: Discovery::empty(),
        }
    }
}
impl Resources {
    pub(super) async fn drain(&mut self) -> io::Result<()> {
        let network = self.network.drain().await;
        let transfer = if let Some(transfer) = &mut self.transfer {
            transfer.drain().await
        } else {
            Ok(())
        };
        let writer = if let Some(writer) = &mut self.writer {
            writer.shutdown().await
        } else {
            Ok(())
        };
        self.transfer = None;
        self.writer = None;
        network.and(transfer).and(writer)
    }
}

pub(super) async fn run(
    resources: &mut Resources,
    mut options: RuntimeOptions,
    networking: Networking,
    mut stop: watch::Receiver<bool>,
    status: watch::Sender<TorrentStatus>,
) -> io::Result<()> {
    let Resources {
        writer: writer_slot,
        transfer,
        network,
    } = resources;
    let writer = writer_slot.as_mut().expect("startup writer");
    status.send_modify(|s| s.progress.phase = TorrentPhase::Recovering);
    let result = tokio::select! {
        biased;
        _ = stop.changed() => Ok(None),
        result = writer.ready() => result.map(Some),
    };
    let verified = match result {
        Ok(Some(verified)) => verified,
        other => {
            status.send_modify(|s| {
                s.progress.phase = TorrentPhase::Stopped;
                s.progress.exit = other.as_ref().ok().map(|_| TorrentExit::StopRequested);
            });
            return other.map(|_| ());
        }
    };
    let pieces: Arc<[bool]> = (0..options.metainfo.layout().piece_count())
        .map(|i| verified[i as usize / 8] & (0x80 >> (i % 8)) != 0)
        .collect();
    let bytes: u64 = pieces
        .iter()
        .enumerate()
        .filter(|(_, passed)| **passed)
        .map(|(i, _)| {
            let range = options
                .metainfo
                .layout()
                .piece_range(i as u32)
                .expect("piece");
            range.end - range.start
        })
        .sum();
    status.send_modify(|s| {
        s.progress.recovered = true;
        s.progress.verified_bytes = bytes;
        s.progress.full_complete = bytes == options.metainfo.layout().total_length();
        s.progress.verified_pieces = pieces.iter().filter(|&&p| p).count() as u32;
        s.progress.verified_piece_map = pieces;
        s.progress.wanted_bytes = options.selection.wanted_bytes();
        s.progress.wanted_verified_bytes = options
            .selection
            .verified_bytes(options.metainfo.layout(), &verified);
    });
    let session = match networking {
        Networking::Recheck => {
            let complete = options.selection.is_complete(&verified);
            let joined = writer.shutdown().await;
            status.send_modify(|s| {
                s.progress.phase = TorrentPhase::Stopped;
                s.progress.completed = complete && joined.is_ok();
                s.progress.exit = Some(TorrentExit::Rechecked);
            });
            return joined;
        }
        Networking::Online(session) => session,
    };
    let mut download = Task::new(
        options.metainfo.clone(),
        options.peer_id,
        options.profile,
        writer,
        verified,
    )?;
    download.select_files(options.selection.clone());
    *transfer = Some(download);
    let transfer = transfer.as_mut().expect("installed shell");
    transfer.attach_writer(writer_slot.take().expect("startup writer"));
    let result = drive(&mut options, session, transfer, network, &mut stop, &status).await;
    finish(
        transfer,
        network,
        &status,
        options.run,
        options.metainfo.layout().piece_count(),
        result,
    )
    .await
}
