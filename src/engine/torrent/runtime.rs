use super::{
    Network, Networking, RunPolicy, RuntimeOptions, TorrentExit, TorrentPhase, TorrentStatus,
    observation::Progress,
};
use crate::engine::{discovery::Discovery, torrent::shell::Shell as Task};
use crate::engine::{next_ready, peer_io::Role};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};
use tracing::{debug, trace};

type Status = watch::Sender<TorrentStatus>;

fn tracker_event(hash: [u8; 20], peers: Vec<std::net::SocketAddr>, discovery: &mut Discovery) {
    if let Some(queue) = &mut discovery.contacts {
        let _ = queue.add_tracker(hash, &peers);
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
    progress: &mut Progress,
) -> io::Result<TorrentExit> {
    transfer.set_bandwidth(options.bandwidth.clone());
    transfer.set_incoming(session.incoming.clone());
    transfer.set_transport(session.utp, session.families, options.log_peer_endpoints);
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
    transfer.set_no_progress_timeout(options.no_progress_timeout);
    transfer.set_advertised_port(session.advertised_port.clone());
    transfer.set_addresses(&session, options.dht.as_ref())?;
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
    if let Some(exit) = start_discovery(options, transfer, network, stop, &session).await? {
        return Ok(exit);
    }
    status.send_modify(|s| {
        s.progress.ready = true;
    });
    let inbound = session.sockets;
    serve(options, transfer, network, stop, status, inbound, progress).await
}

async fn serve(
    options: &RuntimeOptions,
    transfer: &mut Task,
    network: &mut Discovery,
    stop: &mut watch::Receiver<bool>,
    status: &Status,
    mut inbound: tokio::sync::mpsc::Receiver<crate::engine::session::Inbound>,
    progress: &mut Progress,
) -> io::Result<TorrentExit> {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
        if let Some(contacts) = &mut network.contacts {
            transfer.resolve_negotiations(contacts);
        }
        progress.update_counters(transfer, network).await?;
        if options.run == RunPolicy::UntilComplete && status.borrow().progress.completed {
            if !network.announces.iter().any(|a| a.completion_pending()) {
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
        let tracker = next_ready(
            network
                .announces
                .iter_mut()
                .map(|a| async move { (a.hash, a.next().await) }),
        );
        let dht = next_ready(
            network
                .dht
                .iter_mut()
                .map(|d| async move { (d.hash, d.next_event().await) }),
        );
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
                for a in &mut network.announces {
                    a.reannounce();
                }
                discovery_dirty = true;
            },
            (hash, event) = tracker => {
                tracker_event(hash, event?, network);
                discovery_dirty = true;
            },
            (hash, address) = async {
                match &mut network.lsd {
                    Some(lsd) => lsd.next().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(queue) = &mut network.contacts {
                    let _ = queue.add_lsd(hash, &[address]);
                }
                discovery_dirty = true;
            },
            (hash, result) = dht => {
                discovery_dirty = true;
                match result {
                    Ok(crate::engine::dht::DiscoveryEvent::Peers(peers)) => {
                        if let Some(queue) = &mut network.contacts {
                            let _ = queue.add_dht(hash, &peers);
                        }
                    }
                    Ok(crate::engine::dht::DiscoveryEvent::Finished(found)) => {
                        trace!(queried = found.queried, announced = found.announced, peers = found.peers.len(), "dht_lookup_finished");
                        // Include peers produced in the lookup's final poll. The
                        // contact queue deduplicates earlier streamed results.
                        if let Some(queue) = &mut network.contacts {
                            let _ = queue.add_dht(hash, &found.peers);
                        }
                    }
                    Err(error) => trace!(swarm=?hash,kind=?error.kind(),%error,"dht_lookup_failed"),
                }
            },
            event = direct, if progress.pending > 0 => {
                let event = event?;
                if let Some(event) = &event && let Some(q) = &mut network.contacts {
                    q.dial_completed(event.address, event.transport(), event.utp_failed);
                }
                progress.direct_event(event, transfer, options, network.contacts.as_mut().expect("online contacts"))?;
                discovery_dirty = true;
            }
            socket = inbound.recv(), if inbound_open => {
                if let Some(socket) = socket {
                    let (socket, prepared) = match socket {
                        crate::engine::session::Inbound::Tcp(socket, prepared) => match tokio::net::TcpStream::from_std(socket) {
                            Ok(socket) => (crate::engine::peer_io::Stream::Tcp(socket), prepared),
                            Err(error) => { debug!(kind = ?error.kind(), %error, "peer_register_failed"); continue; }
                        },
                        crate::engine::session::Inbound::Utp(socket, prepared) => (crate::engine::peer_io::Stream::Utp(socket), prepared),
                    };
                    progress.attach_peer(socket, Role::Inbound, transfer, "inbound", options.log_peer_endpoints, Some(prepared))?;
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
            diagnostics.dht.clear();
        }
    });
    let exit = result.as_ref().ok().copied();
    let result = result.map(|_| ()).and(setup).and(peers).and(p.map(|_| ()));
    debug!(clean = result.is_ok(),
        ?exit, error_kind = ?result.as_ref().err().map(|e| e.kind()),
        error = %result.as_ref().err().map(ToString::to_string).unwrap_or_default(),
        "torrent_runtime_joined");
    result
}

pub(super) struct Resources {
    pub(super) writer: Option<crate::engine::storage::writer::Writer>,
    pub(super) transfer: Option<Task>,
    pub(super) progress: Option<Progress>,
    network: Discovery,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            writer: None,
            transfer: None,
            progress: None,
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
        self.progress = None;
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
        progress,
        network,
    } = resources;
    let writer = writer_slot.as_mut().expect("startup writer");
    status.send_modify(|s| s.progress.phase = TorrentPhase::Recovering);
    let mut report = tokio::time::interval(Duration::from_secs(2));
    report.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break Ok(None),
            result = writer.ready() => break result.map(Some),
            _ = report.tick() => status.send_modify(|s| {
                if let Some(diagnostics) = &mut s.diagnostics {
                    let diagnostics = Arc::make_mut(diagnostics);
                    diagnostics.sampled_at = Some(Instant::now().into_std());
                    diagnostics.storage = Some(writer.snapshot());
                    diagnostics.writer_free_slots = writer.capacity();
                }
            }),
        }
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
    if let Some(download) = transfer {
        download.promote(
            options.metainfo.clone(),
            verified,
            options.selection.clone(),
        )?;
    } else {
        let mut download = Task::new(
            options.metainfo.clone(),
            options.peer_id,
            options.profile,
            writer,
            verified,
        )?;
        download.select_files(options.selection.clone());
        *transfer = Some(download);
    }
    let transfer = transfer.as_mut().expect("installed shell");
    transfer.attach_writer(writer_slot.take().expect("startup writer"));
    let progress = progress.get_or_insert_with(|| Progress::new(status.clone()));
    progress.retain_peers(transfer);
    let result = drive(
        &mut options,
        session,
        transfer,
        network,
        &mut stop,
        &status,
        progress,
    )
    .await;
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
