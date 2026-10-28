use super::{
    Network, Networking, RunPolicy, RuntimeOptions, TorrentExit, TorrentPhase, TorrentStatus,
    observation::{self, Progress},
};
use crate::peer_io::Role;
use crate::{discovery::Discovery, torrent::shell::Shell as Task};
use std::{io, time::Duration};
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
    options: &RuntimeOptions,
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
    mut inbound: tokio::sync::mpsc::Receiver<crate::session::Inbound>,
) -> io::Result<TorrentExit> {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut progress = Progress::new();
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
        progress.update_counters(transfer, network)?;
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
                    Ok(crate::dht::DiscoveryEvent::Peers(peers)) => {
                        if let Some(queue) = &mut network.contacts {
                            let peers: Vec<_> = peers.into_iter().map(std::net::SocketAddr::V4).collect();
                            let _ = queue.add_dht(options.metainfo.info_hash(), &peers);
                        }
                    }
                    Ok(crate::dht::DiscoveryEvent::Finished(found)) => {
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
                        crate::session::Inbound::Tcp(socket) => match tokio::net::TcpStream::from_std(socket) {
                            Ok(socket) => crate::peer_io::Stream::Tcp(socket),
                            Err(error) => { warn!(kind = ?error.kind(), "peer_register_failed"); continue; }
                        },
                        crate::session::Inbound::Utp(socket) => crate::peer_io::Stream::Utp(socket),
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
                if !matches!(event, crate::core::Event::Block { .. } | crate::core::Event::IgnoredBlock { .. }) {
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
    let joined = setup.is_ok() && peers.is_ok();
    let pieces: std::sync::Arc<[bool]> = (0..piece_count).map(|i| transfer.contains(i)).collect();
    status.send_modify(|s| {
        s.progress.phase = TorrentPhase::Stopped;
        s.progress.verified_pieces = pieces.iter().filter(|&&p| p).count() as u32;
        s.progress.verified_piece_map = pieces;
        if let Ok(p) = &p {
            s.progress.verified_bytes = p.verified_bytes();
            s.progress.total_bytes = p.total_bytes();
            s.progress.wanted_bytes = p.wanted_bytes();
            s.progress.wanted_verified_bytes = p.wanted_verified_bytes();
            s.progress.processed_payload_bytes = p.received_payload_bytes();
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
        s.progress.joined = joined;
    });
    info!(clean = result.is_ok() && joined,
        exit = ?result.as_ref().ok(), error_kind = ?result.as_ref().err().map(|e| e.kind()),
        "torrent_runtime_joined");
    result.map(|_| ()).and(setup).and(peers).and(p.map(|_| ()))
}

async fn serve_torrent(
    mut transfer: Task,
    options: RuntimeOptions,
    session: Network,
    mut stop: watch::Receiver<bool>,
    status: Status,
) -> io::Result<()> {
    let mut network = Discovery::empty();
    let result = drive(
        &options,
        session,
        &mut transfer,
        &mut network,
        &mut stop,
        &status,
    )
    .await;
    finish(
        &mut transfer,
        &mut network,
        &status,
        options.run,
        options.metainfo.layout().piece_count(),
        result,
    )
    .await
}

pub(super) async fn run(
    mut writer: crate::storage::writer::Writer,
    options: RuntimeOptions,
    networking: Networking,
    mut stop: watch::Receiver<bool>,
    status: watch::Sender<TorrentStatus>,
) -> io::Result<()> {
    status.send_modify(|s| s.progress.phase = TorrentPhase::Recovering);
    let result = tokio::select! {
        biased;
        _ = stop.changed() => Ok(None),
        result = writer.ready() => result.map(Some),
    };
    match result {
        Ok(Some(verified)) => {
            let count: u32 = verified.iter().map(|b| b.count_ones()).sum();
            let session = match networking {
                Networking::Recheck => {
                    let pieces: Vec<_> = (0..options.metainfo.layout().piece_count())
                        .map(|i| verified[i as usize / 8] & (0x80 >> (i % 8)) != 0)
                        .collect();
                    let bytes = pieces
                        .iter()
                        .enumerate()
                        .filter(|(_, passed)| **passed)
                        .map(|(i, _)| {
                            let r = options
                                .metainfo
                                .layout()
                                .piece_range(i as u32)
                                .expect("piece");
                            r.end - r.start
                        })
                        .sum();
                    let wanted_bytes = options.selection.wanted_bytes();
                    let wanted_verified_bytes = options
                        .selection
                        .verified_bytes(options.metainfo.layout(), &verified);
                    let complete = options.selection.is_complete(&verified);
                    let joined = writer.shutdown().await;
                    status.send_modify(|s| {
                        s.progress.phase = TorrentPhase::Stopped;
                        s.progress.verified_bytes = bytes;
                        s.progress.verified_pieces = count;
                        s.progress.verified_piece_map = pieces.into();
                        s.progress.total_bytes = options.metainfo.layout().total_length();
                        s.progress.wanted_bytes = wanted_bytes;
                        s.progress.wanted_verified_bytes = wanted_verified_bytes;
                        s.progress.completed = complete && joined.is_ok();
                        s.progress.joined = joined.is_ok();
                        s.progress.exit = Some(TorrentExit::Complete);
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
            serve_torrent(download, options, session, stop, status).await
        }
        other => {
            let joined = writer.shutdown().await;
            status.send_modify(|s| {
                s.progress.phase = TorrentPhase::Stopped;
                s.progress.joined = joined.is_ok();
                s.progress.exit = Some(TorrentExit::StopRequested);
            });
            other.map(|_| ()).and(joined)
        }
    }
}
