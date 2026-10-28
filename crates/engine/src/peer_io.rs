//! Bounded peer transport, not a BitTorrent peer handler. Caller supplies
//! already-connected sockets; dialing/listening/destination policy remain outside
//! this module. No requests, choking, availability, extension negotiation or
//! storage writes are authorized by transport success.

pub(crate) use crate::core::fault::peer::*;
pub(crate) use crate::core::message::Outbound;
use crate::transfer_stats::PayloadWrites;
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    io,
    sync::Arc,
    time::Duration,
};

use protocol::{
    identity::InfoHashV1,
    peer::{HANDSHAKE_LEN, Handshake, message::Message},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{Notify, mpsc, watch},
    task::{AbortHandle, Id, JoinError, JoinSet},
    time::{Instant, timeout_at},
};
use tracing::Instrument;

mod transport;
use transport::*;

pub(crate) use crate::core::{ConnectionId, Frame};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Role {
    Outbound,
    Inbound,
}

pub(crate) enum Stream {
    Tcp(TcpStream),
    Utp(crate::session::utp::Stream),
}
impl Stream {
    pub(crate) fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            Self::Tcp(s) => s.peer_addr(),
            Self::Utp(s) => Ok(s.peer_addr()),
        }
    }
    pub(crate) fn transport(&self) -> &'static str {
        match self {
            Self::Tcp(_) => "tcp",
            Self::Utp(_) => "utp",
        }
    }
}
pub(crate) enum PeerSocket {
    Connected(Stream, Role),
    WebSeed(Box<crate::webseed::Seed>),
}
struct HandshakeStart {
    role: Role,
    deadline: Instant,
}

#[derive(Debug)]
pub(crate) enum PeerEvent {
    Handshake {
        connection: ConnectionId,
        reserved: [u8; 8],
        peer_id: [u8; 20],
    },
    Frame {
        connection: ConnectionId,
        frame: Frame,
    },
    /// Clean EOF at a frame boundary is Ok; truncated frames/deadlines are errors.
    /// Closure acknowledgement is emitted only after task join, even
    /// when a clean close was previously queued. Task failure/disconnect may
    /// overtake buffered events; ignore traffic for IDs already closed by the
    /// scheduling owner. IDs are never reused.
    Closed {
        connection: ConnectionId,
        result: io::Result<()>,
    },
}

struct SocketContext {
    bandwidth: crate::bandwidth::Throttle,
    read_pause: watch::Receiver<bool>,
    local: Handshake,
    written: Arc<PayloadWrites>,
    capacity_changed: Arc<Notify>,
}

struct Entry {
    written: Arc<PayloadWrites>,
    commands: mpsc::Sender<Outbound>,
    capacity: usize,
    uploaded: u64,
    connected: bool,
    abort: AbortHandle,
    disconnecting: bool,
    closed: Option<io::Result<()>>,
}
impl Entry {
    fn send(&mut self, frame: Outbound) -> io::Result<()> {
        if self.disconnecting {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "peer disconnecting",
            ));
        }
        match self.commands.try_send(frame) {
            Ok(()) => {
                assert!(self.capacity > 0, "core peer capacity grant");
                self.capacity -= 1;
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "peer closed"))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                unreachable!("core peer queue reservation");
            }
        }
    }
}
// Both variants share the peer-count ceiling; no separate result backlog.
enum Finished {
    Task(ConnectionId, Option<JoinError>),
    Closed(ConnectionId, io::Result<()>),
}

pub(crate) struct Peers {
    pub(crate) bandwidth: crate::bandwidth::Throttle,
    read_pause: watch::Sender<bool>,
    capacity_changed: Arc<Notify>,
    local: Handshake,
    written: Arc<PayloadWrites>,
    limits: crate::profile::Profile,
    next_id: u64,
    entries: HashMap<ConnectionId, Entry>,
    tasks: JoinSet<ConnectionId>,
    events_tx: mpsc::Sender<PeerEvent>,
    events: mpsc::Receiver<PeerEvent>,
    failure: Option<io::Error>,
    // Task(_, None) acknowledges explicit disconnect racing normal completion.
    // All joined task/closure outcomes retain the same connection-count slot.
    finished_errors: VecDeque<Finished>,
}
impl Peers {
    pub(crate) fn new(
        info_hash: InfoHashV1,
        peer_id: [u8; 20],
        limits: crate::profile::Profile,
        capacity_changed: Arc<Notify>,
    ) -> io::Result<Self> {
        let (events_tx, events) = mpsc::channel(crate::profile::EVENT_QUEUE);
        Ok(Self {
            bandwidth: crate::bandwidth::Throttle::default(),
            read_pause: watch::channel(false).0,
            capacity_changed,
            // The scheduling owners implement BEP 10 and BEP 9.
            local: Handshake {
                reserved: crate::core::extensions::RESERVED,
                info_hash: *info_hash.as_bytes(),
                peer_id,
            },
            limits,
            written: Arc::new(PayloadWrites::default()),
            next_id: 0,
            entries: HashMap::new(),
            tasks: JoinSet::new(),
            events_tx,
            events,
            failure: None,
            finished_errors: VecDeque::new(),
        })
    }

    pub(crate) fn set_dht_enabled(&mut self, enabled: bool) {
        if enabled {
            self.local.reserved[7] |= 0x01;
        } else {
            self.local.reserved[7] &= !0x01;
        }
    }

    pub(crate) fn attach_socket(&mut self, socket: PeerSocket) -> io::Result<ConnectionId> {
        self.reap_ready();
        if self.tasks.len() >= self.limits.peers - self.finished_errors.len() {
            return Err(busy("peer connection limit"));
        }
        let next = self.next_id + 1;
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| invalid("Tokio runtime required"))?;
        let deadline = Instant::now() + crate::profile::HANDSHAKE_TIMEOUT;
        let connection = ConnectionId(self.next_id);
        let (commands, receiver) = mpsc::channel(crate::profile::OUTBOUND_QUEUE);
        let events = self.events_tx.clone();
        let written = Arc::new(PayloadWrites::child(self.written.clone()));
        let context = SocketContext {
            bandwidth: self.bandwidth.clone(),
            read_pause: self.read_pause.subscribe(),
            local: self.local.clone(),
            written: written.clone(),
            capacity_changed: self.capacity_changed.clone(),
        };
        let abort = self.tasks.spawn_on(
            async move {
                // The socket and both loop futures drop before waiting to report exit.
                let result = match socket {
                    PeerSocket::Connected(socket, role) => {
                        run_socket(
                            socket,
                            HandshakeStart { role, deadline },
                            context,
                            connection,
                            receiver,
                            &events,
                        )
                        .await
                    }
                    PeerSocket::WebSeed(seed) => {
                        let result = seed.run(connection, receiver, &events, &context.capacity_changed).await;
                        if let Err(error) = &result {
                            tracing::warn!(?connection, kind = ?error.kind(), reason = %error, "web_seed_failed");
                        }
                        result
                    }
                }
                .map_err(socket_failure);
                let _ = events.send(PeerEvent::Closed { connection, result }).await;
                connection
            }
            .instrument(tracing::debug_span!("peer", ?connection)),
            &runtime,
        );
        self.entries.insert(
            connection,
            Entry {
                written,
                commands,
                capacity: 0,
                uploaded: 0,
                connected: false,
                abort,
                disconnecting: false,
                closed: None,
            },
        );
        self.next_id = next;
        Ok(connection)
    }

    pub(crate) fn send(&mut self, connection: ConnectionId, frame: Frame) -> io::Result<()> {
        let entry = self
            .entries
            .get_mut(&connection)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
        entry.send(Outbound::Encoded(Arc::unwrap_or_clone(frame.bytes)))
    }

    pub(crate) fn send_piece(
        &mut self,
        connection: ConnectionId,
        index: u32,
        begin: u32,
        block: crate::core::SharedBlock,
    ) -> io::Result<()> {
        let entry = self
            .entries
            .get_mut(&connection)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
        let mut header = [0; 13];
        header[..4].copy_from_slice(&((block.len() + 9) as u32).to_be_bytes());
        header[4] = 7;
        header[5..9].copy_from_slice(&index.to_be_bytes());
        header[9..13].copy_from_slice(&begin.to_be_bytes());
        entry.send(Outbound::Piece { header, block })
    }

    pub(crate) fn disconnect(&mut self, connection: ConnectionId) -> io::Result<()> {
        let entry = self
            .entries
            .get_mut(&connection)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
        entry.disconnecting = true;
        entry.abort.abort();
        Ok(())
    }

    /// Pause at frame boundaries without canceling a partially read frame.
    /// One in-flight frame per socket and the event queue remain transport-bounded.
    pub(crate) fn pause_reads(&self, paused: bool) {
        self.read_pause.send_if_modified(|old| {
            if *old == paused {
                false
            } else {
                *old = paused;
                true
            }
        });
    }
    pub(crate) fn handshake_received(&mut self, connection: ConnectionId) {
        if let Some(entry) = self.entries.get_mut(&connection) {
            entry.connected = true;
            self.capacity_changed.notify_one();
        }
    }
    /// Report only changes since the previous grant, net of our own sends.
    pub(crate) fn capacity_changes(&mut self, changes: &mut Vec<crate::core::Capacity>) {
        for (&id, entry) in &mut self.entries {
            // The core creates metadata peer state on the handshake, not attach.
            // Do not lose its initial capacity grant before that state exists.
            if !entry.connected {
                continue;
            }
            let frames = if entry.disconnecting || entry.commands.is_closed() {
                0
            } else {
                entry.commands.capacity()
            };
            let uploaded = entry.written.snapshot();
            if frames != entry.capacity || uploaded != entry.uploaded {
                entry.capacity = frames;
                entry.uploaded = uploaded;
                changes.push(crate::core::Capacity {
                    id,
                    frames,
                    uploaded,
                });
            }
        }
    }
    pub(crate) fn upload_counter(&self) -> Arc<PayloadWrites> {
        self.written.clone()
    }
    pub(crate) fn has_work(&self) -> bool {
        !self.tasks.is_empty() || !self.events.is_empty() || !self.finished_errors.is_empty()
    }

    fn joined(&mut self, result: Result<(Id, ConnectionId), JoinError>) {
        match result {
            Ok((_, connection)) => {
                let entry = self.entries.remove(&connection).expect("tracked peer task");
                tracing::debug!(
                    ?connection,
                    reason = if entry.disconnecting {
                        "disconnect_requested"
                    } else {
                        "transport_finished"
                    },
                    "peer_transport_joined"
                );
                if entry.disconnecting {
                    self.finished_errors
                        .push_back(Finished::Task(connection, None));
                } else if let Some(result) = entry.closed {
                    self.finished_errors
                        .push_back(Finished::Closed(connection, result));
                }
                // Otherwise the real Closed remains in the event queue and is
                // safe to deliver now that this task has actually joined.
            }
            Err(error) => {
                let task_id = error.id();
                let connection = self
                    .entries
                    .iter()
                    .find_map(|(id, entry)| (entry.abort.id() == task_id).then_some(*id))
                    .expect("tracked peer task");
                tracing::debug!(
                    ?connection,
                    reason = if error.is_cancelled() {
                        "cancelled"
                    } else {
                        "task_failed"
                    },
                    "peer_transport_joined"
                );
                self.entries.remove(&connection);
                self.finished_errors
                    .push_back(Finished::Task(connection, Some(error)));
            }
        }
    }
    fn reap_ready(&mut self) {
        while let Some(result) = self.tasks.try_join_next_with_id() {
            self.joined(result);
        }
    }

    pub(crate) async fn next(&mut self) -> Option<PeerEvent> {
        loop {
            self.reap_ready();
            if let Some(finished) = self.finished_errors.pop_front() {
                let (connection, result) = match finished {
                    Finished::Closed(connection, result) => (connection, result),
                    Finished::Task(connection, error) => {
                        let failure = match error {
                            Some(error) if !error.is_cancelled() => task_failed(error),
                            _ => socket_failure(io::Error::new(
                                io::ErrorKind::Interrupted,
                                "peer disconnected",
                            )),
                        };
                        (connection, Err(failure))
                    }
                };
                return Some(PeerEvent::Closed { connection, result });
            }
            let event = if self.tasks.is_empty() {
                self.events.try_recv().ok()?
            } else {
                tokio::select! {
                    event = self.events.recv() => event?,
                    joined = self.tasks.join_next_with_id() => {
                        self.joined(joined.expect("nonempty task set"));
                        continue;
                    }
                }
            };
            let connection = match &event {
                PeerEvent::Handshake { connection, .. }
                | PeerEvent::Frame { connection, .. }
                | PeerEvent::Closed { connection, .. } => *connection,
            };
            // Retain a consumed natural result until the reporting task joins.
            // Cancellation of next() cannot lose it or release the peer slot.
            // Explicit disconnect still wins over a queued natural result.
            if let Some(entry) = self.entries.get_mut(&connection) {
                if entry.disconnecting {
                    continue;
                }
                if let PeerEvent::Closed { result, .. } = event {
                    entry.closed = Some(result);
                    continue;
                }
            }
            return Some(event);
        }
    }

    pub(crate) async fn cancel_and_join(&mut self) -> io::Result<()> {
        self.tasks.abort_all();
        // Retain failures in the owner across canceled barrier waits.
        while let Some(result) = self.tasks.join_next_with_id().await {
            let connection = match &result {
                Ok((_, connection)) => Some(*connection),
                Err(error) => self.entries.iter().find_map(|(connection, entry)| {
                    (entry.abort.id() == error.id()).then_some(*connection)
                }),
            };
            if let Some(connection) = connection {
                tracing::debug!(
                    ?connection,
                    reason = "owner_shutdown",
                    "peer_transport_joined"
                );
            }
            if let Err(error) = result
                && !error.is_cancelled()
            {
                self.failure.get_or_insert_with(|| task_failed(error));
            }
        }
        self.entries.clear();
        for finished in self.finished_errors.drain(..) {
            if let Finished::Task(_, Some(error)) = finished
                && !error.is_cancelled()
            {
                self.failure.get_or_insert_with(|| task_failed(error));
            }
        }
        while self.events.try_recv().is_ok() {}
        self.failure.take().map_or(Ok(()), Err)
    }
}
