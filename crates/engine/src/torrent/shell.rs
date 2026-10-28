//! Transport/storage adapter. All protocol and scheduling choices belong to core.
use crate::{
    core::fault::is_local,
    core::{self, Action, Capacity, ConnectionId, Event, Input, Torrent},
    peer_io::{PeerEvent, PeerSocket, Peers, Role},
    profile::Profile,
    storage::writer::{Output, Writer},
};
use protocol::{identity::InfoHashV1, metainfo::OwnedMetainfo, peer::Handshake};
use std::{collections::VecDeque, io, sync::Arc};
use tokio::{sync::Notify, time::Instant};

pub(crate) struct Shell {
    core: Torrent,
    peers: Peers,
    writer: Option<Writer>,
    capacity_changed: Arc<Notify>,
    ready: Vec<Capacity>,
    writes: usize,
    events: VecDeque<Event>,
    metadata: Option<OwnedMetainfo>,
    wake_at: Option<Instant>,
    changes: crate::bandwidth::Changes,
    peer_hints: Vec<(ConnectionId, Vec<std::net::SocketAddr>)>,
    hint_limit: usize,
    dht_hints: Vec<std::net::SocketAddr>,
    connects: Vec<std::net::SocketAddr>,
    announces: Vec<core::AnnounceEvent>,
    listen: Option<u16>,
    advertised_port: Option<tokio::sync::watch::Receiver<u16>>,
    dht: Option<u16>,
    pex: bool,
    stopped: bool,
}
impl Shell {
    pub(crate) fn new(
        meta: OwnedMetainfo,
        peer_id: [u8; 20],
        profile: Profile,
        writer: Writer,
        verified: Vec<u8>,
    ) -> io::Result<Self> {
        let mut entropy = [0; 8];
        getrandom::getrandom(&mut entropy).map_err(io::Error::other)?;
        let core = Torrent::new(
            Instant::now().into_std(),
            meta.clone(),
            profile,
            verified,
            u64::from_ne_bytes(entropy),
        )?;
        Self::with_core(core, meta.info_hash(), peer_id, profile, Some(writer))
    }
    fn with_core(
        mut core: Torrent,
        hash: InfoHashV1,
        peer_id: [u8; 20],
        profile: Profile,
        writer: Option<Writer>,
    ) -> io::Result<Self> {
        core.set_identity(peer_id);
        let capacity_changed = writer
            .as_ref()
            .map_or_else(|| Arc::new(Notify::new()), Writer::capacity_changed);
        let peers = Peers::new(hash, peer_id, profile, capacity_changed.clone())?;
        capacity_changed.notify_one();
        let changes = peers.bandwidth.changes();
        Ok(Self {
            core,
            peers,
            writer,
            capacity_changed,
            ready: Vec::with_capacity(profile.peers),
            writes: 0,
            events: VecDeque::new(),
            metadata: None,
            wake_at: None,
            changes,
            peer_hints: Vec::new(),
            hint_limit: profile.peers,
            dht_hints: Vec::new(),
            connects: Vec::new(),
            announces: Vec::new(),
            listen: None,
            advertised_port: None,
            dht: None,
            pex: false,
            stopped: false,
        })
    }
    pub(crate) fn magnet(
        hash: InfoHashV1,
        peer_id: [u8; 20],
        profile: Profile,
    ) -> io::Result<Self> {
        let core = Torrent::magnet(Instant::now().into_std(), hash, profile)?;
        Self::with_core(core, hash, peer_id, profile, None)
    }
    pub(crate) fn take_metadata(&mut self) -> Option<OwnedMetainfo> {
        self.metadata.take()
    }
    fn writer(&mut self) -> io::Result<&mut Writer> {
        self.writer
            .as_mut()
            .ok_or_else(|| io::Error::other("payload before storage admission"))
    }
    fn feed(&mut self, input: Input<'_>) -> io::Result<()> {
        if let Some(port) = &self.advertised_port {
            let port = *port.borrow();
            self.listen = Some(port);
            self.dht = self.dht.map(|_| port);
            self.core.set_ports(self.listen, self.dht, self.pex);
        }
        let mut actions = self.core.handle(Instant::now().into_std(), input);
        self.wake_at = None;
        let result = actions.drain(..).try_for_each(|action| self.apply(action));
        self.core.recycle_actions(actions);
        result
    }
    fn apply(&mut self, action: Action) -> io::Result<()> {
        match action {
            Action::Send { id, msg } => {
                Self::send_result(self.peers.send(id, msg))?;
            }
            Action::SendPiece { id, request, block } => {
                Self::send_result(
                    self.peers
                        .send_piece(id, request.index, request.begin, block),
                )?;
            }
            Action::Disconnect { id } => {
                let _ = self.peers.disconnect(id);
            }
            Action::Write {
                piece,
                begin,
                block,
            } => {
                self.writer()?.write_shared(piece, begin, block)?;
                self.used_write_slot();
            }
            Action::Hash { piece } => {
                self.writer()?.hash(piece)?;
                self.used_write_slot();
            }
            Action::Read { request } => {
                self.writer()?
                    .read(request.index, request.begin, request.length)?;
                self.used_write_slot();
            }
            Action::Discard { piece } => self.writer()?.discard(piece),
            Action::Flush => {
                self.writer()?.flush()?;
                self.used_write_slot();
            }
            Action::WakeAt(at) => self.wake_at = Some(Instant::from_std(at)),
            Action::Completed => self.events.push_back(Event::Complete),
            Action::Stalled(reason) => self.events.push_back(Event::Stalled(reason)),
            Action::Observed(event) => self.events.push_back(event),
            Action::MetadataResolved(meta) => {
                self.metadata = Some(meta);
                self.events.push_back(Event::Complete);
            }
            Action::Connect { addr } => self.connects.push(addr),
            Action::Announce { event } => self.announces.push(event),
            Action::DhtHint { addr } => {
                if self.dht_hints.len() < self.hint_limit {
                    self.dht_hints.push(addr);
                }
            }
            Action::PeerHints { id, contacts } => {
                if self.peer_hints.len() < self.hint_limit {
                    self.peer_hints.push((id, contacts));
                }
            }
            Action::Failed(error) => return Err(error),
        }
        Ok(())
    }
    fn used_write_slot(&mut self) {
        assert!(self.writes > 0, "core writer capacity grant");
        self.writes -= 1;
    }
    fn send_result(result: io::Result<()>) -> io::Result<()> {
        match result {
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::BrokenPipe | io::ErrorKind::NotFound
                ) =>
            {
                Ok(())
            }
            result => result,
        }
    }
    fn refresh_capacity(&mut self) -> io::Result<()> {
        let mut peers = std::mem::take(&mut self.ready);
        self.peers.capacity_changes(&mut peers);
        let writes = self.writer.as_ref().map_or(0, Writer::capacity);
        let result = if !peers.is_empty() || writes != self.writes {
            self.writes = writes;
            self.feed(Input::Ready {
                peers: &peers,
                writes,
            })
        } else {
            Ok(())
        };
        peers.clear();
        self.ready = peers;
        result
    }
    pub(crate) async fn next(&mut self) -> io::Result<Option<Event>> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        if self.writer.as_ref().is_some_and(|w| !w.healthy()) {
            return Err(io::Error::other("storage writer failed"));
        }
        let storage_work = self.writer.as_ref().is_some_and(|w| w.has_job());
        let full = self.writer.as_ref().is_some_and(|w| w.capacity() == 0);
        self.peers.pause_reads(full);
        let at = self.wake_at;
        tokio::select! {
            biased;
            // notify_one retains a permit even if another branch wins or next()
            // is canceled. No per-sender reservations or boxed waiters are needed.
            () = self.capacity_changed.notified() => self.refresh_capacity()?,
            event = self.peers.next(), if self.peers.has_work() && !full => {
                if let Some(event) = event { self.peer_event(event)?; }
            }
            result = async { self.writer.as_mut().expect("writer enabled").next().await }, if storage_work => {
                self.storage_event(result?)?;
            }
            () = async { if let Some(at) = at { tokio::time::sleep_until(at).await; } else { std::future::pending().await } } => self.feed(Input::Tick)?,
            () = self.changes.changed() => self.feed(Input::Tick)?,
        }
        Ok(self.events.pop_front())
    }
    fn peer_event(&mut self, event: PeerEvent) -> io::Result<()> {
        match event {
            PeerEvent::Handshake {
                connection: id,
                reserved,
                peer_id,
            } => {
                self.peers.handshake_received(id);
                self.feed(Input::PeerConnected {
                    id,
                    handshake: Handshake {
                        reserved,
                        peer_id,
                        info_hash: *self.info_hash().as_bytes(),
                    },
                })
            }
            PeerEvent::Frame {
                connection: id,
                frame: msg,
            } => self.feed(Input::Frame { id, msg }),
            PeerEvent::Closed {
                connection: id,
                result,
            } => {
                if let Err(error) = &result
                    && is_local(error)
                {
                    return Err(result.unwrap_err());
                }
                self.feed(Input::PeerClosed { id })?;
                if let Err(error) = result
                    && error.kind() != io::ErrorKind::Interrupted
                    && let Some(Event::PeerClosed { result, .. }) = self
                        .events
                        .iter_mut()
                        .rev()
                        .find(|event| matches!(event, Event::PeerClosed { connection, .. } if *connection == id))
                {
                    *result = Err(error);
                }
                Ok(())
            }
        }
    }
    fn storage_event(&mut self, output: Output) -> io::Result<()> {
        match output {
            Output::Written(blocks) => {
                for piece in blocks {
                    self.feed(Input::BlockWritten { piece })?;
                }
            }
            Output::Hashed {
                index: piece,
                verified: ok,
            } => self.feed(Input::HashDone { piece, ok })?,
            Output::Read {
                index,
                offset,
                length,
                block,
            } => {
                self.feed(Input::ReadDone {
                    request: protocol::peer::message::BlockRequest {
                        index,
                        begin: offset,
                        length,
                    },
                    block,
                })?;
            }
            Output::Flushed => self.feed(Input::Flushed)?,
            Output::Error(error) => return Err(error),
        }
        Ok(())
    }
    pub(crate) fn attach_peer(
        &mut self,
        socket: crate::peer_io::Stream,
        role: Role,
    ) -> io::Result<ConnectionId> {
        self.attach_socket(PeerSocket::Connected(socket, role))
    }
    pub(crate) fn attach_socket(&mut self, socket: PeerSocket) -> io::Result<ConnectionId> {
        let (endpoint, local_ip, inbound, web_seed) = match &socket {
            PeerSocket::Connected(socket, role) => {
                let mut endpoint = socket.peer_addr()?;
                let inbound = matches!(role, Role::Inbound);
                if inbound {
                    endpoint.set_port(0);
                }
                (
                    Some(endpoint),
                    match socket {
                        crate::peer_io::Stream::Tcp(s) => Some(s.local_addr()?.ip()),
                        crate::peer_io::Stream::Utp(_) => None,
                    },
                    inbound,
                    false,
                )
            }
            PeerSocket::WebSeed(_) => (None, None, false, true),
        };
        let id = self.peers.attach_socket(socket)?;
        self.feed(Input::PeerAttached {
            id,
            endpoint,
            local_ip,
            inbound,
            web_seed,
        })?;
        Ok(id)
    }
    pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
        if self.stopped {
            return Ok(());
        }
        self.stopped = true;
        let _ = self.feed(Input::Stop);
        let peers = self.peers.cancel_and_join().await;
        let writer = if let Some(writer) = &mut self.writer {
            writer.shutdown().await
        } else {
            Ok(())
        };
        peers.and(writer)
    }
    pub(crate) fn set_bandwidth(&mut self, bandwidth: crate::bandwidth::Throttle) {
        self.core.set_bandwidth(bandwidth.clone());
        self.changes = bandwidth.changes();
        self.peers.bandwidth = bandwidth;
    }
    pub(crate) fn set_advertised_port(&mut self, port: tokio::sync::watch::Receiver<u16>) {
        self.listen = Some(*port.borrow());
        self.advertised_port = Some(port);
        self.core.set_ports(self.listen, self.dht, self.pex);
    }
    pub(crate) fn set_dht_port(&mut self, port: Option<u16>) {
        self.dht = port;
        self.peers.set_dht_enabled(port.is_some());
        self.core.set_ports(self.listen, self.dht, self.pex);
    }
    pub(crate) fn drain_pex(&mut self, queue: &mut crate::session::dial::Contacts) {
        for (id, contacts) in self.peer_hints.drain(..) {
            let _ = queue.add_pex(id, &contacts);
        }
    }
    pub(crate) fn drain_dht(&mut self, node: &crate::session::dht::Handle) {
        for addr in self.dht_hints.drain(..) {
            if let std::net::SocketAddr::V4(addr) = addr {
                let _ = node.try_ping(addr);
            }
        }
    }
    pub(crate) fn settings(
        &mut self,
        settings: crate::settings::TorrentSettings,
    ) -> io::Result<()> {
        self.pex = settings.pex;
        self.core.set_ports(self.listen, self.dht, self.pex);
        self.feed(Input::Settings(settings))
    }
    pub(crate) fn contact(
        &mut self,
        addr: std::net::SocketAddr,
    ) -> io::Result<Option<std::net::SocketAddr>> {
        self.feed(Input::Contacts(vec![addr]))?;
        Ok(self.connects.pop())
    }
    pub(crate) fn connect_failed(&mut self, addr: std::net::SocketAddr) -> io::Result<()> {
        self.feed(Input::ConnectFailed { addr })
    }
    pub(crate) fn set_no_progress_timeout(&mut self, timeout: std::time::Duration) {
        self.core.set_no_progress_timeout(timeout);
    }
    pub(crate) fn take_announces(&mut self) -> Vec<core::AnnounceEvent> {
        std::mem::take(&mut self.announces)
    }
    pub(crate) fn select_files(&mut self, selection: crate::selection::FileSelection) {
        self.core.select_files(selection);
    }
    pub(crate) fn writer_free_slots(&self) -> usize {
        self.writer.as_ref().map_or(0, Writer::capacity)
    }
    pub(crate) fn uploaded_bytes(&self) -> u64 {
        self.peers.upload_counter().snapshot()
    }
}

impl std::ops::Deref for Shell {
    type Target = Torrent;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}
