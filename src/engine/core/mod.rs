//! Clock-driven torrent state. Effects are values; only the shell performs I/O.
mod bitfield;
mod block;
pub(in crate::engine) mod choker;
pub(in crate::engine) mod connection;
pub(in crate::engine) mod extensions;
pub(in crate::engine) mod fault;
pub(in crate::engine) mod file_progress;
pub(in crate::engine) mod live;
pub(in crate::engine) mod message;
mod messages;
pub(in crate::engine) mod metadata;
mod observation;
pub(in crate::engine) mod peer;
mod peers;
mod pex;
mod picker;
mod pieces;
pub(in crate::engine) mod requests;
mod scheduling;
mod uploads;
mod wire;

pub(in crate::engine) use crate::engine::torrent::telemetry::PeerDiagnostics;
use crate::engine::{
    bandwidth,
    network::{IpAddr, SocketAddr},
    profile::Profile,
    selection,
};
use crate::protocol::{
    identity::InfoHashV1,
    metainfo::OwnedMetainfo,
    peer::{
        Handshake,
        message::{BlockRequest, Message},
    },
};
use bitfield::*;
pub(in crate::engine) use block::SharedBlock;
use choker::{Candidate, Choker};
pub(in crate::engine) use connection::ConnectionId;
use connection::UnsolicitedBudget;
pub(in crate::engine) use message::Frame;
pub(in crate::engine) use observation::{Diagnostics, Event, Progress, Stall};
use peer::PeerState as Remote;
pub(in crate::engine) use picker::PIECE_BYTES as PIECE_BUDGET;
use picker::Picker;
use std::{
    collections::{HashMap, VecDeque},
    fmt, io,
    time::{Duration, Instant},
};

pub(in crate::engine) use crate::engine::settings::TorrentSettings;
/// Capacities are reservations available to this single owner, not remote hints.
pub(in crate::engine) struct Capacity {
    pub(in crate::engine) id: ConnectionId,
    pub(in crate::engine) frames: usize,
    pub(in crate::engine) uploaded: u64,
}
pub(in crate::engine) enum Input<'a> {
    PeerAttached {
        id: ConnectionId,
        endpoint: Option<SocketAddr>,
        local_ip: Option<IpAddr>,
        inbound: bool,
        web_seed: bool,
    },
    PeerConnected {
        id: ConnectionId,
        handshake: Handshake,
    },
    PeerClosed {
        id: ConnectionId,
    },
    ConnectFailed {
        addr: SocketAddr,
    },
    Frame {
        id: ConnectionId,
        msg: Frame,
    },
    BlockWritten {
        piece: u32,
    },
    HashDone {
        piece: u32,
        ok: bool,
    },
    ReadDone {
        request: BlockRequest,
        block: Option<SharedBlock>,
    },
    Ready {
        peers: &'a [Capacity],
        writes: usize,
    },
    Flushed,
    Tick,
    Stop,
    Contacts(Vec<SocketAddr>),
    Settings(TorrentSettings),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::engine) enum AnnounceEvent {
    Started,
    Completed,
    Stopped,
}
#[derive(Debug)]
pub(in crate::engine) enum Action {
    Send {
        id: ConnectionId,
        msg: Frame,
    },
    SendPiece {
        id: ConnectionId,
        request: BlockRequest,
        block: SharedBlock,
    },
    Disconnect {
        id: ConnectionId,
    },
    Write {
        piece: u32,
        begin: u32,
        block: SharedBlock,
    },
    Hash {
        piece: u32,
    },
    Read {
        request: BlockRequest,
    },
    Discard {
        piece: u32,
    },
    Flush,
    Connect {
        addr: SocketAddr,
    },
    Announce {
        event: AnnounceEvent,
    },
    DhtHint {
        addr: SocketAddr,
    },
    PeerHints {
        id: ConnectionId,
        contacts: Vec<SocketAddr>,
    },
    WakeAt(Instant),
    Completed,
    Stalled(Stall),
    Observed(Event),
    MetadataResolved(OwnedMetainfo),
    Failed(io::Error),
}

struct Read {
    request: BlockRequest,
    result: Option<Option<SharedBlock>>,
}
struct Duplex {
    completion_reported: bool,
    cursor: usize,
}

// Payload-only methods borrow this field independently of peer state. The
// metadata phase is dispatched before any manifest-dependent method is entered.
struct Manifest(Option<OwnedMetainfo>);
impl std::ops::Deref for Manifest {
    type Target = OwnedMetainfo;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("payload phase")
    }
}
pub(in crate::engine) struct Torrent {
    now: Instant,
    started: bool,
    own_peer_id: Option<[u8; 20]>,
    meta: Manifest,
    metadata: Option<metadata::Fetch>,
    limits: Profile,
    bandwidth: bandwidth::Throttle,
    rate_deadline: Option<Instant>,
    listen_port: Option<u16>,
    dht_port: Option<u16>,
    pex_enabled: bool,
    duplex: Duplex,
    remotes: Vec<Remote>,
    remote_slots: HashMap<ConnectionId, usize>,
    peer_cursor: usize,
    banned: VecDeque<([u8; 20], Option<IpAddr>, Instant)>,
    dialing: Vec<SocketAddr>,
    turnover_due: Instant,
    useful_at: Instant,
    no_progress_timeout: Option<Duration>,
    stall_reported: bool,
    verified: Vec<u8>,
    verified_count: u32,
    excluded: Vec<u8>,
    selection: Option<selection::FileSelection>,
    wanted_remaining: u32,
    wanted_verified_bytes: u64,
    initial_verified_bytes: u64,
    verified_bytes: u64,
    received_payload_bytes: u64,
    hash_failed_bytes: u64,
    hash_failed_pieces: u64,
    picker: Picker,
    choker: Choker<ConnectionId>,
    choker_dirty: bool,
    request_candidates: Vec<(bool, u32)>,
    request_available: Vec<u8>,
    write_capacity: usize,
    writing: HashMap<u32, usize>,
    reads: Vec<Read>,
    flushing: bool,
    flushed: bool,
    failed: bool,
    actions: Vec<Action>,
    deferred: VecDeque<Event>,
}
impl Torrent {
    /// `seed` is supplied by admission; the core never obtains OS entropy.
    pub(in crate::engine) fn new(
        now: Instant,
        meta: OwnedMetainfo,
        limits: Profile,
        verified: Vec<u8>,
        seed: u64,
    ) -> io::Result<Self> {
        let source_limit = (u64::from(meta.layout().piece_length())
            .div_ceil(u64::from(crate::engine::profile::BLOCK_LEN))
            + meta.layout().files().len() as u64) as usize;
        let window = peer::MAX_REQUESTS * limits.peers;
        let mut picker = Picker::bounded(window, source_limit, meta.layout(), seed)?;
        picker.restore_missing(meta.layout().piece_count(), &verified);
        let verified_count = verified.iter().map(|b| b.count_ones()).sum();
        let verified_bytes = (0..meta.layout().piece_count())
            .filter(|&i| has(&verified, i))
            .map(|i| {
                let r = meta.layout().piece_range(i).expect("piece");
                r.end - r.start
            })
            .sum();
        let wanted_remaining = meta.layout().piece_count() - verified_count;
        let mut torrent = Self::empty(now, limits, picker);
        torrent.meta = Manifest(Some(meta));
        torrent.excluded = verified.clone();
        torrent.verified = verified;
        torrent.verified_count = verified_count;
        torrent.wanted_remaining = wanted_remaining;
        torrent.verified_bytes = verified_bytes;
        torrent.initial_verified_bytes = verified_bytes;
        torrent.wanted_verified_bytes = verified_bytes;
        Ok(torrent)
    }
    fn empty(now: Instant, limits: Profile, picker: Picker) -> Self {
        Self {
            now,
            started: false,
            own_peer_id: None,
            meta: Manifest(None),
            metadata: None,
            limits,
            bandwidth: bandwidth::Throttle::default(),
            rate_deadline: None,
            listen_port: None,
            dht_port: None,
            pex_enabled: false,
            duplex: Duplex {
                completion_reported: false,
                cursor: 0,
            },
            remotes: Vec::new(),
            remote_slots: HashMap::new(),
            peer_cursor: 0,
            banned: VecDeque::new(),
            dialing: Vec::new(),
            turnover_due: now,
            useful_at: now,
            no_progress_timeout: None,
            stall_reported: false,
            excluded: Vec::new(),
            verified: Vec::new(),
            verified_count: 0,
            selection: None,
            wanted_remaining: 0,
            wanted_verified_bytes: 0,
            initial_verified_bytes: 0,
            verified_bytes: 0,
            received_payload_bytes: 0,
            hash_failed_bytes: 0,
            hash_failed_pieces: 0,
            picker,
            choker: Choker::new(),
            choker_dirty: true,
            request_candidates: Vec::new(),
            request_available: Vec::new(),
            write_capacity: 0,
            writing: HashMap::new(),
            reads: Vec::new(),
            flushing: false,
            flushed: false,
            failed: false,
            actions: Vec::new(),
            deferred: VecDeque::new(),
        }
    }
    pub(in crate::engine) fn handle(&mut self, now: Instant, input: Input<'_>) -> Vec<Action> {
        self.now = now;
        self.actions.clear();
        if matches!(input, Input::Stop) {
            self.picker.clear();
            self.failed = true;
            for p in &self.remotes {
                self.actions.push(Action::Disconnect { id: p.connection });
            }
            self.actions.push(Action::Announce {
                event: AnnounceEvent::Stopped,
            });
            return std::mem::take(&mut self.actions);
        }
        if self.failed {
            return Vec::new();
        }
        if let Input::PeerConnected { id, handshake } = &input
            && (handshake.info_hash != *self.info_hash().as_bytes()
                || self.own_peer_id == Some(handshake.peer_id))
        {
            self.actions.push(Action::Disconnect { id: *id });
            return std::mem::take(&mut self.actions);
        }
        let result = if self.metadata.is_some() {
            self.handle_metadata(input)
        } else {
            self.dispatch(input).and_then(|()| self.pump())
        };
        if let Err(error) = result {
            self.failed = true;
            self.picker.clear_all_requests();
            self.actions.push(Action::Failed(error));
        }
        while let Some(event) = self.deferred.pop_front() {
            self.actions.push(Action::Observed(event));
        }
        std::mem::take(&mut self.actions)
    }
    pub(in crate::engine) fn recycle_actions(&mut self, mut actions: Vec<Action>) {
        actions.clear();
        self.actions = actions;
    }
    fn dispatch(&mut self, input: Input<'_>) -> io::Result<()> {
        match input {
            Input::PeerAttached {
                id,
                endpoint,
                local_ip,
                inbound,
                web_seed,
            } => self.on_attach(id, endpoint, local_ip, inbound, web_seed)?,
            Input::PeerConnected { id, handshake } => {
                if !self.remote_slots.contains_key(&id) {
                    self.on_attach(id, None, None, false, false)?;
                }
                if let Some(event) = self.on_connect(id, handshake)? {
                    self.deferred.push_back(event);
                }
            }
            Input::Frame { id, msg } => {
                if let Some(event) = self.on_frame(id, msg)? {
                    self.deferred.push_back(event);
                }
            }
            Input::PeerClosed { id } => {
                if let Some(event) = self.on_close(id)? {
                    self.deferred.push_back(event);
                }
            }
            Input::Ready { peers, writes } => {
                self.write_capacity = writes.min(
                    self.limits.write_queue_bytes / crate::engine::profile::BLOCK_LEN as usize,
                );
                for capacity in peers {
                    if let Some(&p) = self.remote_slots.get(&capacity.id) {
                        self.remotes[p].output_capacity =
                            capacity.frames.min(crate::engine::profile::OUTBOUND_QUEUE);
                        self.remotes[p].uploaded = capacity.uploaded;
                        self.remotes[p].dirty = true;
                    }
                }
            }
            Input::BlockWritten { piece } => {
                let count = self
                    .writing
                    .get_mut(&piece)
                    .ok_or_else(|| invalid("unexpected write completion"))?;
                *count -= 1;
                if *count == 0 {
                    self.writing.remove(&piece);
                }
                self.mark_all_dirty();
            }
            Input::HashDone { piece, ok } => {
                let event = self.on_hash(piece, ok)?;
                self.deferred.push_back(event);
            }
            Input::ReadDone { request, block } => {
                if block
                    .as_ref()
                    .is_some_and(|b| b.len() != request.length as usize)
                {
                    return Err(invalid("read length mismatch"));
                }
                let read = self
                    .reads
                    .iter_mut()
                    .find(|read| read.request == request)
                    .ok_or_else(|| invalid("unexpected read completion"))?;
                read.result = Some(block);
                self.mark_all_dirty();
            }
            Input::Flushed => {
                if !self.flushing {
                    return Err(invalid("unexpected flush completion"));
                }
                self.flushed = true;
                self.flushing = false;
            }
            Input::Tick => {
                self.rate_deadline = None;
                self.mark_all_dirty();
            }
            Input::Stop => unreachable!("stop handled before dispatch"),
            Input::ConnectFailed { addr } => self.dialing.retain(|old| *old != addr),
            Input::Contacts(contacts) => {
                for addr in contacts
                    .into_iter()
                    .take(crate::engine::profile::MAX_QUEUED)
                {
                    if self.can_connect() && !self.rejects_contact(addr) {
                        self.dialing.push(addr);
                        self.actions.push(Action::Connect { addr });
                    }
                }
            }
            Input::Settings(settings) => {
                self.bandwidth.set_local(self.now, settings.bandwidth());
                self.pex_enabled = settings.pex;
                if !settings.dht {
                    self.dht_port = None;
                }
                let selected = if settings.file_priorities.is_empty() {
                    selection::FileSelection::all(self.meta.layout())
                } else {
                    selection::FileSelection::new(self.meta.layout(), &settings.file_priorities)?
                };
                let previous = self
                    .selection
                    .clone()
                    .unwrap_or_else(|| selection::FileSelection::all(self.meta.layout()));
                if previous.file_priorities() != selected.file_priorities() {
                    if !self.remotes.is_empty()
                        || self.picker.pieces().len() != 0
                        || self.flushing
                        || self.duplex.completion_reported
                    {
                        return Err(invalid("selection requires readmission"));
                    }
                    self.select_files(selected);
                }
                self.mark_all_dirty();
            }
        }
        Ok(())
    }
    fn pump(&mut self) -> io::Result<()> {
        if !self.started {
            self.started = true;
            self.actions.push(Action::Announce {
                event: AnnounceEvent::Started,
            });
        }
        self.expire_requests()?;
        self.turn_over_peers()?;
        if self.choker_dirty || self.choker.due(self.now, self.wanted_complete()) {
            let candidates: Vec<_> = self
                .remotes
                .iter()
                .map(|p| Candidate {
                    id: p.connection,
                    eligible: p.established && p.remote_interested && !p.retiring,
                    downloaded: p.download_rate.total(),
                    uploaded: p.uploaded,
                })
                .collect();
            self.choker.update(
                self.now,
                self.wanted_complete(),
                crate::engine::profile::UPLOAD_SLOTS.min(self.limits.peers),
                &candidates,
            );
            self.choker_dirty = false;
            self.mark_all_dirty();
        }
        for peer in 0..self.remotes.len() {
            self.remotes[peer].output_blocked = self.remotes[peer].output_capacity == 0;
            self.remotes[peer].upload_granted = self.choker.granted(self.remotes[peer].connection);
            if !self.remotes[peer].upload_granted {
                if self.remotes[peer].fast {
                    let queued: Vec<_> = self.remotes[peer].uploads.drain(..).collect();
                    self.remotes[peer].rejects.extend(queued);
                } else {
                    self.remotes[peer].uploads.clear();
                }
            }
            self.pump_peer(peer)?;
        }
        self.schedule_requests()?;
        self.pump_uploads()?;
        self.drain_hints();
        if self.wanted_complete()
            && !self.duplex.completion_reported
            && self.remotes.iter().all(|p| {
                p.retiring
                    || !p.established
                    || (!p.interested && !p.send_bitfield && p.pending_have.is_none())
            })
        {
            if self.flushed {
                self.duplex.completion_reported = true;
                if self.initial_verified_bytes < self.meta.layout().total_length()
                    && self.verified_count == self.meta.layout().piece_count()
                {
                    self.actions.push(Action::Announce {
                        event: AnnounceEvent::Completed,
                    });
                }
                self.actions.push(Action::Completed);
            } else if !self.flushing && self.write_capacity > 0 {
                self.write_capacity -= 1;
                self.flushing = true;
                self.actions.push(Action::Flush);
            }
        }
        if !self.wanted_complete()
            && self.remotes.is_empty()
            && self.writing.is_empty()
            && !self.picker.pieces().any(|p| p.hashing)
            && self.picker.stall().is_none()
        {
            self.set_stall(Stall::NoPeers);
        }
        if let Some(Event::Stalled(reason)) = self.picker.notification() {
            self.actions.push(Action::Stalled(reason));
        }
        let at = self
            .picker
            .next_deadline()
            .into_iter()
            .chain(self.choker.deadline())
            .chain(self.rate_deadline)
            .chain((!self.remotes.is_empty()).then_some(self.turnover_due))
            .min();
        if self.rate_waiting() {
            self.useful_at = self.now;
        }
        let idle_at = self
            .no_progress_timeout
            .filter(|_| !self.wanted_complete())
            .map(|timeout| self.useful_at + timeout);
        if idle_at.is_some_and(|at| self.now >= at) && !self.stall_reported {
            self.stall_reported = true;
            self.actions.push(Action::Stalled(Stall::NoProgress));
        }
        if let Some(at) = at
            .into_iter()
            .chain(idle_at.filter(|_| !self.stall_reported))
            .min()
        {
            self.actions.push(Action::WakeAt(at));
        }
        Ok(())
    }
    fn mark_all_dirty(&mut self) {
        for p in &mut self.remotes {
            p.dirty = true;
        }
    }
    fn retire_peer(&mut self, position: usize, reason: io::Error) -> io::Result<()> {
        let p = &mut self.remotes[position];
        if !p.retiring {
            p.retiring = true;
            p.uploads.clear();
            p.upload_granted = false;
            p.retirement_reason = Some(reason);
            self.picker.on_retiring(p.connection);
            self.choker_dirty = true;
            self.actions.push(Action::Disconnect { id: p.connection });
        }
        Ok(())
    }
    pub(in crate::engine) fn set_identity(&mut self, peer_id: [u8; 20]) {
        self.own_peer_id = Some(peer_id);
    }
    pub(in crate::engine) fn set_bandwidth(&mut self, bandwidth: bandwidth::Throttle) {
        self.bandwidth = bandwidth;
    }
    pub(in crate::engine) fn can_connect(&self) -> bool {
        self.metadata.as_ref().map_or_else(
            || {
                !self.wanted_complete()
                    && self.remotes.len() + self.dialing.len() < self.limits.peers
            },
            |fetch| fetch.can_connect(self.limits.peers),
        )
    }
    pub(in crate::engine) fn rejects_contact(&self, addr: SocketAddr) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(|fetch| fetch.rejects_contact(addr))
            || self.ip_banned(addr.ip())
            || self.dialing.contains(&addr)
            || self
                .remotes
                .iter()
                .any(|p| p.extensions.pex.endpoint == Some(addr))
    }
    pub(in crate::engine) fn set_no_progress_timeout(&mut self, timeout: Duration) {
        self.no_progress_timeout = Some(timeout);
    }
    pub(in crate::engine) fn rate_waiting(&self) -> bool {
        self.rate_deadline.is_some() && !self.remotes.is_empty()
    }
    pub(in crate::engine) fn info_hash(&self) -> InfoHashV1 {
        self.meta.0.as_ref().map_or_else(
            || self.metadata.as_ref().expect("metadata phase").hash,
            |meta| meta.info_hash(),
        )
    }
    pub(in crate::engine) fn piece_map(&self) -> std::sync::Arc<[bool]> {
        (0..self
            .meta
            .0
            .as_ref()
            .map_or(0, |meta| meta.layout().piece_count()))
            .map(|p| self.contains(p))
            .collect()
    }
    pub(in crate::engine) fn upload_reads_in_flight(&self) -> usize {
        self.reads
            .iter()
            .filter(|read| read.result.is_none())
            .count()
    }
    pub(in crate::engine) fn upload_reads_finished(&self) -> usize {
        self.reads
            .iter()
            .filter(|read| read.result.is_some())
            .count()
    }
    pub(in crate::engine) fn verified_count(&self) -> u32 {
        self.verified_count
    }
    pub(in crate::engine) fn contains(&self, piece: u32) -> bool {
        self.verified
            .get(piece as usize / 8)
            .is_some_and(|byte| byte & (0x80 >> (piece % 8)) != 0)
    }
    pub(in crate::engine) fn set_ports(
        &mut self,
        listen: Option<u16>,
        dht: Option<u16>,
        pex: bool,
    ) {
        self.listen_port = listen;
        self.dht_port = dht;
        self.pex_enabled = pex;
    }
    pub(in crate::engine) fn charge_received_payload(&mut self, bytes: usize) {
        self.received_payload_bytes += bytes as u64;
    }
}
