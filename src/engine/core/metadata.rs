//! BEP 9 is the first phase of the same clock-driven torrent, not a peer owner.
use super::*;
use crate::protocol::peer::{extension, metadata};
use sha1::{Digest, Sha1};

struct Peer {
    endpoint: Option<SocketAddr>,
    remote: extension::ExtensionMap,
    budget: UnsolicitedBudget,
    size: Option<u32>,
    bytes: Vec<u8>,
    requested: Option<(u32, u32)>,
    next_piece: u32,
    pending: VecDeque<Frame>,
    capacity: usize,
    deadline: Instant,
}
pub(super) struct Fetch {
    pub(super) hash: InfoHashV1,
    peers: HashMap<ConnectionId, Peer>,
    attached: HashMap<ConnectionId, Option<SocketAddr>>,
    dialing: Vec<SocketAddr>,
    stalled: bool,
}
impl Torrent {
    pub(in crate::engine) fn magnet(
        now: Instant,
        hash: InfoHashV1,
        limits: Profile,
    ) -> io::Result<Self> {
        let mut torrent = Self::empty(now, limits, Picker::new(1, 1, 1, 1)?);
        torrent.metadata = Some(Fetch {
            hash,
            peers: HashMap::new(),
            attached: HashMap::new(),
            dialing: Vec::new(),
            stalled: false,
        });
        Ok(torrent)
    }
    pub(super) fn handle_metadata(&mut self, input: Input<'_>) -> io::Result<()> {
        let mut fetch = self.metadata.take().expect("metadata phase");
        let resolved = match fetch.handle(self.now, input, self.limits, &mut self.actions) {
            Ok(resolved) => resolved,
            Err(error) => {
                self.metadata = Some(fetch);
                return Err(error);
            }
        };
        if let Some(meta) = resolved {
            for (id, _) in fetch.attached {
                self.actions.push(Action::Disconnect { id });
            }
            for id in fetch.peers.keys() {
                self.actions.push(Action::Disconnect { id: *id });
            }
            self.actions.push(Action::MetadataResolved(meta));
            self.failed = true;
        } else {
            self.metadata = Some(fetch);
        }
        Ok(())
    }
}
impl Fetch {
    pub(super) fn can_connect(&self, max: usize) -> bool {
        self.peers.len() + self.attached.len() + self.dialing.len() < max.min(3)
    }
    pub(super) fn rejects_contact(&self, addr: SocketAddr) -> bool {
        self.dialing.contains(&addr)
            || self.attached.values().any(|a| *a == Some(addr))
            || self.peers.values().any(|p| p.endpoint == Some(addr))
    }
    fn handle(
        &mut self,
        now: Instant,
        input: Input<'_>,
        profile: Profile,
        actions: &mut Vec<Action>,
    ) -> io::Result<Option<OwnedMetainfo>> {
        match input {
            Input::PeerAttached { id, endpoint, .. } => {
                if let Some(addr) = endpoint {
                    self.dialing.retain(|old| *old != addr);
                }
                if self.attached.len() + self.peers.len() >= profile.peers.min(3) {
                    actions.push(Action::Disconnect { id });
                } else {
                    self.attached.insert(id, endpoint);
                }
            }
            Input::PeerConnected { id, handshake } => {
                let endpoint = self.attached.remove(&id).flatten();
                if handshake.info_hash != *self.hash.as_bytes()
                    || handshake.reserved[5] & 0x10 == 0
                    || self.peers.len() >= profile.peers.min(3)
                {
                    actions.push(Action::Disconnect { id });
                } else {
                    let mut peer = Peer {
                        endpoint,
                        remote: extensions::remote_map(),
                        budget: UnsolicitedBudget::new(),
                        size: None,
                        bytes: Vec::new(),
                        requested: None,
                        next_piece: 0,
                        pending: VecDeque::new(),
                        capacity: 0,
                        deadline: now + crate::engine::profile::REQUEST_TIMEOUT,
                    };
                    if handshake.reserved[7] & 4 != 0 {
                        peer.queue(Message::HaveNone)?;
                    }
                    let hello = extensions::handshake(None, None, false)?;
                    peer.queue(Message::Extended {
                        id: 0,
                        payload: &hello,
                    })?;
                    self.peers.insert(id, peer);
                }
            }
            Input::PeerClosed { id } => {
                self.peers.remove(&id);
                self.attached.remove(&id);
            }
            Input::Frame { id, msg } => {
                if let Some(peer) = self.peers.get_mut(&id) {
                    match peer.frame(&msg, self.hash, profile) {
                        Ok(Some(meta)) => return Ok(Some(meta)),
                        Ok(None) => {}
                        Err(error)
                            if crate::engine::profile::AdmissionError::of(&error).is_some() =>
                        {
                            return Err(error);
                        }
                        Err(_) => {
                            self.peers.remove(&id);
                            actions.push(Action::Disconnect { id });
                        }
                    }
                }
            }
            Input::Ready { peers, .. } => {
                for capacity in peers {
                    if let Some(peer) = self.peers.get_mut(&capacity.id) {
                        peer.capacity = capacity.frames;
                    }
                }
            }
            Input::Contacts(contacts) => {
                for addr in contacts
                    .into_iter()
                    .take(crate::engine::profile::MAX_QUEUED)
                {
                    if self.can_connect(profile.peers) && !self.rejects_contact(addr) {
                        self.dialing.push(addr);
                        actions.push(Action::Connect { addr });
                    }
                }
            }
            Input::ConnectFailed { addr } => self.dialing.retain(|old| *old != addr),
            _ => {}
        }
        let cap = (crate::engine::profile::MAX_META - 8) as u32;
        self.peers.retain(|&id, peer| {
            if now >= peer.deadline {
                actions.push(Action::Disconnect { id });
                false
            } else {
                true
            }
        });
        for (&id, peer) in &mut self.peers {
            if peer.requested.is_none()
                && let (Some(total), Some(ext)) = (peer.size, peer.remote.id_for(b"ut_metadata"))
            {
                let payload = metadata::Message::new(
                    metadata::Kind::Request {
                        piece: peer.next_piece,
                    },
                    &[],
                    cap,
                )
                .map_err(invalid_data)?
                .encode(extensions::limits())
                .map_err(invalid_data)?;
                peer.queue(Message::Extended {
                    id: ext,
                    payload: &payload,
                })?;
                peer.requested = Some((peer.next_piece, total));
            }
            while peer.capacity > 0 {
                let Some(msg) = peer.pending.pop_front() else {
                    break;
                };
                peer.capacity -= 1;
                actions.push(Action::Send { id, msg });
            }
        }
        if let Some(at) = self.peers.values().map(|p| p.deadline).min() {
            actions.push(Action::WakeAt(at));
        }
        let empty = self.peers.is_empty() && self.attached.is_empty() && self.dialing.is_empty();
        if empty && !self.stalled {
            actions.push(Action::Stalled(Stall::NoPeers));
        }
        self.stalled = empty;
        Ok(None)
    }
}
impl Peer {
    fn queue(&mut self, message: Message<'_>) -> io::Result<()> {
        if self.pending.len() >= crate::engine::profile::MAX_REQUEST_QUEUE.max(3) {
            return Err(invalid("metadata output limit"));
        }
        self.pending.push_back(Frame::from_message(
            message,
            crate::engine::profile::MAX_FRAME_LENGTH,
        )?);
        Ok(())
    }
    fn frame(
        &mut self,
        frame: &Frame,
        hash: InfoHashV1,
        profile: Profile,
    ) -> io::Result<Option<OwnedMetainfo>> {
        let cap = (crate::engine::profile::MAX_META - 8) as u32;
        match frame.message() {
            Message::Extended { id: 0, payload } => {
                self.budget.charge(frame.wire_len())?;
                let hello = extension::Handshake::parse(payload, extensions::limits())
                    .map_err(invalid_data)?;
                self.remote.apply(&hello).map_err(invalid_data)?;
                if let Some(size) = hello.metadata_size() {
                    metadata::piece_range(0, size, cap).map_err(invalid_data)?;
                    if self.size.is_some_and(|old| old != size) {
                        return Err(invalid_data_text("metadata size changed"));
                    }
                    if self.size.is_none() {
                        self.bytes.reserve_exact(size as usize);
                        self.size = Some(size);
                    }
                }
                if self.requested.is_some() && self.remote.id_for(b"ut_metadata").is_none() {
                    return Err(invalid_data_text("metadata disabled"));
                }
            }
            Message::Extended {
                id: extensions::METADATA_ID,
                payload,
            } => {
                let message = metadata::Message::parse(payload, extensions::limits(), cap)
                    .map_err(invalid_data)?;
                match message.kind() {
                    metadata::Kind::Data { piece, total_size }
                        if self.requested == Some((piece, total_size)) =>
                    {
                        message
                            .validate_size(self.size.expect("requested size"), cap)
                            .map_err(invalid_data)?;
                        self.bytes.extend_from_slice(message.payload());
                        self.requested = None;
                        self.next_piece += 1;
                        if self.bytes.len() == total_size as usize {
                            if Sha1::digest(&self.bytes).as_slice() != hash.as_bytes() {
                                return Err(invalid_data_text("metadata info hash mismatch"));
                            }
                            let mut outer = b"d4:info".to_vec();
                            outer.extend_from_slice(&self.bytes);
                            outer.push(b'e');
                            return profile.parse_owned(&outer).map(Some).map_err(Into::into);
                        }
                    }
                    metadata::Kind::Data { .. } => {
                        self.budget.charge(frame.wire_len())?;
                        if let Some((_, expected)) = self.requested {
                            message.validate_size(expected, cap).map_err(invalid_data)?;
                        }
                    }
                    metadata::Kind::Reject { piece }
                        if self.requested.is_some_and(|(wanted, _)| wanted == piece) =>
                    {
                        return Err(invalid_data_text("metadata request rejected"));
                    }
                    metadata::Kind::Request { piece } => {
                        self.budget.charge(frame.wire_len())?;
                        if let Some(id) = self.remote.id_for(b"ut_metadata") {
                            let payload =
                                metadata::Message::new(metadata::Kind::Reject { piece }, &[], cap)
                                    .map_err(invalid_data)?
                                    .encode(extensions::limits())
                                    .map_err(invalid_data)?;
                            self.queue(Message::Extended {
                                id,
                                payload: &payload,
                            })?;
                        }
                    }
                    _ => self.budget.charge(frame.wire_len())?,
                }
            }
            _ => self.budget.charge(frame.wire_len())?,
        }
        Ok(None)
    }
}
