//! BEP 9 resolves hybrids immediately; v2-only payload needs authenticated layers.
use super::*;
use crate::protocol::{
    metainfo::InfoMetainfo,
    peer::{extension, hash::HashRequest, metadata},
};
use std::collections::BTreeMap;

struct Peer {
    endpoint: Option<SocketAddr>,
    v2: bool,
    remote: extension::ExtensionMap,
    budget: UnsolicitedBudget,
    size: Option<u32>,
    bytes: Vec<u8>,
    requested: Option<(u32, u32)>,
    hashes: Option<HashRequest>,
    next_piece: u32,
    pending: VecDeque<Frame>,
    capacity: usize,
    deadline: Instant,
}
pub(super) struct Fetch {
    pub(super) hash: TorrentIdentity,
    info: Option<InfoMetainfo>,
    discovery_bytes: usize,
    layers: BTreeMap<[u8; 32], BTreeMap<u32, Vec<u8>>>,
    peers: HashMap<ConnectionId, Peer>,
    attached: HashMap<ConnectionId, Option<SocketAddr>>,
    dialing: Vec<SocketAddr>,
    stalled: bool,
}
impl Torrent {
    pub(in crate::engine) fn magnet(
        now: Instant,
        hash: TorrentIdentity,
        limits: Profile,
        discovery_bytes: usize,
    ) -> io::Result<Self> {
        let mut torrent = Self::empty(now, limits, Picker::new(1, 1, 1, 1)?);
        torrent.metadata = Some(Fetch {
            hash,
            info: None,
            discovery_bytes,
            layers: BTreeMap::new(),
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
            for id in fetch.attached.keys().chain(fetch.peers.keys()) {
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
                let attached = self.attached.remove(&id);
                let endpoint = attached.flatten();
                let identity = self
                    .info
                    .as_ref()
                    .map_or(self.hash, InfoMetainfo::info_hash);
                let v2 = identity
                    .v2()
                    .is_some_and(|v2| v2.wire_key() == handshake.info_hash);
                if attached.is_none()
                    || !identity.wire_keys().any(|key| key == handshake.info_hash)
                    || handshake.reserved[5] & 0x10 == 0
                    || self.peers.len() >= profile.peers.min(3)
                {
                    actions.push(Action::Disconnect { id });
                } else {
                    let mut peer = Peer {
                        endpoint,
                        v2,
                        remote: extensions::remote_map(),
                        budget: UnsolicitedBudget::new(),
                        size: None,
                        bytes: Vec::new(),
                        requested: None,
                        hashes: None,
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
                    let result = if let Some(info) = &self.info
                        && matches!(
                            msg.message(),
                            Message::Hashes { .. } | Message::HashReject(_)
                        ) {
                        Self::hash_frame(peer, &msg, info, &mut self.layers).map(|()| None)
                    } else {
                        peer.frame(&msg, self.hash, profile)
                    };
                    match result {
                        Ok(Some(info)) if self.info.is_none() => {
                            if info.completed_size() + self.discovery_bytes
                                > crate::engine::profile::MAX_META
                            {
                                return Err(invalid_data(
                                    crate::protocol::metainfo::Error::MetadataLimit,
                                ));
                            }
                            if info.is_private() {
                                return Err(
                                    crate::engine::profile::AdmissionError::PrivateMagnet.into()
                                );
                            }
                            let identity = info.info_hash();
                            for peer in self.peers.values_mut() {
                                peer.bytes.clear();
                                peer.requested = None;
                                peer.deadline = now + crate::engine::profile::REQUEST_TIMEOUT;
                            }
                            self.hash = identity;
                            self.info = Some(info);
                        }
                        Ok(_) => {}
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
            if now >= peer.deadline && (self.info.is_none() || peer.hashes.is_some()) {
                actions.push(Action::Disconnect { id });
                false
            } else {
                true
            }
        });
        if let Some(info) = &self.info {
            if info.info_hash().v1().is_some() {
                actions.push(Action::MetadataProgress {
                    hash_bytes: 0,
                    hash_total_bytes: 0,
                });
                return info
                    .complete(&BTreeMap::new())
                    .map(Some)
                    .map_err(invalid_data);
            }
            let piece_length = info.layout().piece_length();
            let mut work = VecDeque::new();
            let mut totals = BTreeMap::new();
            for file in info
                .layout()
                .files()
                .iter()
                .filter(|f| f.length() > u64::from(piece_length))
            {
                let root = file.pieces_root().expect("v2 file root");
                let count = file.length().div_ceil(u64::from(piece_length)) as u32;
                if totals.insert(root, u64::from(count) * 32).is_some() {
                    continue;
                }
                for index in (0..count).step_by(512) {
                    if self
                        .layers
                        .get(&root)
                        .is_some_and(|ranges| ranges.contains_key(&index))
                        || self.peers.values().any(|peer| {
                            peer.hashes.is_some_and(|request| {
                                request.pieces_root == root && request.index == index
                            })
                        })
                    {
                        continue;
                    }
                    work.push_back((root, file.length(), index, count));
                }
            }
            let hash_bytes = self
                .layers
                .values()
                .flat_map(|ranges| ranges.values())
                .map(|bytes| bytes.len() as u64)
                .sum();
            let hash_total_bytes = totals.values().sum();
            actions.push(Action::MetadataProgress {
                hash_bytes,
                hash_total_bytes,
            });
            if hash_bytes == hash_total_bytes {
                let layers = std::mem::take(&mut self.layers)
                    .into_iter()
                    .map(|(root, ranges)| (root, ranges.into_values().flatten().collect()))
                    .collect();
                return info.complete(&layers).map(Some).map_err(invalid_data);
            }
            let idle: Vec<_> = self
                .peers
                .iter()
                .filter(|(_, peer)| peer.v2 && peer.hashes.is_none())
                .map(|(&id, _)| id)
                .collect();
            for id in idle {
                let request = if let Some((root, length, index, count)) = work.pop_front() {
                    let width = (count - index).next_power_of_two().min(512);
                    let blocks = length.div_ceil(crate::protocol::merkle::BLOCK_LENGTH as u64);
                    let height = u64::BITS - (blocks - 1).leading_zeros();
                    let base_layer =
                        (piece_length / crate::protocol::merkle::BLOCK_LENGTH as u32).ilog2();
                    let request = HashRequest {
                        pieces_root: root,
                        base_layer,
                        index,
                        length: width.max(2),
                        proof_layers: height - base_layer - 1,
                    };
                    request
                        .validate(length, 512)
                        .expect("constructed hash request geometry");
                    request
                } else {
                    let Some((request, _)) = self
                        .peers
                        .values()
                        .filter_map(|peer| peer.hashes.map(|request| (request, peer.deadline)))
                        .filter(|(request, _)| {
                            !self
                                .layers
                                .get(&request.pieces_root)
                                .is_some_and(|ranges| ranges.contains_key(&request.index))
                                && self
                                    .peers
                                    .values()
                                    .filter(|peer| peer.hashes == Some(*request))
                                    .count()
                                    == 1
                        })
                        .min_by_key(|(_, deadline)| *deadline)
                    else {
                        break;
                    };
                    request
                };
                let peer = self.peers.get_mut(&id).expect("idle metadata peer");
                peer.queue(Message::HashRequest(request))?;
                peer.hashes = Some(request);
                peer.deadline = now + crate::engine::profile::REQUEST_TIMEOUT;
            }
        }
        for (&id, peer) in &mut self.peers {
            if self.info.is_none()
                && peer.requested.is_none()
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
                peer.deadline = now + crate::engine::profile::REQUEST_TIMEOUT;
            }
            while peer.capacity > 0 {
                let Some(msg) = peer.pending.pop_front() else {
                    break;
                };
                peer.capacity -= 1;
                actions.push(Action::Send { id, msg });
            }
        }
        if let Some(at) = self
            .peers
            .values()
            .filter(|peer| self.info.is_none() || peer.hashes.is_some())
            .map(|p| p.deadline)
            .min()
        {
            actions.push(Action::WakeAt(at));
        }
        let empty = self.peers.is_empty() && self.attached.is_empty() && self.dialing.is_empty();
        if empty && !self.stalled {
            actions.push(Action::Stalled(Stall::NoPeers));
        }
        self.stalled = empty;
        Ok(None)
    }
    fn hash_frame(
        peer: &mut Peer,
        frame: &Frame,
        info: &InfoMetainfo,
        layers: &mut BTreeMap<[u8; 32], BTreeMap<u32, Vec<u8>>>,
    ) -> io::Result<()> {
        let Message::Hashes { request, hashes } = frame.message() else {
            return Err(invalid("hash request rejected"));
        };
        if !peer.v2 || peer.hashes != Some(request) {
            return Err(invalid("uncorrelated hashes"));
        }
        peer.hashes = None;
        let file = info
            .layout()
            .files()
            .iter()
            .find(|file| file.pieces_root() == Some(request.pieces_root))
            .expect("requested root");
        request
            .verify(file.length(), 512, hashes)
            .map_err(invalid_data)?;
        let count = file
            .length()
            .div_ceil(u64::from(info.layout().piece_length())) as u32;
        let used = request.length.min(count - request.index) as usize * 32;
        layers
            .entry(request.pieces_root)
            .or_default()
            .entry(request.index)
            .or_insert_with(|| hashes[..used].to_vec());
        Ok(())
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
        hash: TorrentIdentity,
        profile: Profile,
    ) -> io::Result<Option<InfoMetainfo>> {
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
                            if !hash.matches_info_bytes(&self.bytes) {
                                return Err(invalid_data_text("metadata info hash mismatch"));
                            }
                            let info = profile.parse_info(&self.bytes)?;
                            let known = info.info_hash();
                            if hash.v1().is_some_and(|hash| known.v1() != Some(hash))
                                || hash.v2().is_some_and(|hash| known.v2() != Some(hash))
                            {
                                return Err(invalid_data_text(
                                    "metadata format does not match magnet topics",
                                ));
                            }
                            return Ok(Some(info));
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
