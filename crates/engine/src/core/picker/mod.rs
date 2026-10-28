//! Indexed scheduling owns selection, requests, provenance and retry epochs.
//! Peeking never charges work; only successful transport admission advances it.
use super::{Event, Stall};
use crate::{core::ConnectionId, selection::FilePriority};
use protocol::{
    metainfo::{FileKind, Layout},
    peer::message::BlockRequest,
};
use std::time::Instant;
use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
    io,
};

mod blocks;
use blocks::*;

pub(crate) struct Pending {
    pub(crate) connection: Option<ConnectionId>,
    pub(crate) request: BlockRequest,
    pub(crate) deadline: Option<Instant>,
    pub(crate) reassignments: u32,
    pub(crate) retry_grant: bool,
}
impl Pending {
    fn can_reassign(&self, limit: u32) -> bool {
        self.retry_grant || self.reassignments < limit
    }
}
pub(crate) struct Piece {
    pub(crate) index: u32,
    pub(crate) cursor: u32,
    // Last data end learned by the peek; only transport admission commits it.
    exhausted_at: Option<u32>,
    pub(crate) hashing: bool,
    reserved: bool,
    pub(crate) retries: u32,
    pub(crate) retry_deadline: Option<Instant>,
    pub(crate) isolated: bool,
    pub(crate) source: Option<ConnectionId>,
    pub(super) sources: Vec<(BlockRequest, ConnectionId)>,
    requests: Vec<Pending>,
}
// Leave replay headroom beneath the independent M1 queue + piece/peer bound.
pub(crate) const PIECE_BYTES: u64 = 12 * 1024 * 1024;

struct PieceBudget {
    piece_length: u64,
    total_length: u64,
}
impl PieceBudget {
    fn length(&self, index: u32) -> u64 {
        self.piece_length.min(
            self.total_length
                .saturating_sub(u64::from(index) * self.piece_length),
        )
    }
}

pub(crate) struct Picker {
    pieces: BTreeMap<u32, Piece>,
    // Positions in each piece's bounded Vec; swap-remove repairs the moved key.
    requests: HashMap<(u32, u32), usize>,
    deadlines: BTreeMap<Instant, usize>,
    not_live: usize,
    unrequested: Vec<u8>,
    unrequested_count: u32,
    availability: Vec<u32>,
    priorities: Vec<FilePriority>,
    random: Cell<u64>,
    window: usize,
    max_pieces: usize,
    byte_budget: Option<PieceBudget>,
    source_limit: usize,
    stall: Option<Stall>,
    reported: bool,
}
impl Picker {
    pub(crate) fn bounded(
        window: usize,
        source_limit: usize,
        layout: &Layout<'_>,
        seed: u64,
    ) -> io::Result<Self> {
        let piece_length = u64::from(layout.piece_length());
        let slots = PIECE_BYTES.div_ceil(piece_length).clamp(2, 64) as usize;
        let mut picker = Self::new(window, source_limit, slots, seed)?;
        picker.byte_budget = Some(PieceBudget {
            piece_length,
            total_length: layout.total_length(),
        });
        Ok(picker)
    }

    pub(crate) fn set_priorities(&mut self, priorities: &[FilePriority]) {
        self.priorities = priorities.to_vec();
    }

    pub(crate) fn restore_missing(&mut self, piece_count: u32, verified: &[u8]) {
        self.unrequested = vec![0; piece_count.div_ceil(8) as usize];
        self.unrequested_count = 0;
        for index in 0..piece_count {
            self.unrequested(index, !super::has(verified, index));
        }
    }
    pub(crate) fn unrequested(&mut self, index: u32, wanted: bool) {
        let Some(byte) = self.unrequested.get_mut(index as usize / 8) else {
            return;
        };
        let mask = 0x80 >> (index % 8);
        if (*byte & mask != 0) != wanted {
            if wanted {
                *byte |= mask;
                self.unrequested_count += 1;
            } else {
                *byte &= !mask;
                self.unrequested_count -= 1;
            }
        }
    }
    // Count reservations, not dormant corrupt-retry metadata. A failed piece
    // owns no payload after discard and must reacquire capacity before retrying.
    fn remaining_bytes(&self) -> Option<u64> {
        let active = self.pieces.values().filter(|p| p.reserved);
        if active.clone().count() >= self.max_pieces {
            return None;
        }
        Some(self.byte_budget.as_ref().map_or(u64::MAX, |budget| {
            let bytes: u64 = active.map(|p| budget.length(p.index)).sum();
            // Profiles may admit pieces larger than the budget. Allow one alone
            // rather than deadlocking; oversize pieces still run alone.
            PIECE_BYTES.max(budget.piece_length) - bytes
        }))
    }
    fn fits(&self, index: u32, remaining: u64) -> bool {
        self.byte_budget.as_ref().is_none_or(|budget| {
            let length = budget.length(index);
            length != 0 && length <= remaining
        })
    }
    fn can_reserve(&self, index: u32) -> bool {
        self.remaining_bytes()
            .is_some_and(|bytes| self.fits(index, bytes))
    }

    pub(crate) fn new(
        window: usize,
        source_limit: usize,
        max_pieces: usize,
        seed: u64,
    ) -> io::Result<Self> {
        Ok(Self {
            pieces: BTreeMap::new(),
            requests: HashMap::new(),
            deadlines: BTreeMap::new(),
            not_live: 0,
            unrequested: Vec::new(),
            unrequested_count: 0,
            availability: Vec::new(),
            priorities: Vec::new(),
            random: Cell::new(seed.max(1)),
            window,
            max_pieces,
            byte_budget: None,
            source_limit,
            stall: None,
            reported: false,
        })
    }
    /// Update from old/new membership; repeated HAVE/bitfield hints are idempotent.
    pub(crate) fn availability_changed(&mut self, old: &[u8], new: &[u8]) {
        let bytes = old.len().max(new.len());
        if self.availability.len() < bytes * 8 {
            self.availability.resize(bytes * 8, 0);
        }
        // Scan bytes, updating counts only for changed bits. Single HAVEs
        // bypass this scan entirely via `gained`.
        for byte in 0..bytes {
            let before = old.get(byte).copied().unwrap_or(0);
            let after = new.get(byte).copied().unwrap_or(0);
            let mut changed = before ^ after;
            while changed != 0 {
                let bit = changed.leading_zeros() as usize;
                changed &= !(0x80 >> bit);
                let count = &mut self.availability[byte * 8 + bit];
                if after & (0x80 >> bit) != 0 {
                    *count += 1;
                } else {
                    *count = count.checked_sub(1).expect("availability underflow");
                }
            }
        }
    }
    /// One peer newly has `index` (HAVE). Equivalent to `availability_changed`
    /// for a single set bit, without touching the rest of the bitfield.
    pub(crate) fn gained(&mut self, index: u32) {
        let index = index as usize;
        if self.availability.len() <= index {
            self.availability.resize((index / 8 + 1) * 8, 0);
        }
        self.availability[index] += 1;
    }
    fn random(&self) -> u64 {
        let mut value = self.random.get();
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.random.set(value);
        value
    }
    /// Random first four verified pieces, then rarest-first with random ties.
    /// Admitted pieces are handled before new selection by the scheduler.
    pub(crate) fn select_piece<'a, I>(&self, available: I, verified: &[u8]) -> Option<u32>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: Clone,
    {
        self.select_prioritized_piece(available, verified, &self.priorities)
    }

    /// File selection precedes rarity, including during initial random picking.
    /// An empty priorities slice preserves the all-files scheduling policy.
    pub(crate) fn select_prioritized_piece<'a, I>(
        &self,
        available: I,
        verified: &[u8],
        priorities: &[FilePriority],
    ) -> Option<u32>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: Clone,
    {
        let remaining = self.remaining_bytes()?;
        let available = available.into_iter();
        let initial = verified.iter().map(|b| b.count_ones()).sum::<u32>() < 4;
        let mut best = None;
        let mut rarity = u32::MAX;
        let mut priority = FilePriority::Skip;
        let mut ties = 0;
        for (byte, &done) in verified.iter().enumerate() {
            // Skip whole bytes with nothing both offered and still needed.
            let offered = available
                .clone()
                .fold(0u8, |acc, bits| acc | bits.get(byte).copied().unwrap_or(0));
            let mut wanted = offered & !done;
            while wanted != 0 {
                let bit = wanted.leading_zeros();
                wanted &= !(0x80 >> bit);
                let index = byte as u32 * 8 + bit;
                let candidate_priority = if priorities.is_empty() {
                    FilePriority::Normal
                } else {
                    priorities
                        .get(index as usize)
                        .copied()
                        .unwrap_or(FilePriority::Skip)
                };
                if candidate_priority == FilePriority::Skip
                    || candidate_priority < priority
                    || self.pieces.contains_key(&index)
                    || !self.fits(index, remaining)
                {
                    continue;
                }
                if candidate_priority > priority {
                    priority = candidate_priority;
                    rarity = u32::MAX;
                    ties = 0;
                }
                let count = if initial {
                    0
                } else {
                    self.availability.get(index as usize).copied().unwrap_or(0)
                };
                if count < rarity {
                    rarity = count;
                    ties = 0;
                }
                if count == rarity {
                    ties += 1;
                    if self.random().is_multiple_of(ties) {
                        best = Some(index);
                    }
                }
            }
        }
        best
    }
    pub(crate) fn start(&mut self, index: u32) -> io::Result<()> {
        if !self.can_reserve(index) || self.pieces.contains_key(&index) {
            return Err(io::Error::other(
                "piece concurrency limit or duplicate piece",
            ));
        }
        let sources = Vec::with_capacity(self.window.min(self.source_limit));
        let requests = Vec::with_capacity(self.window.min(self.source_limit));
        self.unrequested(index, true);
        self.pieces.insert(
            index,
            Piece {
                index,
                cursor: 0,
                exhausted_at: None,
                hashing: false,
                reserved: true,
                retries: 0,
                retry_deadline: None,
                isolated: false,
                source: None,
                sources,
                requests,
            },
        );
        Ok(())
    }
    pub(crate) fn prefer(&mut self, index: u32, connection: ConnectionId) {
        self.pieces.get_mut(&index).expect("admitted piece").source = Some(connection);
    }
    pub(crate) fn pin(&mut self, index: u32, connection: ConnectionId) {
        let piece = self.pieces.get_mut(&index).expect("admitted piece");
        piece.isolated = true;
        piece.source = Some(connection);
    }
    pub(crate) fn piece(&self, index: u32) -> Option<&Piece> {
        self.pieces.get(&index)
    }
    pub(crate) fn pieces(&self) -> impl DoubleEndedIterator<Item = &Piece> + ExactSizeIterator {
        self.pieces.values()
    }
    pub(crate) fn requests(&self, index: u32) -> &[Pending] {
        self.piece(index)
            .map_or(&[], |piece| piece.requests.as_slice())
    }
    pub(crate) fn all_requests(&self) -> impl Iterator<Item = &Pending> {
        self.pieces.values().flat_map(|piece| &piece.requests)
    }
    pub(crate) fn request_count(&self) -> usize {
        self.requests.len()
    }
    pub(crate) fn pending(&self, request: BlockRequest) -> Option<&Pending> {
        let position = *self.requests.get(&(request.index, request.begin))?;
        self.piece(request.index)?
            .requests
            .get(position)
            .filter(|pending| pending.request == request)
    }
    pub(crate) fn finish(&mut self, index: u32) {
        self.unrequested(index, false);
        self.clear_requests(index);
        if let Some(piece) = self.pieces.remove(&index) {
            change_deadline(&mut self.deadlines, piece.retry_deadline, None);
        }
    }
    pub(crate) fn clear(&mut self) {
        let active: Vec<_> = self.pieces.keys().copied().collect();
        for index in active {
            self.unrequested(index, true);
        }
        self.requests.clear();
        self.deadlines.clear();
        self.not_live = 0;
        self.pieces.clear();
    }
    pub(crate) fn stall(&self) -> Option<Stall> {
        self.stall
    }
    pub(crate) fn set_stall(&mut self, reason: Stall) {
        if self.stall != Some(reason) {
            self.stall = Some(reason);
            self.reported = false;
        }
    }
    pub(crate) fn clear_wait_stall(&mut self) {
        self.stall = None;
        self.reported = false;
    }
    pub(crate) fn notification(&mut self) -> Option<Event> {
        if self.reported {
            return None;
        }
        let reason = self.stall?;
        self.reported = true;
        Some(Event::Stalled(reason))
    }
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.deadlines
            .first_key_value()
            .map(|(&deadline, _)| deadline)
    }
    pub(crate) fn next_request(
        &mut self,
        index: u32,
        layout: &Layout<'_>,
        now: Instant,
    ) -> Option<BlockRequest> {
        if self
            .piece(index)?
            .retry_deadline
            .is_some_and(|due| due > now)
        {
            return None;
        }
        if !self.piece(index)?.reserved {
            if !self.can_reserve(index) {
                return None;
            }
            self.pieces.get_mut(&index)?.reserved = true;
        }
        for pending in self.requests(index) {
            if pending.connection.is_none() {
                if !pending.can_reassign(crate::profile::MAX_REASSIGNMENTS) {
                    let piece = self.pieces.get_mut(&index)?;
                    for request in &mut piece.requests {
                        if request.connection.is_none() {
                            request.reassignments = 0;
                            request.retry_grant = true;
                        }
                    }
                    let deadline = now + crate::profile::STALL_RETRY_INITIAL;
                    change_deadline(&mut self.deadlines, piece.retry_deadline, Some(deadline));
                    piece.retry_deadline = Some(deadline);
                    return None;
                }
                return Some(pending.request);
            }
        }
        let full = self.request_count() >= self.window;
        let piece = self.pieces.get_mut(&index)?;
        if piece.hashing {
            return None;
        }
        let request = next_block(piece, layout, crate::profile::BLOCK_LEN);
        if full && request.is_some() {
            return None;
        }
        if let Some(request) = request {
            let end = request.begin + request.length;
            if next_block_at(index, end, layout, crate::profile::BLOCK_LEN).is_none() {
                piece.exhausted_at = Some(end);
            }
        } else {
            let range = layout.piece_range(index).expect("piece index");
            piece.cursor = u32::try_from(range.end - range.start).expect("piece length");
            self.unrequested(index, false);
        }
        request
    }
    /// Endgame starts only after every missing block has a live request.
    pub(crate) fn endgame(&self) -> bool {
        self.request_count() != 0 && self.unrequested_count == 0 && self.not_live == 0
    }
    pub(crate) fn accepts_peer(&self, index: u32, connection: ConnectionId) -> bool {
        self.piece(index)
            .is_some_and(|p| !p.isolated || p.source.is_none_or(|s| s == connection))
    }
    pub(crate) fn admitted(
        &mut self,
        request: BlockRequest,
        connection: ConnectionId,
        deadline: Instant,
    ) {
        let count = self.request_count();
        let piece = self.pieces.get_mut(&request.index).expect("piece");
        if let Some(&position) = self.requests.get(&(request.index, request.begin)) {
            let pending = &mut piece.requests[position];
            assert_eq!(pending.request, request);
            set_pending(
                pending,
                Some(connection),
                Some(deadline),
                &mut self.deadlines,
                &mut self.not_live,
            );
            if pending.retry_grant {
                pending.retry_grant = false;
            } else {
                pending.reassignments += 1;
            }
        } else {
            assert!(count < self.window, "request window exceeded");
            self.requests
                .insert((request.index, request.begin), piece.requests.len());
            change_deadline(&mut self.deadlines, None, Some(deadline));
            piece.requests.push(Pending {
                request,
                connection: Some(connection),
                deadline: Some(deadline),
                reassignments: 0,
                retry_grant: false,
            });
            piece.cursor = request.begin + request.length;
        }
        change_deadline(&mut self.deadlines, piece.retry_deadline, None);
        piece.retry_deadline = None;
        if piece.isolated || piece.source.is_none() {
            piece.source = Some(connection);
        }
        let exhausted = piece.exhausted_at == Some(piece.cursor);
        if exhausted {
            self.unrequested(request.index, false);
        }
        self.clear_wait_stall();
    }
    pub(crate) fn ready_to_hash(&self, index: u32, layout: &Layout<'_>) -> bool {
        self.piece(index).is_some_and(|p| {
            p.reserved
                && (p.hashing
                    || (p.requests.is_empty()
                        && next_block(p, layout, crate::profile::BLOCK_LEN).is_none()))
        })
    }
    pub(crate) fn hash_started(&mut self, index: u32) {
        let piece = self.pieces.get_mut(&index).expect("piece");
        piece.hashing = true;
        change_deadline(&mut self.deadlines, piece.retry_deadline, None);
        piece.retry_deadline = None;
        self.unrequested(index, false);
    }
    pub(crate) fn matching_block(
        &self,
        index: u32,
        begin: u32,
        authorized: &[BlockRequest],
    ) -> Option<&Pending> {
        let position = *self.requests.get(&(index, begin))?;
        let pending = self.piece(index)?.requests.get(position)?;
        authorized.contains(&pending.request).then_some(pending)
    }
    pub(crate) fn on_block(
        &mut self,
        request: BlockRequest,
        connection: ConnectionId,
    ) -> io::Result<()> {
        let piece = self
            .pieces
            .get_mut(&request.index)
            .ok_or_else(|| io::Error::other("block without piece"))?;
        if piece.sources.len() >= self.source_limit {
            return Err(io::Error::other("block provenance bound violated"));
        }
        let position = *self
            .requests
            .get(&(request.index, request.begin))
            .filter(|&&position| piece.requests[position].request == request)
            .ok_or_else(|| io::Error::other("block without request"))?;
        piece.sources.push((request, connection));
        let removed = piece.requests.swap_remove(position);
        change_deadline(&mut self.deadlines, removed.deadline, None);
        self.not_live -= usize::from(removed.connection.is_none() || removed.deadline.is_none());
        self.requests.remove(&(request.index, request.begin));
        if let Some(moved) = piece.requests.get(position) {
            self.requests
                .insert((moved.request.index, moved.request.begin), position);
        }
        self.clear_wait_stall();
        Ok(())
    }
    pub(crate) fn used_peer(&self, index: u32, connection: ConnectionId) -> bool {
        self.piece(index).is_some_and(|piece| {
            piece
                .sources
                .iter()
                .any(|(_, source)| *source == connection)
        })
    }
    pub(crate) fn culprit(&self, index: u32) -> Option<ConnectionId> {
        let sources = &self.piece(index)?.sources;
        let source = sources.first()?.1;
        sources
            .iter()
            .all(|(_, other)| *other == source)
            .then_some(source)
    }
    pub(crate) fn on_hash_result(
        &mut self,
        index: u32,
        matched: bool,
        now: Instant,
    ) -> io::Result<Event> {
        if matched {
            self.finish(index);
            return Ok(Event::Verified(index));
        }
        if !self.requests(index).is_empty() {
            return Err(io::Error::other("restart with outstanding requests"));
        }
        let piece = self
            .pieces
            .get_mut(&index)
            .ok_or_else(|| io::Error::other("missing failed piece"))?;
        piece.retries = piece.retries.saturating_add(1);
        let delay = if piece.retries > crate::profile::MAX_PIECE_RETRIES {
            crate::profile::STALL_RETRY_INITIAL
                .saturating_mul(
                    1u32 << piece
                        .retries
                        .saturating_sub(crate::profile::MAX_PIECE_RETRIES)
                        .saturating_sub(1)
                        .min(31),
                )
                .min(crate::profile::STALL_RETRY_MAX)
        } else {
            std::time::Duration::ZERO
        };
        let deadline = now + delay;
        piece.cursor = 0;
        piece.exhausted_at = None;
        piece.sources.clear();
        piece.source = None;
        piece.hashing = false;
        piece.isolated = true;
        change_deadline(&mut self.deadlines, piece.retry_deadline, Some(deadline));
        piece.retry_deadline = Some(deadline);
        if self.byte_budget.is_some() {
            piece.reserved = false;
            // Dormant entries are bounded by the metainfo piece count, not the
            // active-piece limit. Retain only retry metadata, not block arrays.
            piece.sources.shrink_to_fit();
            piece.requests.shrink_to_fit();
        }
        let retry = piece.retries;
        self.unrequested(index, true);
        self.clear_wait_stall();
        Ok(Event::PieceRetry { index, retry })
    }
    pub(crate) fn rejected(
        &mut self,
        request: BlockRequest,
        connection: ConnectionId,
        deadline: Instant,
    ) -> bool {
        if let Some(piece) = self.pieces.get_mut(&request.index)
            && let Some(&position) = self.requests.get(&(request.index, request.begin))
            && let Some(pending) = piece.requests.get_mut(position)
            && pending.request == request
            && pending.connection == Some(connection)
        {
            set_pending(
                pending,
                None,
                Some(deadline),
                &mut self.deadlines,
                &mut self.not_live,
            );
            pending.retry_grant = true;
            // Whole-piece affinity is a preference, not authority to monopolize
            // rejected work. Isolated retries must still keep one source.
            if !piece.isolated && piece.source == Some(connection) {
                piece.source = None;
            }
            return true;
        }
        false
    }
    pub(crate) fn on_retiring(&mut self, connection: ConnectionId) {
        for piece in self.pieces.values_mut() {
            for pending in &mut piece.requests {
                if pending.connection == Some(connection) {
                    set_pending(
                        pending,
                        pending.connection,
                        None,
                        &mut self.deadlines,
                        &mut self.not_live,
                    );
                }
            }
        }
    }
    pub(crate) fn on_peer_gone(
        &mut self,
        connection: ConnectionId,
        deadline: Instant,
        choke: bool,
    ) {
        for piece in self.pieces.values_mut() {
            for pending in &mut piece.requests {
                if pending.connection == Some(connection) {
                    set_pending(
                        pending,
                        None,
                        Some(deadline),
                        &mut self.deadlines,
                        &mut self.not_live,
                    );
                    pending.retry_grant |= choke;
                }
            }
        }
    }
    pub(crate) fn release_source(&mut self, index: u32) {
        self.pieces.get_mut(&index).expect("piece").source = None;
    }
    pub(crate) fn clear_requests(&mut self, index: u32) {
        let Some(piece) = self.pieces.get_mut(&index) else {
            return;
        };
        for pending in &piece.requests {
            self.requests.remove(&(index, pending.request.begin));
            change_deadline(&mut self.deadlines, pending.deadline, None);
            self.not_live -=
                usize::from(pending.connection.is_none() || pending.deadline.is_none());
        }
        piece.requests.clear();
    }
    pub(crate) fn clear_all_requests(&mut self) {
        self.requests.clear();
        self.not_live = 0;
        for piece in self.pieces.values_mut() {
            for pending in &piece.requests {
                change_deadline(&mut self.deadlines, pending.deadline, None);
            }
            piece.requests.clear();
        }
    }
    pub(crate) fn tick(&mut self, now: Instant) -> Option<ConnectionId> {
        if self.next_deadline().is_none_or(|deadline| deadline > now) {
            return None;
        }
        let mut expired_piece = false;
        for piece in self.pieces.values_mut() {
            if piece.retry_deadline.is_some_and(|at| at <= now) {
                change_deadline(&mut self.deadlines, piece.retry_deadline, None);
                piece.retry_deadline = None;
                expired_piece = true;
            }
        }
        if expired_piece {
            self.set_stall(Stall::AwaitingPeers);
        }
        while let Some((index, position)) = self.pieces.iter().find_map(|(&index, piece)| {
            piece
                .requests
                .iter()
                .position(|p| p.deadline.is_some_and(|at| at <= now))
                .map(|position| (index, position))
        }) {
            let piece = self.piece(index).expect("piece");
            let pending = &piece.requests[position];
            if let Some(connection) = pending.connection {
                self.on_retiring(connection);
                return Some(connection);
            }
            let pending = &mut self.pieces.get_mut(&index).expect("piece").requests[position];
            set_pending(pending, None, None, &mut self.deadlines, &mut self.not_live);
            self.set_stall(Stall::AwaitingPeers);
        }
        None
    }
}
