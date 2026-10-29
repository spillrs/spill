use super::*;

impl Torrent {
    pub(super) fn maybe_hash(&mut self) -> io::Result<()> {
        let indices: Vec<_> = self
            .picker
            .pieces()
            .filter(|piece| {
                !piece.hashing
                    && !self.writing.contains_key(&piece.index)
                    && self.picker.ready_to_hash(piece.index, self.meta.layout())
            })
            .map(|p| p.index)
            .collect();
        for index in indices {
            if self.write_capacity == 0 {
                break;
            }
            self.write_capacity -= 1;
            self.actions.push(Action::Hash { piece: index });
            self.picker.hash_started(index);
        }
        Ok(())
    }
    pub(super) fn restart_piece(&mut self, index: u32, corrupt: bool) -> io::Result<Event> {
        self.mark_all_dirty();
        self.actions.push(Action::Discard { piece: index });
        let culprit = corrupt.then(|| self.picker.culprit(index)).flatten();
        if let Some(source) = culprit
            && let Some(&position) = self.remote_slots.get(&source)
        {
            if let Some(id) = self.remotes[position]
                .peer_id
                .filter(|_| !self.remotes[position].web_seed)
            {
                self.banned
                    .retain(|(other, _, until)| *other != id && *until > self.now);
                if self.banned.len() >= self.limits.peers.saturating_mul(4).max(1) {
                    self.banned.pop_front();
                }
                self.banned.push_back((
                    id,
                    self.remotes[position]
                        .extensions
                        .pex
                        .endpoint
                        .map(|a| a.ip().to_canonical()),
                    self.now + Duration::from_secs(1800),
                ));
            }
            self.retire_peer(
                position,
                invalid_data_text("single-source piece hash mismatch"),
            )?;
        }
        if let Some(position) = self.remotes.iter().position(|p| {
            eligible(p, index)
                && !self.picker.used_peer(index, p.connection)
                && !p.asked.iter().any(|request| request.index == index)
        }) {
            self.peer_cursor = position;
        }
        for peer in &mut self.remotes {
            peer.asked.retain(|request| request.index != index);
        }
        self.picker.on_hash_result(index, false, self.now)
    }
    pub(super) fn on_hash(&mut self, index: u32, verified: bool) -> io::Result<Event> {
        self.mark_all_dirty();
        if self.picker.piece(index).is_none_or(|p| !p.hashing) {
            return Err(io::Error::other("unexpected hash result"));
        }
        if !verified {
            let range = self.meta.layout().piece_range(index).expect("piece");
            self.hash_failed_bytes += range.end - range.start;
            self.hash_failed_pieces += 1;
            return self.restart_piece(index, true);
        }
        self.mark_verified(index);
        self.useful_at = self.now;
        self.picker.on_hash_result(index, true, self.now)
    }
    pub(super) fn mark_verified(&mut self, index: u32) {
        if has(&self.verified, index) {
            return;
        }
        let range = self.meta.layout().piece_range(index).expect("piece");
        self.verified_bytes += range.end - range.start;
        self.wanted_verified_bytes += self
            .selection
            .as_ref()
            .map_or(range.end - range.start, |s| {
                s.piece_wanted_bytes(self.meta.layout(), index)
            });
        if !has(&self.excluded, index) {
            self.wanted_remaining -= 1;
        }
        set(&mut self.excluded, index);
        set(&mut self.verified, index);
        self.picker.unrequested(index, false);
        self.verified_count += 1;
        for peer in &mut self.remotes {
            if peer.first_needed == Some(index) {
                peer.first_needed = first_difference(&peer.pieces, &self.excluded, index + 1);
            }
            peer.pending_have = Some(peer.pending_have.map_or(index, |old| old.min(index)));
        }
    }
}
