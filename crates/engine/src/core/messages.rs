use super::*;

impl Torrent {
    pub(super) fn on_frame(
        &mut self,
        connection: ConnectionId,
        frame: Frame,
    ) -> io::Result<Option<Event>> {
        if let Message::Piece { block, .. } = frame.message() {
            self.charge_received_payload(block.len());
        }
        self.expire_requests()?;
        let Some(&position) = self.remote_slots.get(&connection) else {
            return Ok(None);
        };
        if self.remotes[position].retiring {
            return Ok(None);
        }
        self.remotes[position].dirty = true;
        if matches!(frame.message(), Message::Choke | Message::Reject(_)) {
            self.mark_all_dirty();
        }
        let wire_bytes = frame.wire_len();
        let message = frame.message();
        let block_limit = crate::profile::BLOCK_LEN;
        if let Message::Request(request) | Message::Cancel(request) = message
            && request.length > block_limit
            && request.validate(self.meta.layout(), u32::MAX).is_ok()
        {
            // A valid range above our upload block policy is not a bad
            // connection. Fast peers get a reject; legacy peers cannot
            // receive that extension, so ignore it. Repeated unsupported
            // traffic still spends the normal bounded ignored budget.
            if let Err(error) = self.remotes[position].unsolicited.charge(wire_bytes) {
                self.retire_peer(position, error)?;
            } else {
                self.remotes[position].first_message = false;
                if matches!(message, Message::Request(_)) && self.remotes[position].fast {
                    self.reject_upload(position, request)?;
                }
            }
            return Ok(None);
        }
        if let Err(error) = message.validate_layout(self.meta.layout(), block_limit) {
            self.retire_peer(position, invalid_data(error))?;
            return Ok(None);
        }
        if let Message::Extended { id, payload } = message {
            let peer = &mut self.remotes[position];
            let result = peer
                .unsolicited
                .charge(wire_bytes)
                .and_then(|()| peer.extensions.receive(id, payload, &self.meta, self.now));
            if let Err(error) = result {
                self.retire_peer(position, error)?;
            }
            return Ok(None);
        }
        if matches!(
            message,
            Message::HaveAll
                | Message::HaveNone
                | Message::Reject(_)
                | Message::SuggestPiece(_)
                | Message::AllowedFast(_)
        ) && !self.remotes[position].fast
        {
            self.retire_peer(
                position,
                invalid_data_text("Fast message without negotiation"),
            )?;
            return Ok(None);
        }
        let peer = &mut self.remotes[position];
        let first = peer.first_message;
        if !matches!(message, Message::KeepAlive) {
            peer.first_message = false;
        }
        match message {
            Message::Bitfield(bits) => {
                if !first {
                    if let Err(error) = peer.unsolicited.charge(wire_bytes) {
                        self.retire_peer(position, error)?;
                        return Ok(None);
                    }
                    // Bounded additional availability hints: union only,
                    // never erase HAVE history or grant upload authority.
                    let mut union = peer.pieces.clone();
                    for (old, new) in union.iter_mut().zip(bits) {
                        *old |= new;
                    }
                    self.picker.availability_changed(&peer.pieces, &union);
                    peer.have_count = union.iter().map(|b| b.count_ones()).sum();
                    peer.pieces = union;
                    peer.first_needed = first_difference(&peer.pieces, &self.excluded, 0);
                    return Ok(None);
                }
                self.picker.availability_changed(&peer.pieces, bits);
                peer.pieces.copy_from_slice(bits);
                peer.have_count = bits.iter().map(|b| b.count_ones()).sum();
                peer.first_needed = first_difference(bits, &self.excluded, 0);
            }
            Message::HaveAll | Message::HaveNone => {
                if !first {
                    self.retire_peer(
                        position,
                        invalid_data_text("late Fast availability message"),
                    )?;
                    return Ok(None);
                }
                let mut bits = vec![0; peer.pieces.len()];
                if matches!(message, Message::HaveAll) {
                    for index in 0..self.meta.layout().piece_count() {
                        set(&mut bits, index);
                    }
                }
                self.picker.availability_changed(&peer.pieces, &bits);
                peer.have_count = if matches!(message, Message::HaveAll) {
                    self.meta.layout().piece_count()
                } else {
                    0
                };
                peer.pieces = bits;
                peer.first_needed = first_difference(&peer.pieces, &self.excluded, 0);
            }
            Message::AllowedFast(index) => {
                set(&mut peer.allowed_fast, index);
            }
            Message::SuggestPiece(_) => {
                if let Err(error) = peer.unsolicited.charge(wire_bytes) {
                    self.retire_peer(position, error)?;
                }
            }
            Message::Reject(request) => {
                if peer.asked.contains(&request) {
                    peer.asked.remove_request(&request);
                    let deadline = self.now + crate::profile::REQUEST_TIMEOUT;
                    let primary = self.picker.rejected(request, connection, deadline);
                    if primary {
                        self.peer_cursor = position + 1;
                    }

                    if peer.choked {
                        peer.allowed_fast[request.index as usize / 8] &=
                            !(0x80 >> (request.index % 8));
                    }
                } else if let Err(error) = peer.unsolicited.charge(wire_bytes) {
                    self.retire_peer(position, error)?;
                }
            }
            Message::Have(index) => {
                if !has(&peer.pieces, index) {
                    // O(1): HAVE arrives once per piece per peer.
                    set(&mut peer.pieces, index);
                    peer.have_count += 1;
                    self.picker.gained(index);
                }
                if !has(&self.excluded, index) {
                    peer.first_needed = Some(peer.first_needed.map_or(index, |old| old.min(index)));
                }
            }
            Message::Choke => {
                peer.choked = true;
                let deadline = self.now + crate::profile::REQUEST_TIMEOUT;
                if !peer.fast {
                    self.picker.on_peer_gone(connection, deadline, true);
                }
            }
            Message::Unchoke => {
                peer.choked = false;
            }
            Message::Piece {
                index,
                begin,
                block,
            } => {
                let Some(pending) = self.picker.matching_block(index, begin, &peer.asked) else {
                    return self.ignored_block(position, wire_bytes, index, begin);
                };
                if pending.connection == Some(connection)
                    && pending
                        .deadline
                        .is_some_and(|deadline| deadline <= self.now)
                {
                    // The deadline can cross during frame validation,
                    // after peer_event's initial clock check. Apply the
                    // same retirement policy, never stage this late frame.
                    self.expire_requests()?;
                    if self.remotes[position].retiring {
                        return Ok(None);
                    }
                    // A different peer's exhausted budget can suspend
                    // deadline retirement. Such dropped late traffic must
                    // still spend this active connection's allowance.
                    return self.ignored_block(position, wire_bytes, index, begin);
                }
                if u64::try_from(block.len()).ok() != Some(u64::from(pending.request.length)) {
                    self.retire_peer(
                        position,
                        invalid_data_text("block response length differs from request"),
                    )?;
                    return Ok(None);
                }
                if !self.picker.accepts_peer(index, connection) {
                    return self.ignored_block(position, wire_bytes, index, begin);
                }
                let fulfilled = pending.request;
                if self.write_capacity == 0 {
                    return Err(invalid("frame without writer capacity"));
                }
                self.write_capacity -= 1;
                *self.writing.entry(index).or_default() += 1;
                self.actions.push(Action::Write {
                    piece: index,
                    begin,
                    block: frame.piece_block(),
                });
                peer.download_rate.received(self.now, block.len());
                peer.last_useful = self.now;
                self.useful_at = self.now;
                self.picker.on_block(fulfilled, connection)?;
                peer.asked.remove_request(&fulfilled);
                // Picker fulfillment identifies exactly the cancel key;
                // do not scan every peer's whole authorization window.
                for other in &mut self.remotes {
                    if other.asked.contains(&fulfilled) {
                        other.cancels.push_back(fulfilled);
                        other.dirty = true;
                    }
                }
                return Ok(Some(Event::Block {
                    index,
                    begin,
                    length: u32::try_from(block.len()).expect("validated block"),
                }));
            }
            Message::Request(request) => {
                self.request_upload(position, request, wire_bytes)?;
            }
            Message::Cancel(request) => {
                self.cancel_upload(position, request, wire_bytes)?;
            }
            Message::Interested | Message::NotInterested => {
                self.remote_interest(position, matches!(message, Message::Interested));
            }
            Message::Port(port) => {
                let admitted =
                    self.dht_port.is_some() && peer.remote_dht && !peer.dht_port_seen && port != 0;
                if let Err(error) = peer.unsolicited.charge(wire_bytes) {
                    self.retire_peer(position, error)?;
                } else if admitted {
                    peer.received_dht_port = Some(port);
                    peer.dht_port_seen = true;
                }
            }
            Message::KeepAlive => {}
            _ => {
                // Unsupported/inadmissible work, never negotiated extensions. Bound
                // ignored payloads/work, not only retained queue memory.
                if let Err(error) = peer.unsolicited.charge(wire_bytes) {
                    self.retire_peer(position, error)?;
                }
            }
        }
        Ok(None)
    }
}
