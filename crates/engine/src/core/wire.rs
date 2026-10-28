use super::*;

impl Torrent {
    pub(super) fn drain_hints(&mut self) {
        let local: Vec<_> = self
            .remotes
            .iter()
            .filter_map(|p| {
                Some(SocketAddr::new(
                    p.extensions.pex.local_ip?,
                    self.listen_port?,
                ))
            })
            .collect();
        for peer in &mut self.remotes {
            if self.pex_enabled {
                let mut contacts = std::mem::take(&mut peer.extensions.pex.received);
                contacts.retain(|a| !local.contains(a) && Some(*a) != peer.extensions.pex.endpoint);
                if !contacts.is_empty() {
                    self.actions.push(Action::PeerHints {
                        id: peer.connection,
                        contacts,
                    });
                }
            }
            if self.dht_port.is_some()
                && let Some(port) = peer.received_dht_port.take()
                && let Some(mut addr) = peer.extensions.pex.endpoint
            {
                addr.set_port(port);
                self.actions.push(Action::DhtHint { addr });
            }
        }
    }
    pub(super) fn pump_peer(&mut self, peer: usize) -> io::Result<()> {
        if !self.remotes[peer].established || self.remotes[peer].retiring {
            return Ok(());
        }
        // Fulfillment removes the picker request immediately. Other peers retain
        // authorization until their Cancel is admitted, even under backpressure.
        while let Some(request) = self.remotes[peer].cancels.front().copied() {
            if self
                .picker
                .pending(request)
                .is_some_and(|pending| pending.connection == Some(self.remotes[peer].connection))
            {
                // A restarted piece may have reissued this wire key. The old
                // cancel must not revoke its new authorization.
                self.remotes[peer].cancels.pop_front();
                continue;
            }
            if !self.send(peer, Message::Cancel(request))? {
                return Ok(());
            }
            self.remotes[peer].asked.remove_request(&request);
            self.remotes[peer].cancels.pop_front();
        }
        if self.remotes[peer].send_bitfield {
            let availability = if self.remotes[peer].fast && self.verified_count == 0 {
                Message::HaveNone
            } else if self.remotes[peer].fast
                && self.verified_count == self.meta.layout().piece_count()
            {
                Message::HaveAll
            } else {
                Message::Bitfield(&self.verified)
            };
            let frame = Frame::from_message(availability, crate::profile::MAX_FRAME_LENGTH)?;
            if !self.queue_frame(peer, frame)? {
                return Ok(());
            }
            self.remotes[peer].send_bitfield = false;
            self.remotes[peer].announced.copy_from_slice(&self.verified);
            self.remotes[peer].pending_have = None;
        }
        if self.remotes[peer].extensions.handshake_pending {
            let payload = crate::core::extensions::handshake(
                Some(self.meta.info_bytes().len()),
                self.listen_port,
                self.pex_enabled,
            )?;
            if !self.send(
                peer,
                Message::Extended {
                    id: 0,
                    payload: &payload,
                },
            )? {
                return Ok(());
            }
            self.remotes[peer].extensions.handshake_pending = false;
        }
        if self.remotes[peer].send_dht_port {
            if let Some(port) = self.dht_port
                && !self.send(peer, Message::Port(port))?
            {
                return Ok(());
            }
            self.remotes[peer].send_dht_port = false;
        }
        while let Some((id, payload)) = self.remotes[peer].extensions.response(&self.meta)? {
            if !self.send(
                peer,
                Message::Extended {
                    id,
                    payload: &payload,
                },
            )? {
                return Ok(());
            }
            self.remotes[peer].extensions.sent();
        }
        if self.pex_enabled
            && self.remotes[peer].extensions.pex.due(self.now)
            && let Some(id) = self.remotes[peer].extensions.remote.id_for(b"ut_pex")
        {
            let recipient = self.remotes[peer].extensions.pex.endpoint;
            let mut contacts = Vec::new();
            for other in &self.remotes {
                if other.connection != self.remotes[peer].connection
                    && other.established
                    && !other.retiring
                    && let Some(address) = other.extensions.pex.endpoint
                    && Some(address) != recipient
                    && crate::network::eligible(address)
                    && !contacts.contains(&address)
                {
                    contacts.push(address);
                }
            }
            if let Some(payload) = self.remotes[peer]
                .extensions
                .pex
                .update(&contacts, self.now)?
            {
                if !self.send(
                    peer,
                    Message::Extended {
                        id,
                        payload: &payload,
                    },
                )? {
                    return Ok(());
                }
                self.remotes[peer].extensions.pex.sent(&payload, self.now)?;
            }
        }
        while let Some(index) = self.remotes[peer].pending_have {
            if !self.send(peer, Message::Have(index))? {
                return Ok(());
            }
            let remote = &mut self.remotes[peer];
            set(&mut remote.announced, index);
            remote.pending_have = first_difference(&self.verified, &remote.announced, index + 1);
        }
        let desired = self.remotes[peer].first_needed.is_some();
        if self.remotes[peer].interested != desired {
            if !self.send(
                peer,
                if desired {
                    Message::Interested
                } else {
                    Message::NotInterested
                },
            )? {
                return Ok(());
            }

            self.remotes[peer].interested = desired;
        }
        self.pump_upload_control(peer)
    }
    pub(super) fn send(&mut self, peer: usize, message: Message<'_>) -> io::Result<bool> {
        if self.remotes[peer].retiring || self.remotes[peer].output_capacity == 0 {
            self.remotes[peer].output_blocked = true;
            return Ok(false);
        }
        let permit = if let Message::Request(request) = message {
            match self
                .bandwidth
                .take(self.now, bandwidth::Direction::Down, request.length)
            {
                Ok(permit) => Some(permit),
                Err(at) => {
                    self.rate_deadline = Some(self.rate_deadline.map_or(at, |old| old.min(at)));
                    return Ok(false);
                }
            }
        } else {
            None
        };
        let frame = Frame::from_message(message, crate::profile::MAX_FRAME_LENGTH)?;
        let sent = self.queue_frame(peer, frame)?;
        if sent && let Some(permit) = permit {
            permit.commit();
        }
        Ok(sent)
    }
    fn queue_frame(&mut self, peer: usize, msg: Frame) -> io::Result<bool> {
        let remote = &mut self.remotes[peer];
        if remote.retiring || remote.output_capacity == 0 {
            remote.output_blocked = true;
            return Ok(false);
        }
        remote.output_capacity -= 1;
        remote.output_blocked = remote.output_capacity == 0;
        self.actions.push(Action::Send {
            id: remote.connection,
            msg,
        });
        Ok(true)
    }
    pub(super) fn ignored_block(
        &mut self,
        position: usize,
        wire_bytes: usize,
        index: u32,
        begin: u32,
    ) -> io::Result<Option<Event>> {
        if let Err(error) = self.remotes[position].unsolicited.charge(wire_bytes) {
            self.retire_peer(position, error)?;
            return Ok(None);
        }

        Ok(Some(Event::IgnoredBlock { index, begin }))
    }
}
