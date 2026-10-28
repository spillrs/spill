use super::*;

impl Torrent {
    pub(super) fn on_attach(
        &mut self,
        id: ConnectionId,
        endpoint: Option<SocketAddr>,
        local_ip: Option<IpAddr>,
        inbound: bool,
        web_seed: bool,
    ) -> io::Result<()> {
        if let Some(addr) = endpoint {
            self.dialing.retain(|old| *old != addr);
        }
        if self.remote_slots.contains_key(&id) {
            return Err(invalid("duplicate connection ID"));
        }
        if self.remotes.len() >= self.limits.peers
            || endpoint.is_some_and(|a| self.ip_banned(a.ip()))
        {
            self.actions.push(Action::Disconnect { id });
            return Ok(());
        }
        let pieces = vec![0; self.verified.len()];
        let allowed_fast = vec![0; self.verified.len()];
        let announced = vec![0; self.verified.len()];
        let pending_have = first_difference(&self.verified, &announced, 0);
        let asked = requests::Asked::with_capacity(peer::MAX_REQUESTS);
        let uploads = requests::Uploads::with_capacity(crate::engine::profile::MAX_UPLOAD_REQUESTS);
        self.clear_wait_stall();
        self.remote_slots.insert(id, self.remotes.len());
        self.remotes.push(Remote {
            connection: id,
            peer_id: None,
            established: false,
            last_useful: self.now,
            have_count: 0,
            last_uploaded: 0,
            uninteresting_since: None,
            extensions: {
                let mut extensions = extensions::Extensions::default();
                extensions.pex.enabled = self.pex_enabled;
                extensions.pex.endpoint = endpoint;
                extensions.pex.local_ip = local_ip;
                extensions.pex.inbound = inbound;
                extensions
            },
            first_message: true,
            choked: true,
            fast: false,
            remote_dht: false,
            send_dht_port: false,
            dht_port_seen: false,
            received_dht_port: None,
            allowed_fast,
            interested: false,
            remote_interested: false,
            upload_choked: true,
            upload_granted: false,
            uploads,
            dirty: true,
            cancels: VecDeque::new(),
            rejects: VecDeque::new(),
            send_bitfield: false,
            output_blocked: false,
            retiring: false,
            retirement_reason: None,
            pieces,
            announced,
            first_needed: None,
            pending_have,
            asked,
            web_seed,
            download_rate: peer::DownloadRate::default(),
            uploaded: 0,
            output_capacity: 0,
            unsolicited: UnsolicitedBudget::new(),
        });
        Ok(())
    }
    // Called only during recovery, before any peer or scheduling work starts.
    pub(in crate::engine) fn select_files(
        &mut self,
        selection: crate::engine::selection::FileSelection,
    ) {
        assert!(self.remotes.is_empty() && self.picker.pieces().len() == 0);
        self.excluded.clone_from(&self.verified);
        self.picker.set_priorities(selection.piece_priorities());
        self.wanted_remaining = 0;
        for (index, priority) in selection.piece_priorities().iter().enumerate() {
            if *priority == crate::engine::selection::FilePriority::Skip {
                set(&mut self.excluded, index as u32);
            } else if !has(&self.verified, index as u32) {
                self.wanted_remaining += 1;
            }
        }
        self.picker
            .restore_missing(self.meta.layout().piece_count(), &self.excluded);
        self.wanted_verified_bytes = selection.verified_bytes(self.meta.layout(), &self.verified);
        self.selection = Some(selection);
    }
    pub(in crate::engine) fn wanted_complete(&self) -> bool {
        self.metadata.is_none() && self.wanted_remaining == 0
    }
    /// Synchronous bounded observation, valid while failed/stopping/stopped.
    /// Reads at most the admitted pieces' catalog entries; it does not poll
    /// workers, reconcile notifications, issue I/O or drive persistence/trackers.
    ///
    /// For tracker reporting, explicitly check info_hash, choose an independently
    /// observed uploaded total and construct AnnounceCounters from received payload
    /// and remaining bytes. The torrent task owns reporting; durable cross-run
    /// accounting remains in the daemon catalog.
    pub(in crate::engine) fn progress(&self) -> io::Result<Progress> {
        let meta = self
            .meta
            .0
            .as_ref()
            .ok_or_else(|| invalid("metadata unresolved"))?;
        Ok(Progress {
            total_bytes: meta.layout().total_length(),
            initial_verified_bytes: self.initial_verified_bytes,
            verified_bytes: self.verified_bytes,
            wanted_bytes: self
                .selection
                .as_ref()
                .map_or(meta.layout().total_length(), |s| s.wanted_bytes()),
            wanted_verified_bytes: self.wanted_verified_bytes,
            received_payload_bytes: self.received_payload_bytes,
        })
    }
    /// Observe scheduling state without polling/reconciling events or replenishing
    /// budgets. Bounded O(peers + outstanding requests), no allocation or locks.
    /// Available during failure and lifecycle barriers; it does not establish join.
    pub(in crate::engine) fn diagnostics(&self) -> Diagnostics {
        let established = |peer: &&Remote| peer.established && !peer.retiring;
        let assigned = self
            .picker
            .all_requests()
            .filter(|p| p.connection.is_some())
            .count();
        let observed_piece = self.picker.pieces().next();
        Diagnostics {
            peers: self.remotes.len(),
            established_peers: self.remotes.iter().filter(|p| p.established).count(),
            unchoked_peers: self
                .remotes
                .iter()
                .filter(established)
                .filter(|p| !p.choked)
                .count(),
            interested_peers: self
                .remotes
                .iter()
                .filter(established)
                .filter(|p| p.interested)
                .count(),
            remote_interested_peers: self
                .remotes
                .iter()
                .filter(established)
                .filter(|p| p.remote_interested)
                .count(),
            retiring_peers: self.remotes.iter().filter(|p| p.retiring).count(),
            output_blocked_peers: self.remotes.iter().filter(|p| p.output_blocked).count(),
            upload_unchoked_peers: self
                .remotes
                .iter()
                .filter(|p| p.established && !p.retiring && !p.upload_choked)
                .count(),
            pending_upload_requests: self.remotes.iter().map(|p| p.uploads.len()).sum(),
            outstanding_requests: self.picker.request_count(),
            assigned_requests: assigned,
            waiting_requests: self.picker.request_count() - assigned,
            requests_at_retry_limit: self
                .picker
                .all_requests()
                .filter(|p| p.reassignments == crate::engine::profile::MAX_REASSIGNMENTS)
                .count(),
            current_piece: observed_piece.map(|p| p.index),
            piece_retries_remaining: observed_piece
                .map(|p| crate::engine::profile::MAX_PIECE_RETRIES.saturating_sub(p.retries)),
            hashing: self.picker.pieces().any(|p| p.hashing),
            stalled: self.picker.stall(),
            failed: self.failed,
        }
    }
    pub(in crate::engine) fn has_peer(&self, connection: ConnectionId) -> bool {
        self.remotes
            .iter()
            .any(|peer| peer.connection == connection)
    }
    pub(in crate::engine) fn peer_client(&self, connection: ConnectionId) -> Option<&str> {
        self.remotes
            .iter()
            .find(|p| p.connection == connection)?
            .extensions
            .client
            .as_deref()
    }
    /// Allocation-free observations in admission order. Each item scans at most
    /// MAX_REQUESTS descriptors per peer; the full iterator is O(peers * requests).
    /// The iterator borrows the owner; returned records are independent snapshots.
    pub(in crate::engine) fn peer_diagnostics(
        &self,
    ) -> impl ExactSizeIterator<Item = PeerDiagnostics> + '_ {
        self.remotes.iter().map(|peer| {
            let remaining = peer.unsolicited.remaining();
            PeerDiagnostics {
                downloaded_bytes: peer.download_rate.total(),
                uploaded_bytes: Some(peer.uploaded),
                connection: peer.connection.0,
                established: peer.established,
                fast_extension: peer.fast,
                remote_choking: peer.choked,
                local_interested: peer.interested,
                remote_interested: peer.remote_interested,
                local_choking: peer.upload_choked,
                upload_slot: peer.upload_granted,
                pending_upload_requests: peer.uploads.len(),
                has_needed_pieces: peer.first_needed.is_some(),
                retiring: peer.retiring,
                output_blocked: peer.output_blocked,
                assigned_requests: self
                    .picker
                    .all_requests()
                    .filter(|p| p.connection == Some(peer.connection))
                    .count(),
                authorized_requests: peer.asked.len(),
                request_queue: peer.extensions.request_queue,
                ignored_frames_remaining: remaining.0,
                ignored_bytes_remaining: remaining.1,
            }
        })
    }
    pub(in crate::engine) fn ip_banned(&self, ip: IpAddr) -> bool {
        let ip = ip.to_canonical();
        self.banned
            .iter()
            .any(|(_, address, until)| *address == Some(ip) && *until > self.now)
    }
    pub(super) fn on_connect(
        &mut self,
        connection: ConnectionId,
        handshake: Handshake,
    ) -> io::Result<Option<Event>> {
        self.expire_requests()?;
        let Some(&position) = self.remote_slots.get(&connection) else {
            return Ok(None);
        };
        if self.remotes[position].retiring {
            return Ok(None);
        }
        self.remotes[position].dirty = true;
        let Handshake {
            reserved, peer_id, ..
        } = handshake;
        self.banned.retain(|(_, _, until)| *until > self.now);
        if !self.remotes[position].web_seed
            && (self.banned.iter().any(|(id, _, _)| *id == peer_id)
                || self.remotes[position]
                    .extensions
                    .pex
                    .endpoint
                    .is_some_and(|a| self.ip_banned(a.ip())))
        {
            self.retire_peer(position, invalid_data_text("banned corrupt peer"))?;
            return Ok(None);
        }
        if !self.remotes[position].web_seed
            && let Some(other) = self.remotes.iter().position(|p| {
                p.connection != connection
                    && !p.web_seed
                    && !p.retiring
                    && p.peer_id == Some(peer_id)
            })
        {
            // Both endpoints choose the same TCP direction during a cross-dial.
            let prefer_inbound = self.own_peer_id.expect("local peer identity") > peer_id;
            let keep_new = self.remotes[position].extensions.pex.inbound == prefer_inbound
                && self.remotes[other].extensions.pex.inbound != prefer_inbound;
            if !keep_new {
                self.retire_peer(position, invalid_data_text("duplicate peer identity"))?;
                return Ok(None);
            }
            self.retire_peer(other, invalid_data_text("duplicate peer identity"))?;
        }
        self.remotes[position].peer_id = Some(peer_id);
        self.remotes[position].fast = reserved[7] & 0x04 != 0;
        self.remotes[position].remote_dht = reserved[7] & 0x01 != 0;
        self.remotes[position].send_dht_port =
            self.dht_port.is_some() && self.remotes[position].remote_dht;
        let enabled = reserved[5] & 0x10 != 0;
        self.remotes[position].extensions.enabled = enabled;
        self.remotes[position].extensions.handshake_pending = enabled;
        self.remotes[position].established = true;
        self.choker_dirty = true;
        self.remotes[position].send_bitfield = true;
        Ok(Some(Event::PeerReady(connection)))
    }
    pub(super) fn on_close(&mut self, connection: ConnectionId) -> io::Result<Option<Event>> {
        let Some(&position) = self.remote_slots.get(&connection) else {
            return Ok(Some(Event::PeerClosed {
                connection,
                result: Ok(()),
            }));
        };
        let result: io::Result<()> = Ok(());
        let departed = self.remotes.remove(position);
        self.remote_slots.remove(&connection);
        for (slot, remote) in self.remotes.iter().enumerate().skip(position) {
            self.remote_slots.insert(remote.connection, slot);
        }
        self.mark_all_dirty();
        self.choker_dirty = true;
        self.picker.availability_changed(&departed.pieces, &[]);
        let result = departed.retirement_reason.map_or(result, Err);
        let isolated_pieces: Vec<_> = self
            .picker
            .pieces()
            .filter(|piece| piece.isolated && piece.source == Some(connection))
            .map(|piece| piece.index)
            .collect();
        self.peer_cursor = if self.remotes.is_empty() {
            0
        } else {
            self.peer_cursor % self.remotes.len()
        };
        let waiting_deadline = self.now + crate::engine::profile::REQUEST_TIMEOUT;
        self.picker
            .on_peer_gone(connection, waiting_deadline, false);
        for index in isolated_pieces {
            if self.picker.ready_to_hash(index, self.meta.layout()) {
                self.maybe_hash()?;
                continue;
            }
            if self.picker.piece(index).is_some_and(|piece| !piece.hashing) {
                if self.picker.used_peer(index, connection)
                    || departed.asked.iter().any(|request| request.index == index)
                {
                    // Never mix partial isolated bytes with a replacement
                    // source. A source loss spends a whole-attempt budget.
                    self.picker.clear_requests(index);
                    let event = self.restart_piece(index, false)?;
                    self.deferred.push_back(event);
                } else {
                    self.picker.release_source(index);
                }
            }
        }
        Ok(Some(Event::PeerClosed { connection, result }))
    }
}
