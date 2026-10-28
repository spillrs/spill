use super::*;

impl Torrent {
    pub(super) fn set_stall(&mut self, reason: Stall) {
        self.picker.set_stall(reason);
    }
    pub(super) fn clear_wait_stall(&mut self) {
        self.picker.clear_wait_stall();
    }
    pub(crate) fn retry_status(&self) -> crate::torrent::TorrentRetryStatus {
        use crate::torrent::TorrentRetryStatus;
        if let Some(piece) = self
            .picker
            .pieces()
            .filter(|p| p.retries > 0 && p.retry_deadline.is_some_and(|due| due > self.now))
            .min_by_key(|p| p.retry_deadline)
        {
            TorrentRetryStatus::Backoff {
                attempt: piece.retries,
                retry_at: piece.retry_deadline.expect("retry deadline"),
            }
        } else if let Some(piece) = self
            .picker
            .pieces()
            .filter(|p| p.retries > 0)
            .max_by_key(|p| p.retries)
        {
            TorrentRetryStatus::Retrying {
                attempt: piece.retries,
            }
        } else {
            TorrentRetryStatus::Idle
        }
    }
    pub(super) fn turn_over_peers(&mut self) -> io::Result<()> {
        let now = self.now;
        if now < self.turnover_due {
            return Ok(());
        }
        self.turnover_due = now + Duration::from_secs(1);
        self.mark_all_dirty();
        let complete = self.verified_count == self.meta.layout().piece_count();
        for position in 0..self.remotes.len() {
            let peer = &mut self.remotes[position];
            if !peer.established || peer.retiring || peer.web_seed {
                continue;
            }
            let uploaded = peer.uploaded;
            if uploaded > peer.last_uploaded {
                peer.last_useful = now;
                peer.last_uploaded = uploaded;
            }
            let seed = complete && peer.have_count == self.meta.layout().piece_count();
            if peer.first_needed.is_some() || peer.remote_interested {
                peer.uninteresting_since = None;
            } else {
                peer.uninteresting_since.get_or_insert(now);
            }
            let uninteresting = peer
                .uninteresting_since
                .is_some_and(|at| now.duration_since(at) >= Duration::from_secs(60));
            let waiting_for_upload = peer.remote_interested
                && peer.upload_choked
                && if complete {
                    peer.have_count < self.meta.layout().piece_count()
                } else {
                    self.verified
                        .iter()
                        .zip(&peer.pieces)
                        .any(|(ours, theirs)| ours & !theirs != 0)
                };
            if seed
                || uninteresting
                || (!waiting_for_upload
                    && now.duration_since(peer.last_useful) >= Duration::from_secs(180))
            {
                self.retire_peer(
                    position,
                    io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        if seed {
                            "seed-to-seed connection"
                        } else {
                            "unproductive peer"
                        },
                    ),
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn expire_requests(&mut self) -> io::Result<()> {
        while let Some(connection) = self.picker.tick(self.now) {
            let position = *self
                .remote_slots
                .get(&connection)
                .ok_or_else(|| io::Error::other("request owner missing"))?;
            self.retire_peer(position, request_timeout())?;
        }
        Ok(())
    }
    pub(super) fn schedule_requests(&mut self) -> io::Result<()> {
        if self.write_capacity == 0 {
            return Ok(());
        }
        self.maybe_hash()?;
        let count = self.remotes.len();
        for step in 0..count {
            let peer = (self.peer_cursor + step) % count;
            let remote = &self.remotes[peer];
            if !remote.dirty || !ready(remote) || remote.output_blocked {
                continue;
            }
            let connection = remote.connection;
            let window = if remote.web_seed {
                crate::profile::MAX_REQUEST_QUEUE.min(crate::core::peer::MAX_REQUESTS)
            } else {
                remote
                    .download_rate
                    .window(crate::profile::BLOCK_LEN, remote.extensions.request_queue)
            };
            if self.remotes[peer].asked.len() >= window {
                continue;
            }
            // Reuse the bounded candidate buffer for this peer's whole window.
            // Only new admission/affinity changes require reordering it.
            self.request_candidates.clear();
            self.request_candidates.extend(
                self.picker
                    .pieces()
                    .filter(|p| {
                        !p.hashing
                            && eligible(&self.remotes[peer], p.index)
                            && self.picker.accepts_peer(p.index, connection)
                    })
                    .map(|p| (p.source != Some(connection), p.index)),
            );
            self.request_candidates.sort_unstable();
            while self.remotes[peer].asked.len() < window {
                let request = self
                    .request_candidates
                    .iter()
                    .copied()
                    .find_map(|(_, index)| {
                        self.picker
                            .next_request(index, self.meta.layout(), self.now)
                    });
                let request = if let Some(request) = request {
                    request
                } else {
                    let remote = &self.remotes[peer];
                    let available = if remote.choked {
                        self.request_available.clone_from(&remote.pieces);
                        for (bits, allowed) in
                            self.request_available.iter_mut().zip(&remote.allowed_fast)
                        {
                            *bits &= allowed;
                        }
                        self.request_available.as_slice()
                    } else {
                        remote.pieces.as_slice()
                    };
                    let Some(index) = self.picker.select_piece([available], &self.verified) else {
                        break;
                    };
                    self.picker.start(index)?;
                    if self.remotes[peer].web_seed {
                        self.picker.pin(index, connection);
                    } else {
                        self.picker.prefer(index, connection);
                    }
                    self.request_candidates.push((false, index));
                    self.request_candidates.sort_unstable();
                    // Padding-only pieces go directly to read-back verification.
                    let Some(request) =
                        self.picker
                            .next_request(index, self.meta.layout(), self.now)
                    else {
                        self.maybe_hash()?;
                        continue;
                    };
                    request
                };
                let deadline = self.now + crate::profile::REQUEST_TIMEOUT;
                if !self.send(peer, Message::Request(request))? {
                    break;
                }
                let remote = &mut self.remotes[peer];
                if !remote.asked.contains(&request) {
                    remote.asked.push(request);
                }
                remote.download_rate.start(self.now);

                self.picker.admitted(request, connection, deadline);
                let affinity =
                    self.picker.piece(request.index).expect("piece").source != Some(connection);
                if let Some(candidate) = self
                    .request_candidates
                    .iter_mut()
                    .find(|p| p.1 == request.index)
                    && candidate.0 != affinity
                {
                    candidate.0 = affinity;
                    self.request_candidates.sort_unstable();
                }
            }
        }
        if count > 0 {
            self.peer_cursor = (self.peer_cursor + 1) % count;
        }
        self.schedule_endgame()?;
        self.maybe_hash()?;
        Ok(())
    }
    pub(super) fn schedule_endgame(&mut self) -> io::Result<()> {
        // Stalled duplicates must not block unrelated endgame requests.
        if !self.picker.endgame() {
            return Ok(());
        }
        for peer in 0..self.remotes.len() {
            let remote = &self.remotes[peer];
            if !ready(remote) || remote.web_seed || remote.output_blocked {
                continue;
            }
            let window = remote
                .download_rate
                .window(crate::profile::BLOCK_LEN, remote.extensions.request_queue);
            while self.remotes[peer].asked.len() < window {
                let remote = &self.remotes[peer];
                let request = self
                    .picker
                    .all_requests()
                    .find(|p| {
                        p.connection.is_some_and(|c| c != remote.connection)
                            && eligible(remote, p.request.index)
                            && !self.picker.piece(p.request.index).expect("piece").isolated
                            && !remote.asked.contains(&p.request)
                            && self
                                .remotes
                                .iter()
                                .filter(|r| r.asked.contains(&p.request))
                                .count()
                                < 2
                    })
                    .map(|p| p.request);
                let Some(request) = request else { break };
                if !self.send(peer, Message::Request(request))? {
                    break;
                }
                self.remotes[peer].asked.push(request);
            }
        }
        Ok(())
    }
}
