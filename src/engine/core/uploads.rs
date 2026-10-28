use super::*;

impl Torrent {
    pub(super) fn pump_uploads(&mut self) -> io::Result<()> {
        // Canceled reads still need their acknowledgement; completed results
        // without demand must not retain payload memory.
        self.reads.retain(|read| {
            read.result.is_none()
                || self
                    .remotes
                    .iter()
                    .any(|p| p.uploads.contains(&read.request))
        });
        let count = self.remotes.len();
        if count == 0 {
            return Ok(());
        }
        let start = self.duplex.cursor % count;
        for step in 0..count {
            let peer = (start + step) % count;
            while self.remotes[peer].upload_granted
                && !self.remotes[peer].upload_choked
                && !self.remotes[peer].retiring
                && self.remotes[peer].output_capacity > 0
            {
                let Some(request) = self.remotes[peer].uploads.front().copied() else {
                    break;
                };
                if !has(&self.verified, request.index) {
                    self.remotes[peer].uploads.pop_front();
                    if self.remotes[peer].fast {
                        self.reject_upload(peer, request)?;
                    }
                    continue;
                }
                let Some(position) = self.reads.iter().position(|read| read.request == request)
                else {
                    if self.upload_reads_in_flight() < crate::engine::profile::MAX_UPLOAD_READS
                        && self.write_capacity > 0
                    {
                        self.write_capacity -= 1;
                        self.reads.push(Read {
                            request,
                            result: None,
                        });
                        self.actions.push(Action::Read { request });
                    }
                    break;
                };
                if self.reads[position].result.is_none() {
                    break;
                }
                // Other peers can reload this block from the upload cache;
                // retaining it behind their queue heads can deadlock admission.
                let result = self
                    .reads
                    .swap_remove(position)
                    .result
                    .expect("completed read");
                if let Some(block) = result {
                    self.remotes[peer].output_capacity -= 1;
                    self.remotes[peer].output_blocked = self.remotes[peer].output_capacity == 0;
                    self.actions.push(Action::SendPiece {
                        id: self.remotes[peer].connection,
                        request,
                        block,
                    });
                } else if self.remotes[peer].fast {
                    self.reject_upload(peer, request)?;
                }
                self.remotes[peer].uploads.pop_front();
            }
        }
        self.duplex.cursor = (start + 1) % count;
        Ok(())
    }
    pub(super) fn request_upload(
        &mut self,
        position: usize,
        request: BlockRequest,
        wire_bytes: usize,
    ) -> io::Result<()> {
        let peer = &mut self.remotes[position];
        peer.dirty = true;
        if !peer.upload_granted
            || peer.upload_choked
            || !peer.remote_interested
            || !has(&self.verified, request.index)
        {
            return if peer.fast {
                self.reject_upload(position, request)
            } else {
                self.ignored_upload(position, wire_bytes)
            };
        }
        if peer.uploads.contains(&request) {
            return self.ignored_upload(position, wire_bytes);
        }
        if peer.uploads.len() + peer.rejects.len() >= crate::engine::profile::MAX_UPLOAD_REQUESTS {
            return self.retire_peer(position, invalid_data_text("upload request limit exceeded"));
        }
        peer.uploads.push_back(request);
        Ok(())
    }
    pub(super) fn cancel_upload(
        &mut self,
        position: usize,
        request: BlockRequest,
        wire_bytes: usize,
    ) -> io::Result<()> {
        if self.remotes[position].uploads.remove_request(&request) {
            if self.remotes[position].fast {
                self.reject_upload(position, request)?;
            }
            Ok(())
        } else {
            self.ignored_upload(position, wire_bytes)
        }
    }
    pub(super) fn remote_interest(&mut self, position: usize, interested: bool) {
        let peer = &mut self.remotes[position];
        peer.dirty = true;
        if peer.remote_interested != interested {
            self.choker_dirty = true;
        }
        peer.remote_interested = interested;
        if !interested {
            if peer.fast {
                peer.rejects.extend(peer.uploads.drain(..));
            } else {
                peer.uploads.clear();
            }
        }
    }
    pub(super) fn reject_upload(
        &mut self,
        position: usize,
        request: BlockRequest,
    ) -> io::Result<()> {
        let peer = &mut self.remotes[position];
        if peer.uploads.len() + peer.rejects.len() >= crate::engine::profile::MAX_UPLOAD_REQUESTS {
            return self.retire_peer(
                position,
                invalid_data_text("Fast reject queue limit exceeded"),
            );
        }
        peer.rejects.push_back(request);
        Ok(())
    }
    fn ignored_upload(&mut self, position: usize, wire_bytes: usize) -> io::Result<()> {
        if let Err(error) = self.remotes[position].unsolicited.charge(wire_bytes) {
            self.retire_peer(position, error)?;
        }
        Ok(())
    }
    pub(super) fn pump_upload_control(&mut self, peer: usize) -> io::Result<()> {
        while let Some(request) = self.remotes[peer].rejects.front().copied() {
            if !self.send(peer, Message::Reject(request))? {
                return Ok(());
            }
            self.remotes[peer].rejects.pop_front();
        }
        if self.remotes[peer].upload_choked == self.remotes[peer].upload_granted {
            let unchoke = self.remotes[peer].upload_granted;
            if self.send(
                peer,
                if unchoke {
                    Message::Unchoke
                } else {
                    Message::Choke
                },
            )? {
                self.remotes[peer].upload_choked = !unchoke;
            }
        }
        Ok(())
    }
}
