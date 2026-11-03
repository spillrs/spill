use super::*;
use crate::protocol::merkle;

pub(super) struct Job {
    pub(super) request: HashRequest,
    pub(super) work: Work,
}
pub(super) enum Work {
    Stored {
        file: usize,
    },
    Reply {
        hashes: Option<Vec<u8>>,
        charge: u32,
    },
    Leaves {
        file: usize,
        piece: u32,
        bytes: u32,
        started: bool,
    },
}

pub(super) fn response(
    meta: &OwnedMetainfo,
    file: usize,
    request: HashRequest,
    leaves: &[[u8; 32]],
) -> Vec<u8> {
    let tree = meta.piece_tree(file);
    let file = &meta.layout().files()[file];
    let piece_length = u64::from(meta.layout().piece_length());
    let first = (file.offset() / piece_length) as usize;
    let count = file.length().div_ceil(piece_length) as usize;
    let height = file
        .length()
        .min(piece_length)
        .div_ceil(merkle::BLOCK_LENGTH as u64)
        .next_power_of_two()
        .ilog2();
    request.response(
        &meta.view().v2_hashes()[first..first + count],
        height,
        tree,
        leaves,
    )
}

impl Torrent {
    fn hash_work(&self, peer: usize, request: HashRequest) -> Option<Work> {
        let files = self.hash_files.get(&request.pieces_root)?;
        let file = &self.meta.layout().files()[files[0]];
        let piece_length = u64::from(self.meta.layout().piece_length());
        let blocks = file
            .length()
            .min(piece_length)
            .div_ceil(merkle::BLOCK_LENGTH as u64)
            .next_power_of_two();
        let height = blocks.ilog2();
        if request.validate(file.length(), 512).is_err()
            || (request.base_layer != 0 && request.base_layer != height)
        {
            return None;
        }
        if request.base_layer == height {
            return Some(Work::Stored { file: files[0] });
        }
        let relative = u64::from(request.index) / blocks;
        if relative != (u64::from(request.index) + u64::from(request.length) - 1) / blocks {
            return None;
        }
        if relative >= file.length().div_ceil(piece_length) {
            return Some(Work::Stored { file: files[0] });
        }
        // Admission is fixed here; a later choke must not revoke an owed proof.
        if !self.remotes[peer].upload_granted {
            return None;
        }
        let (file, piece) = files.iter().find_map(|&file| {
            let piece =
                (self.meta.layout().files()[file].offset() / piece_length + relative) as u32;
            has(&self.verified, piece).then_some((file, piece))
        })?;
        Some(Work::Leaves {
            file,
            piece,
            bytes: self
                .meta
                .layout()
                .piece_data_end(piece)
                .expect("leaf piece data"),
            started: false,
        })
    }
    pub(super) fn request_hashes(
        &mut self,
        peer: usize,
        request: HashRequest,
        wire_bytes: usize,
    ) -> io::Result<()> {
        if self.remotes[peer].hashes.len()
            + self.remotes[peer].uploads.len()
            + self.remotes[peer].rejects.len()
            >= crate::engine::profile::MAX_UPLOAD_REQUESTS
        {
            return self.retire_peer(peer, invalid_data_text("hash request limit exceeded"));
        }
        let work = match self.hash_work(peer, request) {
            Some(work) => work,
            None => {
                if let Err(error) = self.remotes[peer].unsolicited.charge(wire_bytes) {
                    return self.retire_peer(peer, error);
                }
                Work::Reply {
                    hashes: None,
                    charge: 0,
                }
            }
        };
        self.remotes[peer].hashes.push_back(Job { request, work });
        Ok(())
    }
    pub(super) fn pump_hashes(&mut self) -> io::Result<()> {
        for peer in 0..self.remotes.len() {
            if self.remotes[peer].retiring {
                continue;
            }
            while let Some(job) = self.remotes[peer].hashes.front() {
                let request = job.request;
                if let Work::Stored { file } = job.work {
                    let hashes = response(&self.meta, file, request, &[]);
                    self.remotes[peer]
                        .hashes
                        .front_mut()
                        .expect("hash job")
                        .work = Work::Reply {
                        charge: hashes.len() as u32,
                        hashes: Some(hashes),
                    };
                    continue;
                }
                if let Work::Reply { hashes, charge } = &job.work {
                    if self.remotes[peer].output_capacity == 0 {
                        break;
                    }
                    // Read debt must not delay an already-paid proof or a reject.
                    let permit = if *charge == 0 {
                        None
                    } else {
                        match self
                            .bandwidth
                            .take(self.now, bandwidth::Direction::Up, *charge)
                        {
                            Ok(permit) => Some(permit),
                            Err(at) => {
                                self.rate_deadline =
                                    Some(self.rate_deadline.map_or(at, |old| old.min(at)));
                                break;
                            }
                        }
                    };
                    let message = match hashes {
                        Some(hashes) => Message::Hashes { request, hashes },
                        None => Message::HashReject(request),
                    };
                    let frame =
                        Frame::from_message(message, crate::engine::profile::MAX_FRAME_LENGTH)?;
                    if !self.queue_frame(peer, frame)? {
                        break;
                    }
                    if let Some(permit) = permit {
                        permit.commit();
                    }
                    self.remotes[peer].hashes.pop_front();
                    continue;
                }
                let Work::Leaves {
                    piece,
                    bytes,
                    started,
                    ..
                } = &job.work
                else {
                    unreachable!("leaf work")
                };
                let piece = *piece;
                if *started
                    || self.write_capacity == 0
                    || self.upload_reads_in_flight() >= crate::engine::profile::MAX_UPLOAD_READS
                {
                    break;
                }
                // Pay for the entire proof read before I/O, not its small wire reply.
                let permit = match self
                    .bandwidth
                    .take(self.now, bandwidth::Direction::Up, *bytes)
                {
                    Ok(permit) => permit,
                    Err(at) => {
                        self.rate_deadline = Some(self.rate_deadline.map_or(at, |old| old.min(at)));
                        break;
                    }
                };
                self.write_capacity -= 1;
                self.leaf_reads += 1;
                let Work::Leaves { started, .. } = &mut self.remotes[peer]
                    .hashes
                    .front_mut()
                    .expect("hash job")
                    .work
                else {
                    unreachable!("leaf work")
                };
                *started = true;
                self.actions.push(Action::ReadLeaves {
                    id: self.remotes[peer].connection,
                    request,
                    piece,
                });
                permit.commit();
                break;
            }
        }
        Ok(())
    }
}
