//! BEP 55 coordination. Payload and dial admission remain on the normal paths.
use super::*;
use crate::protocol::peer::holepunch::{self, Kind, Message};

pub(super) const ID: u8 = 3;
const QUEUED: usize = 8;
const PENDING: usize = 8;
const WINDOW: Duration = Duration::from_secs(10);

#[derive(Default)]
pub(super) struct Peer {
    pub(super) outbound: VecDeque<Message>,
    rendezvous: Option<(Instant, u8)>,
    connect: Option<(Instant, u8)>,
}
pub(super) struct Pending {
    target: SocketAddr,
    relay: ConnectionId,
    pub(super) deadline: Instant,
}
fn allowance(window: &mut Option<(Instant, u8)>, now: Instant) -> bool {
    let (start, used) = window.get_or_insert((now, 0));
    if now >= *start + WINDOW {
        *start = now;
        *used = 0;
    }
    if *used == 4 {
        return false;
    }
    *used += 1;
    true
}
impl Torrent {
    pub(super) fn holepunch_enabled(&self) -> bool {
        self.pex_enabled && self.utp_enabled && self.metadata.is_none()
    }
    fn punch_peer(&self, peer: usize) -> bool {
        let remote = &self.remotes[peer];
        remote.established
            && !remote.retiring
            && remote.extensions.enabled
            && remote.extensions.remote.id_for(b"ut_holepunch").is_some()
    }
    pub(super) fn request_holepunch(&mut self, target: SocketAddr) {
        self.expire_holepunches();
        if !self.holepunch_enabled()
            || !self.families[usize::from(target.is_ipv6())]
            || self.punches.len() == PENDING
            || self.punches.iter().any(|p| p.target == target)
        {
            return;
        }
        let relay =
            (0..self.remotes.len()).find(|&i| {
                let p = &self.remotes[i];
                self.punch_peer(i)
                    && p.extensions.pex.introduced.iter().any(|&(a, f)| {
                        a == target && f & crate::protocol::peer::pex::HOLEPUNCH != 0
                    })
                    && !self
                        .punches
                        .iter()
                        .any(|pending| pending.relay == p.connection)
                    && p.extensions.holepunch.outbound.len() < QUEUED
            });
        if let Some(relay) = relay {
            self.punches.push(Pending {
                target,
                relay: self.remotes[relay].connection,
                deadline: self.now + WINDOW,
            });
            self.remotes[relay]
                .extensions
                .holepunch
                .outbound
                .push_back(Message {
                    kind: Kind::Rendezvous,
                    addr: target,
                });
        }
    }
    pub(super) fn expire_holepunches(&mut self) {
        if !self.holepunch_enabled() {
            self.punches.clear();
            for p in &mut self.remotes {
                p.extensions.holepunch.outbound.clear();
            }
            return;
        }
        self.punches.retain(|pending| {
            pending.deadline > self.now
                && self.remotes.iter().any(|p| {
                    p.connection == pending.relay
                        && p.established
                        && !p.retiring
                        && p.extensions.remote.id_for(b"ut_holepunch").is_some()
                        && p.extensions.remote.id_for(b"ut_pex").is_some()
                })
                && !self.remotes.iter().any(|p| {
                    p.established
                        && !p.retiring
                        && (p.extensions.pex.endpoint == Some(pending.target)
                            || p.extensions.pex.observed == Some(pending.target))
                })
        });
        for p in &mut self.remotes {
            if p.retiring || p.extensions.remote.id_for(b"ut_holepunch").is_none() {
                p.extensions.holepunch.outbound.clear();
            } else {
                p.extensions.holepunch.outbound.retain(|message| {
                    message.kind != Kind::Rendezvous
                        || self.punches.iter().any(|pending| {
                            pending.relay == p.connection && pending.target == message.addr
                        })
                });
            }
        }
    }
    pub(super) fn on_holepunch(&mut self, peer: usize, payload: &[u8]) -> io::Result<()> {
        if !self.holepunch_enabled() || !self.punch_peer(peer) {
            return Ok(());
        }
        let message = Message::parse(payload).map_err(invalid_data)?;
        let target = crate::engine::network::canonical(message.addr);
        let relay = self.remotes[peer].connection;
        match message.kind {
            Kind::Error(_) => {
                self.punches
                    .retain(|p| p.relay != relay || p.target != target);
            }
            Kind::Connect => {
                if !allowance(
                    &mut self.remotes[peer].extensions.holepunch.connect,
                    self.now,
                ) {
                    return Ok(());
                }
                self.punches
                    .retain(|p| p.relay != relay || p.target != target);
                if self.families[usize::from(target.is_ipv6())]
                    && crate::engine::network::eligible(target)
                    && !self.ip_banned(target.ip())
                    && !self.connected_contact(target)
                {
                    self.actions.push(Action::Holepunch { addr: target });
                }
            }
            Kind::Rendezvous => {
                if !allowance(
                    &mut self.remotes[peer].extensions.holepunch.rendezvous,
                    self.now,
                ) {
                    return Ok(());
                }
                let matches = |p: &Remote| {
                    p.extensions.pex.endpoint == Some(target)
                        || p.extensions.pex.observed == Some(target)
                };
                let found = self
                    .remotes
                    .iter()
                    .position(|p| matches(p) && p.established && !p.retiring)
                    .or_else(|| self.remotes.iter().position(matches));
                let error = match found {
                    Some(i) if i == peer => Some(holepunch::NO_SELF),
                    None => Some(holepunch::NO_SUCH_PEER),
                    Some(i) if !self.remotes[i].established || self.remotes[i].retiring => {
                        Some(holepunch::NOT_CONNECTED)
                    }
                    Some(i) if !self.punch_peer(i) => Some(holepunch::NO_SUPPORT),
                    _ => None,
                };
                if let Some(code) = error {
                    let queue = &mut self.remotes[peer].extensions.holepunch.outbound;
                    if queue.len() < QUEUED {
                        queue.push_back(Message {
                            kind: Kind::Error(code),
                            addr: message.addr,
                        });
                    }
                    return Ok(());
                }
                let other = found.expect("connected target");
                let endpoint = |i: usize| {
                    let p = &self.remotes[i].extensions.pex;
                    p.observed
                        .or(p.endpoint)
                        .filter(|a| crate::engine::network::eligible(*a))
                };
                let (Some(requester), Some(destination)) = (endpoint(peer), endpoint(other)) else {
                    return Ok(());
                };
                if self.remotes[peer].extensions.holepunch.outbound.len() < QUEUED
                    && self.remotes[other].extensions.holepunch.outbound.len() < QUEUED
                {
                    self.remotes[peer]
                        .extensions
                        .holepunch
                        .outbound
                        .push_back(Message {
                            kind: Kind::Connect,
                            addr: destination,
                        });
                    self.remotes[other]
                        .extensions
                        .holepunch
                        .outbound
                        .push_back(Message {
                            kind: Kind::Connect,
                            addr: requester,
                        });
                }
            }
        }
        Ok(())
    }
}
