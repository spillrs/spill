//! Address votes are bounded, short-lived and independent of remote node IDs.
use crate::{engine::network, protocol::dht::NodeId};
use std::{
    collections::VecDeque,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio::time::Instant;

const VOTE_LIFETIME: Duration = Duration::from_secs(10 * 60);
const MAX_VOTES: usize = 32;
const MIN_VOTES: usize = 3;
const SWITCH_MARGIN: usize = 3;

pub(super) struct Identity {
    pub(super) id: NodeId,
    pub(super) address: Option<IpAddr>,
    votes: VecDeque<(IpAddr, IpAddr, Instant)>,
}
impl Identity {
    pub(super) fn new(seed: NodeId, address: Option<IpAddr>) -> Self {
        Self {
            id: address.map_or(seed, |address| seed.for_address(address)),
            address,
            votes: VecDeque::new(),
        }
    }

    pub(super) fn observe(
        &mut self,
        seed: NodeId,
        source: SocketAddr,
        observed: SocketAddr,
        now: Instant,
    ) -> bool {
        if source.is_ipv6() != observed.is_ipv6()
            || !network::source(source, false)
            || !network::source(observed, false)
            || !super::valid_address(observed, false)
        {
            return false;
        }
        self.votes
            .retain(|(_, _, at)| now.duration_since(*at) < VOTE_LIFETIME);
        // Ports and multiple addresses in one subnet must not multiply a vote.
        let voter = match source.ip() {
            IpAddr::V4(ip) => IpAddr::V4((u32::from(ip) & 0xffff_ff00).into()),
            IpAddr::V6(ip) => IpAddr::V6((u128::from(ip) & (u128::MAX << 64)).into()),
        };
        if self.votes.iter().any(|(source, _, _)| *source == voter) {
            return false;
        }
        if self.votes.len() == MAX_VOTES {
            self.votes.pop_front();
        }
        let address = observed.ip();
        self.votes.push_back((voter, address, now));
        let votes = self
            .votes
            .iter()
            .filter(|(_, ip, _)| *ip == address)
            .count();
        if self.address == Some(address) || votes < MIN_VOTES || votes * 2 <= self.votes.len() {
            return false;
        }
        if let Some(current) = self.address {
            let current_votes = self
                .votes
                .iter()
                .filter(|(_, ip, _)| *ip == current)
                .count();
            // Destination-dependent NAT addresses must not flip the ID on a narrow lead.
            if votes * 3 < self.votes.len() * 2 || votes < current_votes + SWITCH_MARGIN {
                return false;
            }
        }
        self.address = Some(address);
        self.id = seed.for_address(address);
        true
    }
}
