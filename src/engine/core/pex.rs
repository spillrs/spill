//! BEP 11 per-connection state. Untrusted additions are bounded; dropped hints
//! never disconnect peers. Output reports connection deltas at most once/minute.
use crate::engine::core::extensions::{invalid, limits};
use crate::engine::network::SocketAddr;
use crate::protocol::{
    bencode::{self, Node, Value},
    compact,
    peer::pex,
};
use std::{
    collections::BTreeMap,
    io,
    time::{Duration, Instant},
};

pub(in crate::engine) const ID: u8 = 2;
const INTERVAL: Duration = Duration::from_secs(60);
const UPDATE: usize = 50;
const INITIAL: usize = 200;

#[derive(Default)]
pub(in crate::engine) struct Pex {
    pub(in crate::engine) enabled: bool,
    pub(in crate::engine) endpoint: Option<SocketAddr>,
    pub(in crate::engine) observed: Option<SocketAddr>,
    pub(in crate::engine) local_ip: Option<crate::engine::network::IpAddr>,
    pub(in crate::engine) inbound: bool,
    pub(in crate::engine) received: Vec<SocketAddr>,
    pub(in crate::engine) introduced: Vec<(SocketAddr, u8)>,
    received_at: Option<Instant>,
    sent_at: Option<Instant>,
    advertised: Vec<SocketAddr>,
}
impl Pex {
    pub(in crate::engine) fn receive(
        &mut self,
        payload: &[u8],
        capacity: usize,
        now: Instant,
    ) -> io::Result<()> {
        let cap = if self.received_at.is_none() {
            INITIAL
        } else {
            UPDATE
        };
        // The bencode byte budget bounds parsing; peer counts only bound the
        // hints retained below, not whether a valid sender stays connected.
        let message =
            pex::Message::parse(payload, limits(), usize::MAX, usize::MAX).map_err(invalid)?;
        if self
            .received_at
            .is_some_and(|at| now.saturating_duration_since(at) < INTERVAL)
        {
            return Ok(());
        }
        self.received_at = Some(now);
        let dropped: Vec<_> = message
            .dropped_v4()
            .iter()
            .map(SocketAddr::V4)
            .chain(message.dropped_v6().iter().map(SocketAddr::V6))
            .map(crate::engine::network::canonical)
            .collect();
        self.introduced.retain(|(a, _)| !dropped.contains(a));
        let v4_flags = message.added_v4_flags().unwrap_or_default();
        let v6_flags = message.added_v6_flags().unwrap_or_default();
        let added = message
            .added_v4()
            .iter()
            .enumerate()
            .map(|(i, a)| (SocketAddr::V4(a), v4_flags.get(i).copied().unwrap_or(0)))
            .chain(
                message
                    .added_v6()
                    .iter()
                    .enumerate()
                    .map(|(i, a)| (SocketAddr::V6(a), v6_flags.get(i).copied().unwrap_or(0))),
            );
        for (address, flags) in added.take(cap) {
            let address = crate::engine::network::canonical(address);
            if !crate::engine::network::eligible(address) {
                continue;
            }
            if let Some((_, old_flags)) = self.introduced.iter_mut().find(|(a, _)| *a == address) {
                *old_flags = flags;
            } else if self.introduced.len() < INITIAL {
                self.introduced.push((address, flags));
            }
            if self.received.len() < capacity.min(INITIAL) && !self.received.contains(&address) {
                self.received.push(address);
            }
        }
        Ok(())
    }
    pub(in crate::engine) fn due(&self, now: Instant) -> bool {
        self.sent_at
            .is_none_or(|at| now.saturating_duration_since(at) >= INTERVAL)
    }
    pub(in crate::engine) fn update(
        &self,
        peers: &[(SocketAddr, u8)],
        now: Instant,
    ) -> io::Result<Option<Vec<u8>>> {
        if !self.due(now) {
            return Ok(None);
        }
        let added: Vec<_> = peers
            .iter()
            .copied()
            .filter(|(address, _)| !self.advertised.contains(address))
            .take(UPDATE)
            .collect();
        let dropped: Vec<_> = self
            .advertised
            .iter()
            .copied()
            .filter(|p| !peers.iter().any(|(address, _)| *address == *p))
            .take(UPDATE)
            .collect();
        if added.is_empty() && dropped.is_empty() {
            return Ok(None);
        }
        let mut fields = BTreeMap::new();
        let mut buffers = [
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ];
        for &(address, flags) in &added {
            match address {
                SocketAddr::V4(a) => {
                    buffers[0].extend_from_slice(&compact::encode_v4(a));
                    buffers[4].push(flags);
                }
                SocketAddr::V6(a) => {
                    buffers[1].extend_from_slice(&compact::encode_v6(a).map_err(invalid)?);
                    buffers[5].push(flags);
                }
            }
        }
        for address in &dropped {
            match address {
                SocketAddr::V4(a) => buffers[2].extend_from_slice(&compact::encode_v4(*a)),
                SocketAddr::V6(a) => {
                    buffers[3].extend_from_slice(&compact::encode_v6(*a).map_err(invalid)?)
                }
            }
        }
        for (key, bytes) in [
            b"added".as_slice(),
            b"added6",
            b"dropped",
            b"dropped6",
            b"added.f",
            b"added6.f",
        ]
        .into_iter()
        .zip(&buffers)
        {
            fields.insert(key, Node::new(Value::Bytes(bytes)));
        }
        let bytes =
            bencode::encode(&Node::new(Value::Dictionary(fields)), limits()).map_err(invalid)?;
        Ok(Some(bytes))
    }
    // Commit only after output admission, so backpressure never loses changes.
    pub(in crate::engine) fn sent(&mut self, payload: &[u8], now: Instant) -> io::Result<()> {
        let message = pex::Message::parse(payload, limits(), UPDATE, UPDATE).map_err(invalid)?;
        let dropped: Vec<_> = message
            .dropped_v4()
            .iter()
            .map(SocketAddr::V4)
            .chain(message.dropped_v6().iter().map(SocketAddr::V6))
            .collect();
        self.advertised.retain(|p| !dropped.contains(p));
        self.advertised.extend(
            message
                .added_v4()
                .iter()
                .map(SocketAddr::V4)
                .chain(message.added_v6().iter().map(SocketAddr::V6)),
        );
        self.sent_at = Some(now);
        Ok(())
    }
    pub(in crate::engine) fn disabled(&mut self) {
        self.advertised.clear();
        self.received.clear();
        self.introduced.clear();
    }
}
