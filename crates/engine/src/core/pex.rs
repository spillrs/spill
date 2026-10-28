//! BEP 11 per-connection state. Untrusted additions are bounded; dropped hints
//! never disconnect peers. Output reports connection deltas at most once/minute.
use crate::core::extensions::{invalid, limits};
use crate::network::SocketAddr;
use protocol::{
    bencode::{self, Node, Value},
    compact,
    peer::pex,
};
use std::{
    collections::BTreeMap,
    io,
    time::{Duration, Instant},
};

pub(crate) const ID: u8 = 2;
const INTERVAL: Duration = Duration::from_secs(60);
const UPDATE: usize = 50;
const INITIAL: usize = 200;

#[derive(Default)]
pub(crate) struct Pex {
    pub(crate) enabled: bool,
    pub(crate) endpoint: Option<SocketAddr>,
    pub(crate) local_ip: Option<crate::network::IpAddr>,
    pub(crate) inbound: bool,
    pub(crate) received: Vec<SocketAddr>,
    received_at: Option<Instant>,
    sent_at: Option<Instant>,
    advertised: Vec<SocketAddr>,
}
impl Pex {
    pub(crate) fn receive(
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
        for address in message
            .added_v4()
            .iter()
            .map(SocketAddr::V4)
            .chain(message.added_v6().iter().map(SocketAddr::V6))
            .take(cap)
        {
            if self.received.len() >= capacity.min(INITIAL) {
                break;
            }
            if crate::network::eligible(address) && !self.received.contains(&address) {
                self.received.push(address);
            }
        }
        Ok(())
    }
    pub(crate) fn due(&self, now: Instant) -> bool {
        self.sent_at
            .is_none_or(|at| now.saturating_duration_since(at) >= INTERVAL)
    }
    pub(crate) fn update(&self, peers: &[SocketAddr], now: Instant) -> io::Result<Option<Vec<u8>>> {
        if !self.due(now) {
            return Ok(None);
        }
        let added: Vec<_> = peers
            .iter()
            .copied()
            .filter(|p| !self.advertised.contains(p))
            .take(UPDATE)
            .collect();
        let dropped: Vec<_> = self
            .advertised
            .iter()
            .copied()
            .filter(|p| !peers.contains(p))
            .take(UPDATE)
            .collect();
        if added.is_empty() && dropped.is_empty() {
            return Ok(None);
        }
        let mut fields = BTreeMap::new();
        let mut buffers = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for (list, v4, v6) in [(&added, 0, 1), (&dropped, 2, 3)] {
            for address in list {
                match address {
                    SocketAddr::V4(a) => buffers[v4].extend_from_slice(&compact::encode_v4(*a)),
                    SocketAddr::V6(a) => {
                        buffers[v6].extend_from_slice(&compact::encode_v6(*a).map_err(invalid)?)
                    }
                }
            }
        }
        for (key, bytes) in [b"added".as_slice(), b"added6", b"dropped", b"dropped6"]
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
    pub(crate) fn sent(&mut self, payload: &[u8], now: Instant) -> io::Result<()> {
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
    pub(crate) fn disabled(&mut self) {
        self.advertised.clear();
        self.received.clear();
    }
}
