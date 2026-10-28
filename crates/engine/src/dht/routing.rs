//! BEP 5 k=8 prefix buckets. Only our bucket splits; unverified contacts are not served.
use protocol::dht::{NodeId, krpc::NodeV4};
use std::time::Duration;
use tokio::time::Instant;

pub(super) const K: usize = 8;
const GOOD_FOR: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum State {
    Good,
    Questionable,
    Bad,
}
struct Contact {
    node: NodeV4,
    reply: Option<Instant>,
    query: Instant,
    failures: u8,
}
impl Contact {
    fn state(&self, now: Instant) -> State {
        if self.failures >= 2 {
            State::Bad
        } else if self.failures == 0
            && self
                .reply
                .is_some_and(|reply| now.duration_since(reply.max(self.query)) < GOOD_FOR)
        {
            State::Good
        } else {
            State::Questionable
        }
    }
}
struct Bucket {
    prefix: NodeId,
    depth: usize,
    contacts: Vec<Contact>,
    touched: Instant,
}
pub(super) struct Routing {
    own: NodeId,
    max_buckets: usize,
    buckets: Vec<Bucket>,
}
fn bit(id: NodeId, index: usize) -> bool {
    id.0[index / 8] & (0x80 >> (index % 8)) != 0
}
fn contains(bucket: &Bucket, id: NodeId) -> bool {
    (0..bucket.depth).all(|i| bit(bucket.prefix, i) == bit(id, i))
}
pub(super) fn distance(a: NodeId, b: NodeId) -> [u8; 20] {
    std::array::from_fn(|i| a.0[i] ^ b.0[i])
}
impl Routing {
    pub(crate) fn new(own: NodeId, max_buckets: usize) -> Self {
        Self {
            own,
            max_buckets,
            buckets: vec![Bucket {
                prefix: NodeId([0; 20]),
                depth: 0,
                contacts: vec![],
                touched: Instant::now(),
            }],
        }
    }
    pub(crate) fn touch(&mut self, target: NodeId, now: Instant) {
        if let Some(bucket) = self.buckets.iter_mut().find(|b| contains(b, target)) {
            bucket.touched = now;
        }
    }
    /// Refresh at most two stale buckets per recovery cycle. Preserve the bucket
    /// prefix and randomize the suffix to explore its full keyspace.
    pub(crate) fn refresh_targets(&mut self, now: Instant) -> std::io::Result<Vec<NodeId>> {
        let mut stale: Vec<_> = self
            .buckets
            .iter()
            .enumerate()
            .filter(|(_, b)| now.duration_since(b.touched) >= GOOD_FOR)
            .map(|(index, b)| (b.touched, index))
            .collect();
        stale.sort_unstable();
        let mut targets = Vec::new();
        for (_, index) in stale.into_iter().take(2) {
            let bucket = &mut self.buckets[index];
            let mut target = NodeId(super::entropy()?);
            for bit_index in 0..bucket.depth {
                let mask = 0x80 >> (bit_index % 8);
                target.0[bit_index / 8] =
                    (target.0[bit_index / 8] & !mask) | (bucket.prefix.0[bit_index / 8] & mask);
            }
            bucket.touched = now;
            targets.push(target);
        }
        Ok(targets)
    }
    pub(crate) fn heard(&mut self, node: NodeV4, replied: bool, now: Instant) {
        if node.id == self.own || node.address.port() == 0 {
            return;
        }
        // One endpoint per node ID; an unsolicited query cannot overwrite a verified endpoint.
        if let Some(contact) = self
            .buckets
            .iter_mut()
            .flat_map(|b| &mut b.contacts)
            .find(|c| c.node.id == node.id)
        {
            if contact.node.address != node.address {
                return;
            }
            if replied {
                contact.reply = Some(now);
                contact.failures = 0;
            } else {
                contact.query = now;
            }
            return;
        }
        for bucket in &mut self.buckets {
            if let Some(index) = bucket
                .contacts
                .iter()
                .position(|c| c.node.address == node.address)
            {
                if !replied {
                    return;
                }
                // A restarted remote can change its ID, but only a matched reply
                // authorizes moving its endpoint to the new ID's bucket.
                bucket.contacts.remove(index);
                break;
            }
        }
        loop {
            let index = self
                .buckets
                .iter()
                .position(|b| contains(b, node.id))
                .expect("partition");
            let bucket = &mut self.buckets[index];
            bucket.contacts.retain(|c| c.state(now) != State::Bad);
            if bucket.contacts.len() < K {
                bucket.contacts.push(Contact {
                    node,
                    reply: replied.then_some(now),
                    query: now,
                    failures: 0,
                });
                return;
            }
            if bucket.depth == 160
                || !contains(bucket, self.own)
                || self.buckets.len() == self.max_buckets
            {
                return;
            }
            let bucket = &mut self.buckets[index];
            let depth = bucket.depth;
            bucket.depth += 1;
            let mut sibling = Bucket {
                prefix: bucket.prefix,
                depth: depth + 1,
                contacts: vec![],
                touched: bucket.touched,
            };
            sibling.prefix.0[depth / 8] |= 0x80 >> (depth % 8);
            let mut keep = Vec::new();
            for contact in bucket.contacts.drain(..) {
                if bit(contact.node.id, depth) {
                    sibling.contacts.push(contact);
                } else {
                    keep.push(contact);
                }
            }
            bucket.contacts = keep;
            self.buckets.push(sibling);
        }
    }
    pub(crate) fn failed(&mut self, address: std::net::SocketAddrV4) {
        if let Some(c) = self
            .buckets
            .iter_mut()
            .flat_map(|b| &mut b.contacts)
            .find(|c| c.node.address == address)
        {
            c.failures = c.failures.saturating_add(1);
        }
    }
    pub(crate) fn closest(&self, target: NodeId, now: Instant) -> Vec<NodeV4> {
        let mut contacts: Vec<_> = self
            .buckets
            .iter()
            .flat_map(|b| &b.contacts)
            .filter(|c| c.state(now) == State::Good)
            .map(|c| c.node)
            .collect();
        contacts.sort_unstable_by_key(|c| distance(c.id, target));
        contacts.truncate(K);
        contacts
    }
    /// A lookup itself verifies liveness; previously verified questionable
    /// contacts remain usable at the 15-minute re-announce boundary.
    pub(crate) fn lookup_candidates(&self, target: NodeId, now: Instant) -> Vec<NodeV4> {
        let mut contacts: Vec<_> = self
            .buckets
            .iter()
            .flat_map(|b| &b.contacts)
            .filter(|c| c.reply.is_some() && c.state(now) != State::Bad)
            .map(|c| c.node)
            .collect();
        contacts.sort_unstable_by_key(|c| distance(c.id, target));
        contacts.truncate(super::MAX_SAVED_NODES);
        contacts
    }
    pub(crate) fn questionable(&self, now: Instant) -> Vec<NodeV4> {
        let mut contacts: Vec<_> = self
            .buckets
            .iter()
            .flat_map(|b| &b.contacts)
            .filter(|c| c.state(now) == State::Questionable)
            .collect();
        // A bounded maintenance slice must eventually reach every stale node.
        contacts.sort_unstable_by_key(|c| c.reply);
        contacts.into_iter().map(|c| c.node).collect()
    }
    pub(crate) fn remove_addresses(
        &mut self,
        addresses: &std::collections::HashSet<std::net::SocketAddrV4>,
    ) {
        for bucket in &mut self.buckets {
            bucket
                .contacts
                .retain(|c| !addresses.contains(&c.node.address));
        }
    }
    pub(crate) fn saved(&self, now: Instant) -> Vec<NodeV4> {
        self.buckets
            .iter()
            .flat_map(|b| &b.contacts)
            .filter(|c| c.state(now) == State::Good)
            .map(|c| c.node)
            .take(super::MAX_SAVED_NODES)
            .collect()
    }
    pub(crate) fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.contacts.len()).sum()
    }
    pub(crate) fn good(&self, now: Instant) -> usize {
        self.buckets
            .iter()
            .flat_map(|b| &b.contacts)
            .filter(|c| c.state(now) == State::Good)
            .count()
    }
}
