//! Bounded request collections. Keys avoid changing the wire codec's types.
use crate::protocol::peer::message::BlockRequest;
use std::{
    collections::HashMap,
    ops::{Deref, RangeFull},
};

type Key = (u32, u32, u32);
fn key(r: &BlockRequest) -> Key {
    (r.index, r.begin, r.length)
}

#[derive(Default)]
pub(in crate::engine) struct Asked {
    items: Vec<BlockRequest>,
    positions: HashMap<Key, usize>,
}
impl Deref for Asked {
    type Target = [BlockRequest];
    fn deref(&self) -> &Self::Target {
        &self.items
    }
}
impl<'a> IntoIterator for &'a Asked {
    type Item = &'a BlockRequest;
    type IntoIter = std::slice::Iter<'a, BlockRequest>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}
impl Asked {
    pub(in crate::engine) fn with_capacity(n: usize) -> Self {
        Self {
            items: Vec::with_capacity(n),
            positions: HashMap::with_capacity(n),
        }
    }
    pub(in crate::engine) fn contains(&self, r: &BlockRequest) -> bool {
        self.positions.contains_key(&key(r))
    }
    pub(in crate::engine) fn push(&mut self, r: BlockRequest) {
        if self.contains(&r) {
            return;
        }
        self.positions.insert(key(&r), self.items.len());
        self.items.push(r);
    }
    pub(in crate::engine) fn remove_request(&mut self, r: &BlockRequest) -> bool {
        let Some(position) = self.positions.remove(&key(r)) else {
            return false;
        };
        self.items.swap_remove(position);
        if let Some(moved) = self.items.get(position) {
            self.positions.insert(key(moved), position);
        }
        true
    }
    pub(in crate::engine) fn retain(&mut self, mut keep: impl FnMut(&BlockRequest) -> bool) {
        let mut position = 0;
        while position < self.items.len() {
            let r = self.items[position];
            if keep(&r) {
                position += 1;
            } else {
                self.remove_request(&r);
            }
        }
    }
}

struct Node {
    request: BlockRequest,
    previous: Option<Key>,
    next: Option<Key>,
}
/// Intrusive-by-key FIFO: cancellation unlinks in O(1), with no tombstones or
/// monotonically growing sequence/index space. Count is the owner's profile cap.
#[derive(Default)]
pub(in crate::engine) struct Uploads {
    nodes: HashMap<Key, Node>,
    first: Option<Key>,
    last: Option<Key>,
}
impl Uploads {
    pub(in crate::engine) fn with_capacity(n: usize) -> Self {
        Self {
            nodes: HashMap::with_capacity(n),
            ..Self::default()
        }
    }
    pub(in crate::engine) fn len(&self) -> usize {
        self.nodes.len()
    }
    pub(in crate::engine) fn contains(&self, r: &BlockRequest) -> bool {
        self.nodes.contains_key(&key(r))
    }
    pub(in crate::engine) fn front(&self) -> Option<&BlockRequest> {
        self.nodes.get(&self.first?).map(|node| &node.request)
    }
    pub(in crate::engine) fn push_back(&mut self, r: BlockRequest) {
        let k = key(&r);
        if self.nodes.contains_key(&k) {
            return;
        }
        if let Some(last) = self.last {
            self.nodes.get_mut(&last).expect("FIFO tail").next = Some(k);
        } else {
            self.first = Some(k);
        }
        self.nodes.insert(
            k,
            Node {
                request: r,
                previous: self.last,
                next: None,
            },
        );
        self.last = Some(k);
    }
    pub(in crate::engine) fn remove_request(&mut self, r: &BlockRequest) -> bool {
        let Some(node) = self.nodes.remove(&key(r)) else {
            return false;
        };
        if let Some(previous) = node.previous {
            self.nodes
                .get_mut(&previous)
                .expect("FIFO predecessor")
                .next = node.next;
        } else {
            self.first = node.next;
        }
        if let Some(next) = node.next {
            self.nodes.get_mut(&next).expect("FIFO successor").previous = node.previous;
        } else {
            self.last = node.previous;
        }
        true
    }
    pub(in crate::engine) fn pop_front(&mut self) -> Option<BlockRequest> {
        let r = *self.front()?;
        self.remove_request(&r);
        Some(r)
    }
    pub(in crate::engine) fn clear(&mut self) {
        self.nodes.clear();
        self.first = None;
        self.last = None;
    }
    pub(in crate::engine) fn drain(&mut self, _: RangeFull) -> std::vec::IntoIter<BlockRequest> {
        let mut requests = Vec::with_capacity(self.len());
        while let Some(r) = self.pop_front() {
            requests.push(r);
        }
        requests.into_iter()
    }
}
