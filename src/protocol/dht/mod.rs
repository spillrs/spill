//! Pure DHT message codecs only; no routing, token service or networking.

pub mod krpc;
mod security;

/// A DHT node identity, distinct from a torrent's v1 info hash. All bit patterns
/// are structurally valid; this is not proof of node ownership or reachability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub [u8; 20]);

use std::net::SocketAddr;

/// A DHT contact; IPv4 and IPv6 contacts belong to separate routing tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Contact {
    pub id: NodeId,
    pub address: SocketAddr,
}
#[derive(Clone, Debug)]
pub enum OwnedQuery {
    Ping,
    FindNode(NodeId),
    GetPeers([u8; 20]),
    AnnouncePeer {
        hash: [u8; 20],
        port: u16,
        token: Vec<u8>,
    },
}
#[derive(Debug)]
pub struct OwnedReply {
    pub id: NodeId,
    pub nodes: Vec<Contact>,
    pub token: Option<Vec<u8>>,
    pub peers: Vec<SocketAddr>,
}

impl OwnedQuery {
    /// Encode owned request data without granting any network authority.
    pub fn encode(
        &self,
        transaction: &[u8],
        id: NodeId,
        want: Option<krpc::Want>,
        limits: crate::protocol::bencode::Limits,
        max_nodes: usize,
        max_peers: usize,
    ) -> Result<Vec<u8>, krpc::Error> {
        use crate::protocol::bencode::{Node as BNode, Value};
        use krpc::{Body, Message};
        use std::collections::BTreeMap;
        fn bytes(value: &[u8]) -> BNode<'_> {
            BNode::new(Value::Bytes(value))
        }
        let mut arguments = BTreeMap::from([(b"id".as_slice(), bytes(&id.0))]);
        let method: &[u8] = match self {
            Self::Ping => b"ping",
            Self::FindNode(target) => {
                arguments.insert(b"target", bytes(&target.0));
                b"find_node"
            }
            Self::GetPeers(hash) => {
                arguments.insert(b"info_hash", bytes(hash));
                b"get_peers"
            }
            Self::AnnouncePeer { hash, port, token } => {
                arguments.insert(b"info_hash", bytes(hash));
                arguments.insert(b"port", BNode::new(Value::Integer(i64::from(*port))));
                arguments.insert(b"token", bytes(token));
                b"announce_peer"
            }
        };
        if matches!(self, Self::FindNode(_) | Self::GetPeers(_))
            && let Some(want) = want
        {
            if !want.ipv4 && !want.ipv6 {
                return Err(krpc::Error::InvalidField("want"));
            }
            let mut families = Vec::new();
            if want.ipv4 {
                families.push(bytes(b"n4"));
            }
            if want.ipv6 {
                families.push(bytes(b"n6"));
            }
            arguments.insert(b"want", BNode::new(Value::List(families)));
        }
        Message::new(
            transaction,
            Body::ReadOnlyQuery {
                method,
                arguments: BNode::new(Value::Dictionary(arguments)),
            },
            None,
            max_nodes,
            max_peers,
        )
        .and_then(|m| m.encode(limits))
    }
}
