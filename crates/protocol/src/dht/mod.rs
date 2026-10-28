//! Pure DHT message codecs only; no routing, token service or networking.

pub mod krpc;

/// A DHT node identity, distinct from a torrent's v1 info hash. All bit patterns
/// are structurally valid; this is not proof of node ownership or reachability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub [u8; 20]);

use krpc::NodeV4;
use std::net::SocketAddrV4;
#[derive(Clone, Debug)]
pub enum OwnedQuery {
    Ping,
    FindNode(NodeId),
    GetPeers(crate::identity::InfoHashV1),
    AnnouncePeer {
        hash: crate::identity::InfoHashV1,
        port: u16,
        token: Vec<u8>,
    },
}
#[derive(Debug)]
pub struct OwnedReply {
    pub id: NodeId,
    pub nodes: Vec<NodeV4>,
    pub token: Option<Vec<u8>>,
    pub peers: Vec<SocketAddrV4>,
}

impl OwnedQuery {
    /// Encode owned request data without granting any network authority.
    pub fn encode(
        &self,
        transaction: &[u8],
        id: NodeId,
        limits: crate::bencode::Limits,
        max_nodes: usize,
        max_peers: usize,
    ) -> Result<Vec<u8>, krpc::Error> {
        use crate::bencode::{Node as BNode, Value};
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
                arguments.insert(b"info_hash", bytes(hash.as_bytes()));
                b"get_peers"
            }
            Self::AnnouncePeer { hash, port, token } => {
                arguments.insert(b"info_hash", bytes(hash.as_bytes()));
                arguments.insert(b"port", BNode::new(Value::Integer(i64::from(*port))));
                arguments.insert(b"token", bytes(token));
                b"announce_peer"
            }
        };
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
