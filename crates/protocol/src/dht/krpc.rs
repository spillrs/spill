//! BEP 5 KRPC envelopes, four base queries and BEP 32 IPv4/IPv6 contacts.
//! No sockets, transaction/source matching, routing, token verification or
//! automatic replies. Successful parsing never authorizes an announce/dial.
//! Unknown fields/methods/types remain inspectable; runtime owns address policy.

use std::{
    collections::BTreeMap,
    fmt,
    iter::FusedIterator,
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
};

use super::NodeId;
use crate::{
    bencode::{self, Node, Value},
    compact,
    identity::InfoHashV1,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Bencode(bencode::Error),
    Dictionary,
    Missing(&'static str),
    InvalidField(&'static str),
    NodeLimit,
    PeerLimit,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KRPC: {self:?}")
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bencode(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeV4 {
    pub id: NodeId,
    pub address: SocketAddrV4,
}
impl NodeV4 {
    pub fn encode(&self) -> [u8; 26] {
        let mut bytes = [0; 26];
        bytes[..20].copy_from_slice(&self.id.0);
        bytes[20..].copy_from_slice(&compact::encode_v4(self.address));
        bytes
    }
}

/// Borrowed 26-byte node records. Like compact peer lists, expose partial
/// tails instead of silently losing them; strict consumers can reject tails.
#[derive(Clone, Copy, Debug)]
pub struct NodesV4<'a> {
    bytes: &'a [u8],
}
impl<'a> NodesV4<'a> {
    pub fn parse(bytes: &'a [u8], max_nodes: usize) -> Result<Self, Error> {
        if bytes.len() / 26 > max_nodes {
            return Err(Error::NodeLimit);
        }
        Ok(Self { bytes })
    }
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn remainder(&self) -> &'a [u8] {
        &self.bytes[self.bytes.len() / 26 * 26..]
    }
    pub fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = NodeV4> + DoubleEndedIterator + FusedIterator + Clone + 'a
    {
        self.bytes.as_chunks::<26>().0.iter().map(|bytes| NodeV4 {
            id: NodeId(bytes[..20].try_into().expect("twenty-byte node id")),
            address: compact::decode_v4(&bytes[20..]).expect("six-byte endpoint"),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeV6 {
    pub id: NodeId,
    pub address: SocketAddrV6,
}
impl NodeV6 {
    /// Reject nonzero scope/flow context rather than silently discarding it.
    pub fn encode(&self) -> Result<[u8; 38], compact::Error> {
        let mut bytes = [0; 38];
        bytes[..20].copy_from_slice(&self.id.0);
        bytes[20..].copy_from_slice(&compact::encode_v6(self.address)?);
        Ok(bytes)
    }
}

/// Borrowed 38-byte BEP 32 node records, preserving partial tails.
#[derive(Clone, Copy, Debug)]
pub struct NodesV6<'a> {
    bytes: &'a [u8],
}
impl<'a> NodesV6<'a> {
    pub fn parse(bytes: &'a [u8], max_nodes: usize) -> Result<Self, Error> {
        if bytes.len() / 38 > max_nodes {
            return Err(Error::NodeLimit);
        }
        Ok(Self { bytes })
    }
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn remainder(&self) -> &'a [u8] {
        &self.bytes[self.bytes.len() / 38 * 38..]
    }
    pub fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = NodeV6> + DoubleEndedIterator + FusedIterator + Clone + 'a
    {
        self.bytes.as_chunks::<38>().0.iter().map(|bytes| NodeV6 {
            id: NodeId(bytes[..20].try_into().expect("twenty-byte node id")),
            address: compact::decode_v6(&bytes[20..]).expect("eighteen-byte endpoint"),
        })
    }
}

/// Explicit BEP 32 node-family hints, not peer-family filters. Unknown strings
/// are ignored here and retained in `document()["a"]["want"]`. Absence is distinct
/// from a list containing only unknown hints; transport defaults are not inferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Want {
    pub ipv4: bool,
    pub ipv6: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Query<'a> {
    Ping,
    FindNode {
        target: NodeId,
        want: Option<Want>,
    },
    GetPeers {
        info_hash: InfoHashV1,
        want: Option<Want>,
    },
    AnnouncePeer {
        info_hash: InfoHashV1,
        /// None when implied_port is nonzero: use the actual UDP source port.
        port: Option<u16>,
        token: &'a [u8],
    },
    /// Not a supported handler. Arguments remain in document()["a"].
    Unknown {
        method: &'a [u8],
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Kind<'a> {
    Query {
        id: NodeId,
        query: Query<'a>,
    },
    /// Responses have no method name; a runtime must correlate the full
    /// transaction ID AND source endpoint before deciding required fields.
    Response {
        id: NodeId,
        token: Option<&'a [u8]>,
        nodes: Option<NodesV4<'a>>,
        nodes6: Option<NodesV6<'a>>,
    },
    Error {
        code: i64,
        message: &'a [u8],
    },
    /// Unknown single-byte y value. Do not dispatch as query/response/error.
    Unknown {
        message_type: u8,
    },
}

/// Constructor input for standard envelopes. Use raw nodes for additional
/// query/response fields; the same semantic validation applies as on receive.
#[derive(Clone, Debug)]
pub enum Body<'a> {
    Query {
        method: &'a [u8],
        arguments: Node<'a>,
    },
    /// BEP 43 query from a node that must not be added to routing tables.
    ReadOnlyQuery {
        method: &'a [u8],
        arguments: Node<'a>,
    },
    Response(Node<'a>),
    Error {
        code: i64,
        message: &'a [u8],
    },
}

#[derive(Clone, Debug)]
pub struct Message<'a> {
    document: Node<'a>,
    transaction_id: &'a [u8],
    kind: Kind<'a>,
}
impl<'a> Message<'a> {
    /// Decode one whole datagram in compatible bencode mode. Bencode budgets
    /// cover unknown/overwritten fields, transaction IDs and tokens as well as
    /// known data. Count caps include duplicate/zero-port contacts; max_nodes
    /// covers the sum of complete IPv4 and IPv6 records. The future
    /// transport must bound buffering and discard socket-truncated datagrams.
    pub fn parse(
        input: &'a [u8],
        limits: bencode::Limits,
        max_nodes: usize,
        max_peers: usize,
    ) -> Result<Self, Error> {
        let document = bencode::decode(input, limits).map_err(Error::Bencode)?;
        Self::from_node(document, max_nodes, max_peers)
    }

    /// Version is explicit, not a fabricated client identity. None omits it.
    /// Trusted callers construct nodes; encoding later enforces AST/output
    /// budgets. No size hint allocates a response/contact buffer.
    pub fn new(
        transaction_id: &'a [u8],
        body: Body<'a>,
        version: Option<&'a [u8]>,
        max_nodes: usize,
        max_peers: usize,
    ) -> Result<Self, Error> {
        let mut fields = BTreeMap::new();
        fields.insert(b"t".as_slice(), Node::new(Value::Bytes(transaction_id)));
        if let Some(version) = version {
            fields.insert(b"v".as_slice(), Node::new(Value::Bytes(version)));
        }
        let read_only = matches!(&body, Body::ReadOnlyQuery { .. });
        let message_type: &[u8] = match body {
            Body::Query { method, arguments } | Body::ReadOnlyQuery { method, arguments } => {
                if read_only {
                    fields.insert(b"ro".as_slice(), Node::new(Value::Integer(1)));
                }
                fields.insert(b"q".as_slice(), Node::new(Value::Bytes(method)));
                fields.insert(b"a".as_slice(), arguments);
                b"q"
            }
            Body::Response(response) => {
                fields.insert(b"r".as_slice(), response);
                b"r"
            }
            Body::Error { code, message } => {
                fields.insert(
                    b"e".as_slice(),
                    Node::new(Value::List(vec![
                        Node::new(Value::Integer(code)),
                        Node::new(Value::Bytes(message)),
                    ])),
                );
                b"e"
            }
        };
        fields.insert(b"y".as_slice(), Node::new(Value::Bytes(message_type)));
        Self::from_node(Node::new(Value::Dictionary(fields)), max_nodes, max_peers)
    }

    pub fn from_node(
        document: Node<'a>,
        max_nodes: usize,
        max_peers: usize,
    ) -> Result<Self, Error> {
        if !matches!(document.value(), Value::Dictionary(_)) {
            return Err(Error::Dictionary);
        }
        let transaction_id = bytes(&document, "t")?;
        let message_type = bytes(&document, "y")?;
        let kind = match message_type {
            b"q" => {
                let method = bytes(&document, "q")?;
                let args = dictionary(&document, "a")?;
                let id = NodeId(array20(args, "id")?);
                let query = match method {
                    b"ping" => Query::Ping,
                    b"find_node" => Query::FindNode {
                        target: NodeId(array20(args, "target")?),
                        want: want(args)?,
                    },
                    b"get_peers" => Query::GetPeers {
                        info_hash: InfoHashV1::from_bytes(array20(args, "info_hash")?),
                        want: want(args)?,
                    },
                    b"announce_peer" => {
                        let implied = match args.get(b"implied_port") {
                            None => false,
                            Some(node) => match node.value() {
                                Value::Integer(value) => *value != 0,
                                _ => return Err(Error::InvalidField("implied_port")),
                            },
                        };
                        let port = if implied {
                            // BEP 5 says to ignore port when implied_port is
                            // nonzero, even if absent, wrong-typed or oversized.
                            None
                        } else {
                            let Value::Integer(value) = field(args, "port")?.value() else {
                                return Err(Error::InvalidField("port"));
                            };
                            Some(u16::try_from(*value).map_err(|_| Error::InvalidField("port"))?)
                        };
                        Query::AnnouncePeer {
                            info_hash: InfoHashV1::from_bytes(array20(args, "info_hash")?),
                            port,
                            token: bytes(args, "token")?,
                        }
                    }
                    _ => Query::Unknown { method },
                };
                Kind::Query { id, query }
            }
            b"r" => {
                let response = dictionary(&document, "r")?;
                let id = NodeId(array20(response, "id")?);
                let token = response
                    .get(b"token")
                    .map(|_| bytes(response, "token"))
                    .transpose()?;
                let nodes = response
                    .get(b"nodes")
                    .map(|_| NodesV4::parse(bytes(response, "nodes")?, max_nodes))
                    .transpose()?;
                // The IPv4 parse has already established this subtraction is safe.
                let remaining = max_nodes - nodes.map_or(0, |view| view.iter().len());
                let nodes6 = response
                    .get(b"nodes6")
                    .map(|_| NodesV6::parse(bytes(response, "nodes6")?, remaining))
                    .transpose()?;
                if let Some(values) = response.get(b"values") {
                    let Value::List(peers) = values.value() else {
                        return Err(Error::InvalidField("values"));
                    };
                    if peers.len() > max_peers {
                        return Err(Error::PeerLimit);
                    }
                    for peer in peers {
                        if !matches!(peer.value(), Value::Bytes(data) if matches!(data.len(), 6 | 18))
                        {
                            return Err(Error::InvalidField("values"));
                        }
                    }
                }
                Kind::Response {
                    id,
                    token,
                    nodes,
                    nodes6,
                }
            }
            b"e" => {
                let Value::List(parts) = field(&document, "e")?.value() else {
                    return Err(Error::InvalidField("e"));
                };
                let [code, message] = parts.as_slice() else {
                    return Err(Error::InvalidField("e"));
                };
                let (Value::Integer(code), Value::Bytes(message)) = (code.value(), message.value())
                else {
                    return Err(Error::InvalidField("e"));
                };
                Kind::Error {
                    code: *code,
                    message,
                }
            }
            [message_type] => Kind::Unknown {
                message_type: *message_type,
            },
            _ => return Err(Error::InvalidField("y")),
        };
        Ok(Self {
            document,
            transaction_id,
            kind,
        })
    }

    pub fn transaction_id(&self) -> &'a [u8] {
        self.transaction_id
    }
    pub fn kind(&self) -> Kind<'a> {
        self.kind
    }
    pub fn document(&self) -> &Node<'a> {
        &self.document
    }

    /// Absent differs from an explicit empty values list. Validated IPv4/IPv6
    /// contacts retain order and duplicates, including hybrid lists required by
    /// BEP 32. No address-family filtering or endpoint allocation.
    pub fn peers(
        &self,
    ) -> Option<
        impl ExactSizeIterator<Item = SocketAddr> + DoubleEndedIterator + FusedIterator + Clone + '_,
    > {
        if !matches!(self.kind, Kind::Response { .. }) {
            return None;
        }
        let Value::List(peers) = self.document.get(b"r")?.get(b"values")?.value() else {
            unreachable!("validated values")
        };
        Some(peers.iter().map(|peer| {
            let Value::Bytes(data) = peer.value() else {
                unreachable!("validated peer")
            };
            match data.len() {
                6 => SocketAddr::V4(compact::decode_v4(data).expect("validated IPv4 peer")),
                18 => SocketAddr::V6(compact::decode_v6(data).expect("validated IPv6 peer")),
                _ => unreachable!("validated peer length"),
            }
        }))
    }

    /// Canonical bencode, including unknown fields. This is not an outbound
    /// sanitizer: raw node tails/extra fields survive. No v1 identity hashing.
    pub fn encode(&self, limits: bencode::Limits) -> Result<Vec<u8>, Error> {
        bencode::encode(&self.document, limits).map_err(Error::Bencode)
    }
}

fn want(args: &Node<'_>) -> Result<Option<Want>, Error> {
    let Some(node) = args.get(b"want") else {
        return Ok(None);
    };
    let Value::List(hints) = node.value() else {
        return Err(Error::InvalidField("want"));
    };
    if hints.is_empty() {
        return Err(Error::InvalidField("want"));
    }
    let mut want = Want {
        ipv4: false,
        ipv6: false,
    };
    for hint in hints {
        match hint.value() {
            Value::Bytes(b"n4") => want.ipv4 = true,
            Value::Bytes(b"n6") => want.ipv6 = true,
            Value::Bytes(_) => {}
            _ => return Err(Error::InvalidField("want")),
        }
    }
    Ok(Some(want))
}

fn field<'s, 'a>(node: &'s Node<'a>, name: &'static str) -> Result<&'s Node<'a>, Error> {
    node.get(name.as_bytes()).ok_or(Error::Missing(name))
}
fn bytes<'a>(node: &Node<'a>, name: &'static str) -> Result<&'a [u8], Error> {
    match field(node, name)?.value() {
        Value::Bytes(value) => Ok(value),
        _ => Err(Error::InvalidField(name)),
    }
}
fn array20(node: &Node<'_>, name: &'static str) -> Result<[u8; 20], Error> {
    bytes(node, name)?
        .try_into()
        .map_err(|_| Error::InvalidField(name))
}
fn dictionary<'s, 'a>(node: &'s Node<'a>, name: &'static str) -> Result<&'s Node<'a>, Error> {
    let value = field(node, name)?;
    if matches!(value.value(), Value::Dictionary(_)) {
        Ok(value)
    } else {
        Err(Error::InvalidField(name))
    }
}
