//! Per-peer BEP 10 negotiation, bounded BEP 9 serving and BEP 11 dispatch. Incoming IDs are
//! local; outgoing IDs follow the remote's additive handshake updates.
use crate::protocol::{
    bencode::{self, Node, Value},
    metainfo::OwnedMetainfo,
    peer::{extension, metadata},
};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
};

pub(in crate::engine) const RESERVED: [u8; 8] = [0, 0, 0, 0, 0, 0x10, 0, 0x04];
pub(in crate::engine) const METADATA_ID: u8 = 1;
// A full metadata block needs room for its bencoded header and two wire IDs.
pub(in crate::engine) const MIN_FRAME: u32 = metadata::PIECE_LENGTH + 256;

pub(in crate::engine) fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
pub(in crate::engine) fn limits() -> bencode::Limits {
    bencode::Limits {
        max_input_len: MIN_FRAME as usize,
        max_output_len: MIN_FRAME as usize - 2,
        max_string_len: MIN_FRAME as usize,
        max_nodes: 128,
        max_depth: 8,
    }
}
pub(in crate::engine) fn remote_map() -> extension::ExtensionMap {
    extension::ExtensionMap::new(&[(b"ut_metadata", 0), (b"ut_pex", 0)])
        .expect("fixed extension map")
}
pub(in crate::engine) fn handshake(
    size: Option<usize>,
    port: Option<u16>,
    pex: bool,
) -> io::Result<Vec<u8>> {
    let mut map = BTreeMap::from([(
        b"ut_metadata".as_slice(),
        Node::new(Value::Integer(i64::from(METADATA_ID))),
    )]);
    if pex {
        map.insert(
            b"ut_pex",
            Node::new(Value::Integer(i64::from(crate::engine::core::pex::ID))),
        );
    }
    let mut fields = BTreeMap::from([(b"m".as_slice(), Node::new(Value::Dictionary(map)))]);
    if let Some(port) = port {
        fields.insert(b"p", Node::new(Value::Integer(i64::from(port))));
    }
    fields.insert(
        b"v",
        Node::new(Value::Bytes(
            concat!("Spill/", env!("CARGO_PKG_VERSION")).as_bytes(),
        )),
    );
    fields.insert(
        b"reqq",
        Node::new(Value::Integer(
            // BEP 10 reqq limits requests the remote may send to us, not our
            // own download pipeline. Advertising 32 throttles external leechers.
            crate::engine::profile::MAX_UPLOAD_REQUESTS.min(u32::MAX as usize) as i64,
        )),
    );
    if let Some(size) = size {
        fields.insert(b"metadata_size", Node::new(Value::Integer(size as i64)));
    }
    extension::Handshake::from_node(Node::new(Value::Dictionary(fields)))
        .map_err(invalid)?
        .encode(limits())
        .map_err(invalid)
}

pub(in crate::engine) struct Extensions {
    pub(in crate::engine) enabled: bool,
    pub(in crate::engine) handshake_pending: bool,
    pub(in crate::engine) request_queue: u32,
    pub(in crate::engine) client: Option<String>,
    pub(in crate::engine) remote: extension::ExtensionMap,
    pub(in crate::engine) pex: crate::engine::core::pex::Pex,
    pending: VecDeque<metadata::Kind>,
}
impl Default for Extensions {
    fn default() -> Self {
        Self {
            enabled: false,
            handshake_pending: false,
            request_queue: 250,
            client: None,
            remote: remote_map(),
            pex: crate::engine::core::pex::Pex::default(),
            pending: VecDeque::new(),
        }
    }
}
impl Extensions {
    pub(in crate::engine) fn receive(
        &mut self,
        id: u8,
        payload: &[u8],
        meta: &OwnedMetainfo,
        now: std::time::Instant,
    ) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if id == 0 {
            let handshake = extension::Handshake::parse(payload, limits()).map_err(invalid)?;
            self.remote.apply(&handshake).map_err(invalid)?;
            if let Some(client) = handshake.client() {
                // Untrusted display hint; retain a bounded prefix, never log it.
                self.client =
                    Some(String::from_utf8_lossy(&client[..client.len().min(128)]).into_owned());
            }
            if self.pex.inbound
                && let Some(port) = handshake.port()
                && let Some(address) = &mut self.pex.endpoint
            {
                address.set_port(port);
            }
            if self.remote.id_for(b"ut_pex").is_none() {
                self.pex.disabled();
            }
            if let Some(request_queue) = handshake.request_queue() {
                self.request_queue = request_queue;
            }
            if self.remote.id_for(b"ut_metadata").is_none() {
                self.pending.clear();
            }
        } else if id == crate::engine::core::pex::ID
            && self.pex.enabled
            && !meta.is_private()
            && self.remote.id_for(b"ut_pex").is_some()
        {
            self.pex
                .receive(payload, crate::engine::profile::MAX_QUEUED, now)?;
        } else if id == METADATA_ID {
            let message = metadata::Message::parse(
                payload,
                limits(),
                crate::engine::profile::MAX_META.min(u32::MAX as usize) as u32,
            )
            .map_err(invalid)?;
            if let metadata::Kind::Request { piece } = message.kind()
                && self.remote.id_for(b"ut_metadata").is_some()
            {
                if self.pending.len() >= crate::engine::profile::MAX_REQUEST_QUEUE {
                    return Err(invalid("metadata request queue exhausted"));
                }
                let size = meta.info_bytes().len() as u32;
                let kind = if metadata::piece_range(
                    piece,
                    size,
                    crate::engine::profile::MAX_META.min(u32::MAX as usize) as u32,
                )
                .is_ok()
                {
                    metadata::Kind::Data {
                        piece,
                        total_size: size,
                    }
                } else {
                    metadata::Kind::Reject { piece }
                };
                self.pending.push_back(kind);
            }
        }
        Ok(())
    }
    pub(in crate::engine) fn response(
        &self,
        meta: &OwnedMetainfo,
    ) -> io::Result<Option<(u8, Vec<u8>)>> {
        let Some(id) = self.remote.id_for(b"ut_metadata") else {
            return Ok(None);
        };
        let Some(&kind) = self.pending.front() else {
            return Ok(None);
        };
        let cap = crate::engine::profile::MAX_META.min(u32::MAX as usize) as u32;
        let payload = if let metadata::Kind::Data { piece, total_size } = kind {
            let range = metadata::piece_range(piece, total_size, cap).map_err(invalid)?;
            &meta.info_bytes()[range.start as usize..range.end as usize]
        } else {
            &[]
        };
        let bytes = metadata::Message::new(kind, payload, cap)
            .map_err(invalid)?
            .encode(limits())
            .map_err(invalid)?;
        Ok(Some((id, bytes)))
    }
    pub(in crate::engine) fn sent(&mut self) {
        self.pending.pop_front();
    }
}
