//! Connection admission and discovery handoff for the torrent runtime.
use super::{
    RuntimeOptions,
    discovery::Discovery,
    observation::{Observer, Peer},
    shell::Shell,
};
use crate::engine::{
    peer_io::{PeerSocket, Prepared, Role, Stream},
    session::dial::{ConnectionEvent, DialId},
};
use std::{collections::HashMap, io};
use tokio::time::Instant;
use tracing::trace;

#[derive(Default)]
pub(super) struct Admission {
    pub(super) pending: usize,
    sources: HashMap<DialId, (&'static str, [u8; 20])>,
}
impl Admission {
    pub(super) async fn tracker_changed(
        &mut self,
        transfer: &mut Shell,
        discovery: &mut Discovery,
        observer: &mut Observer,
    ) -> io::Result<()> {
        let dials = discovery
            .dials
            .as_mut()
            .expect("online dials")
            .shutdown()
            .await;
        discovery
            .contacts
            .as_mut()
            .expect("online contacts")
            .clear();
        let peers = transfer.tracker_changed().await;
        self.sources.clear();
        self.pending = 0;
        observer.retain_peers(transfer);
        dials.and(peers)
    }

    pub(super) fn prepare_dials(
        &mut self,
        transfer: &mut Shell,
        discovery: &mut Discovery,
        options: &RuntimeOptions,
    ) -> io::Result<()> {
        let queue = discovery.contacts.as_mut().expect("online contacts");
        let dials = discovery.dials.as_mut().expect("online dial client");
        transfer.drain_holepunches(queue);
        loop {
            let punch = queue.has_due_punch();
            if !transfer.can_punch() || (!transfer.can_connect() && !punch) {
                dials.withdraw();
                break;
            }
            if !dials.ready() {
                break;
            }
            let Some(entry) = queue.candidate(
                |a, source| {
                    transfer.rejects_contact(a)
                        || (source != "holepunch" && !transfer.can_connect())
                },
                |a| transfer.contact_priority(a),
            ) else {
                dials.withdraw();
                return Ok(());
            };
            let Some(address) = transfer.contact(entry.address, entry.source == "holepunch")?
            else {
                queue.dial_completed(entry.address, None, false);
                dials.withdraw();
                return Ok(());
            };
            let utp = queue.try_utp(address);
            match dials.admit(
                address,
                utp,
                queue.fallback(address),
                queue.methods(address),
            ) {
                Ok(id) => {
                    self.sources.insert(id, (entry.source, entry.hash));
                    self.pending += 1;
                    trace!(dial=?id,source=entry.source,transport=dials.transport(utp),endpoint=?options.log_peer_endpoints.then_some(address),"peer_dial_started");
                }
                Err(error) => {
                    trace!(source=entry.source,kind=?error.kind(),%error,"peer_dial_deferred");
                    transfer.connect_failed(address, false)?;
                    queue.dial_completed(address, None, false);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub(super) fn direct_event(
        &mut self,
        event: Option<ConnectionEvent>,
        transfer: &mut Shell,
        options: &RuntimeOptions,
        contacts: &mut crate::engine::session::dial::Contacts,
        observer: &mut Observer,
    ) -> io::Result<()> {
        let event = event.ok_or_else(|| io::Error::other("unexpected dial completion"))?;
        let _dial = tracing::trace_span!("dial", dial=?event.id).entered();
        self.pending = self.pending.saturating_sub(1);
        let (source, hash) = self.sources.remove(&event.id).expect("admitted dial");
        match event.result {
            Ok(socket) => {
                if !attach_peer(
                    observer,
                    socket,
                    Role::Outbound(hash, event.methods, event.address),
                    transfer,
                    source,
                    options.log_peer_endpoints,
                    None,
                )? {
                    contacts.negotiated(
                        event.address,
                        crate::engine::session::dial::NegotiationOutcome::Failed,
                    );
                    transfer.connect_failed(event.address, false)?;
                }
            }
            Err(error) => {
                transfer
                    .connect_failed(event.address, source != "holepunch" && event.utp_failed)?;
                trace!(dial=?event.id,source,utp_failed=event.utp_failed,kind=?error.kind(),os_code=error.raw_os_error(),%error,"dial_failed");
            }
        }
        Ok(())
    }
}

pub(super) fn attach_peer(
    observer: &mut Observer,
    socket: Stream,
    role: Role,
    transfer: &mut Shell,
    source: &'static str,
    log_endpoint: bool,
    prepared: Option<Prepared>,
) -> io::Result<bool> {
    let address = socket.peer_addr().ok();
    let transport = socket.transport().as_str();
    let direction = if matches!(role, Role::Inbound) {
        "inbound"
    } else {
        "outbound"
    };
    let retransmits = match &socket {
        Stream::Utp(s) => Some(s.retransmits()),
        Stream::Tcp(_) => None,
    };
    match transfer.attach_peer(socket, role, prepared) {
        Ok(id) => {
            trace!(connection=?id,source,transport,direction,endpoint=?address.filter(|_|log_endpoint),"peer_admitted");
            observer.remember(
                id,
                Peer {
                    address,
                    direction,
                    source,
                    transport,
                    retransmits,
                    web_seed_url: None,
                    sampled: Instant::now(),
                    downloaded: 0,
                    uploaded: 0,
                },
            );
            Ok(true)
        }
        Err(error) => {
            if crate::engine::core::fault::is_local(&error) {
                return Err(error);
            }
            trace!(source,transport,direction,endpoint=?address.filter(|_|log_endpoint),kind=?error.kind(),%error,"peer_rejected");
            Ok(false)
        }
    }
}

pub(super) fn prepare_discovery(
    observer: &mut Observer,
    transfer: &mut Shell,
    discovery: &mut Discovery,
    options: &RuntimeOptions,
) -> io::Result<()> {
    if let Some(seeds) = &mut discovery.web_seeds {
        seeds.active.retain(|id| transfer.has_peer(*id));
        let slots = options
            .profile
            .peers
            .min(crate::engine::webseed::MAX_ACTIVE);
        let web_slots =
            if options.settings.trackers || options.dht.is_some() || !options.peers.is_empty() {
                slots.div_ceil(2)
            } else {
                slots
            };
        while transfer.can_connect() && seeds.active.len() < web_slots {
            let Some(seed) = seeds.next(options.metainfo.clone()) else {
                break;
            };
            let url = seed.url().to_owned();
            let id = transfer.attach_socket(PeerSocket::WebSeed(Box::new(seed)))?;
            seeds.active.push(id);
            observer.remember(
                id,
                Peer {
                    address: None,
                    direction: "outbound",
                    source: "web_seed",
                    transport: "http",
                    retransmits: None,
                    web_seed_url: Some(url),
                    sampled: Instant::now(),
                    downloaded: 0,
                    uploaded: 0,
                },
            );
            trace!(connection=?id,source="web_seed","web_seed_admitted");
        }
    }
    if let Some(queue) = &mut discovery.contacts {
        transfer.drain_pex(queue);
    }
    if let Some(dht) = &options.dht {
        transfer.drain_dht(dht);
    }
    Ok(())
}
