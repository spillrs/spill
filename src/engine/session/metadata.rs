//! Session discovery adapter for the core's metadata phase. No peer protocol state.
use crate::engine::{
    core::extensions::invalid,
    discovery::{AnnounceOptions, Announcer},
    profile::Profile,
    session::dht::Handle,
    torrent::shell::Shell,
};
use crate::protocol::{magnet::Magnet, metainfo::OwnedMetainfo};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    sync::watch,
    time::{Duration, Instant},
};

/// Shared network authority obtained from a live Session. Each attempt uses the
/// same shell/core as payload transfer and joins its peers and pending dials.
pub struct Network {
    pub(in crate::engine) port: watch::Receiver<u16>,
    pub(in crate::engine) dial_queue: crate::engine::session::dial::Queue,
    pub(in crate::engine) tracker_http: reqwest::Client,
    pub(in crate::engine) tracker_udp: crate::engine::session::udp::Transport,
    pub(in crate::engine) allow_local_network: bool,
    pub(in crate::engine) dht: Option<Handle>,
    pub(in crate::engine) stop: watch::Receiver<bool>,
}
impl Network {
    pub async fn resolve(
        &mut self,
        magnet: &Magnet,
        trackers: bool,
        dht: bool,
        profile: Profile,
        mut stop: watch::Receiver<bool>,
    ) -> io::Result<OwnedMetainfo> {
        let peer_id = crate::engine::identity::generate_peer_id()?;
        let mut shell = Shell::magnet(magnet.info_hash(), peer_id, profile)?;
        shell.set_advertised_port(self.port.clone());
        let (mut contacts, mut dials) = self.dial_queue.client(profile);
        let mut manager = if trackers {
            Announcer::new(
                vec![magnet.trackers().map(|u| u.to_vec()).collect()],
                AnnounceOptions {
                    hash: magnet.info_hash(),
                    peer: peer_id,
                    port: self.port.clone(),
                    left: 1,
                    allow_local_network: self.allow_local_network,
                    client: self.tracker_http.clone(),
                    udp: self.tracker_udp.clone(),
                    completed_announce: None,
                    checkpoint: Arc::new(|_| Ok(())),
                },
            )?
        } else {
            None
        };
        let node = self.dht.as_ref().filter(|_| dht);
        if manager.is_none() && node.is_none() {
            return Err(invalid("no metadata discovery sources are enabled"));
        }
        let (sender, mut incoming) = tokio::sync::mpsc::channel(crate::engine::profile::MAX_QUEUED);
        let lookup = async {
            if let Some(node) = node {
                loop {
                    if let Err(error) = node
                        .metadata_peers(magnet.info_hash(), sender.clone())
                        .await
                        && error.kind() == io::ErrorKind::BrokenPipe
                    {
                        return Err(error);
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            } else {
                std::future::pending::<io::Result<()>>().await
            }
        };
        tokio::pin!(lookup);
        let deadline = Instant::now() + crate::engine::profile::TOTAL_TIMEOUT;
        let mut maintenance = tokio::time::interval(Duration::from_millis(250));
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let result=async {
            loop {
                if *stop.borrow() || *self.stop.borrow() {return Err(io::Error::new(io::ErrorKind::Interrupted,"metadata stopped"));}
                while shell.can_connect() && dials.ready() {
                    let Some(address)=contacts.candidate(|a|shell.rejects_contact(a)).map_err(invalid)?else{dials.withdraw();break};
                    let Some(address)=shell.contact(address)?else{contacts.dial_completed(address,false,false);break};
                    if dials.admit(address, contacts.try_utp(address)).is_err() {shell.connect_failed(address)?;contacts.dial_completed(address,false,false);break;}
                }
                if !shell.can_connect(){dials.withdraw();}
                tokio::select! {
                    biased;
                    _=stop.changed()=>{},
                    _=self.stop.changed()=>{},
                    _=tokio::time::sleep_until(deadline)=>return Err(io::Error::new(io::ErrorKind::TimedOut,"metadata resolution deadline")),
                    event=announce(&mut manager)=>{
                        let peers = event?;
                        let _ = contacts.add_tracker(&peers);
                    }
                    result=&mut lookup=>{result?;return Err(invalid("metadata lookup ended"));}
                    Some(address)=incoming.recv()=>{let _=contacts.add_dht(magnet.info_hash(),&[SocketAddr::V4(address)]);}
                    event=dials.next(),if dials.has_work()=>{
                        if let Some(event)=event? {
                            contacts.dial_completed(event.address,event.result.is_ok(),event.utp_failed);
                            match event.result {
                                Ok(socket)=>{shell.attach_peer(socket,crate::engine::peer_io::Role::Outbound)?;}
                                Err(_)=>shell.connect_failed(event.address)?,
                            }
                        }
                    }
                    event=shell.next()=>{
                        event?;
                        if let Some(meta)=shell.take_metadata(){return retain_trackers(meta,magnet,profile);}
                    }
                    _=maintenance.tick()=>{},
                }
            }
        }.await;
        // Never return stopped/success/failure before all admitted socket work joins.
        let dials = dials.shutdown().await;
        let peers = shell.shutdown().await;
        if let Some(manager) = &mut manager {
            manager.shutdown().await;
        }
        result.and_then(|meta| dials.and(peers).map(|()| meta))
    }
}
async fn announce(manager: &mut Option<Announcer>) -> io::Result<Vec<SocketAddr>> {
    let Some(manager) = manager else {
        return std::future::pending().await;
    };
    manager.next().await
}
fn retain_trackers(
    meta: OwnedMetainfo,
    magnet: &Magnet,
    profile: Profile,
) -> io::Result<OwnedMetainfo> {
    if meta.is_private() {
        return Err(invalid("private torrents require a .torrent file"));
    }
    if magnet.trackers().next().is_none() {
        return Ok(meta);
    }
    let mut bytes = b"d13:announce-listll".to_vec();
    for tracker in magnet.trackers() {
        bytes.extend_from_slice(format!("{}:", tracker.len()).as_bytes());
        bytes.extend_from_slice(tracker);
    }
    bytes.extend_from_slice(b"ee4:info");
    bytes.extend_from_slice(meta.info_bytes());
    bytes.push(b'e');
    profile.parse_owned(&bytes).map_err(Into::into)
}
