//! Session discovery adapter for the core's metadata phase. No peer protocol state.
use crate::engine::{
    core::extensions::invalid,
    discovery::{AnnounceOptions, Announcer},
    next_ready,
    profile::Profile,
    torrent::shell::Shell,
};
use crate::protocol::{magnet::Magnet, metainfo::OwnedMetainfo};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    sync::watch,
    time::{Duration, Instant},
};

#[derive(Default)]
pub(in crate::engine) struct Resources {
    pub(in crate::engine) peers: Vec<([u8; 20], SocketAddr, &'static str)>,
    shell: Option<Shell>,
    dials: Option<crate::engine::session::dial::Client>,
    managers: Vec<([u8; 20], Announcer)>,
}
impl Resources {
    pub(in crate::engine) async fn drain(&mut self) -> io::Result<()> {
        let dials = match &mut self.dials {
            Some(dials) => dials.shutdown().await,
            None => Ok(()),
        };
        let peers = match &mut self.shell {
            Some(shell) => shell.drain().await,
            None => Ok(()),
        };
        let mut announces = Ok(());
        for (_, manager) in &mut self.managers {
            announces = announces.and(manager.drain().await);
        }
        self.dials = None;
        self.shell = None;
        self.managers.clear();
        dials.and(peers).and(announces)
    }
}
impl crate::engine::torrent::Network {
    pub(in crate::engine) async fn resolve(
        &mut self,
        resources: &mut Resources,
        magnet: &Magnet,
        settings: &crate::engine::settings::TorrentSettings,
        profile: Profile,
        mut stop: watch::Receiver<bool>,
        status: &watch::Sender<crate::engine::torrent::TorrentStatus>,
    ) -> io::Result<OwnedMetainfo> {
        if *stop.borrow() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "metadata stopped",
            ));
        }
        let node = if settings.dht
            && let Some(dht) = &self.dht
        {
            Some(tokio::select! {
                biased;
                _ = stop.changed() => return Err(io::ErrorKind::Interrupted.into()),
                node = dht.get() => node?,
            })
        } else {
            None
        };
        let peer_id = crate::engine::identity::generate_peer_id()?;
        let identity = magnet.info_hash();
        let mut lsd = self
            .lsd
            .as_ref()
            .filter(|_| settings.dht)
            .map(|lsd| lsd.register(identity));

        let discovery_bytes = if magnet.trackers().next().is_some() {
            20 + magnet
                .trackers()
                .map(|url| url.len().to_string().len() + 1 + url.len())
                .sum::<usize>()
        } else {
            0
        };
        resources.shell = Some(Shell::magnet(identity, peer_id, profile, discovery_bytes)?);
        let shell = resources.shell.as_mut().expect("metadata shell");
        shell.set_advertised_port(self.advertised_port.clone());
        shell.set_incoming(self.incoming.clone());
        let (mut contacts, dials) = self.dial_queue.client(profile);
        resources.dials = Some(dials);
        let dials = resources.dials.as_mut().expect("metadata dials");
        let manager = |key| {
            if settings.trackers {
                Announcer::new(
                    vec![magnet.trackers().map(|u| u.to_vec()).collect()],
                    AnnounceOptions {
                        hash: key,
                        peer: peer_id,
                        port: self.advertised_port.clone(),
                        left: 1,
                        allow_local_network: self.allow_local_network,
                        client: self.tracker_http.clone(),
                        udp: self.tracker_udp.clone(),
                        completed_announce: None,
                        checkpoint: Arc::new(|_| Ok(())),
                    },
                )
            } else {
                Ok(None)
            }
        };
        for key in identity.wire_keys() {
            if let Some(announcer) = manager(key)? {
                resources.managers.push((key, announcer));
            }
        }
        let managers = &mut resources.managers;
        if managers.is_empty() && node.is_none() && lsd.is_none() {
            return Err(invalid("no metadata discovery sources are enabled"));
        }
        let (sender, mut incoming) = tokio::sync::mpsc::channel(crate::engine::profile::MAX_QUEUED);
        let mut dial_keys = std::collections::HashMap::new();
        let lookup = async {
            if let Some(node) = &node {
                let lookups = identity.wire_keys().map(|key| {
                        let sender = &sender;
                        async move { loop {
                            let (peers, mut stream) =
                                tokio::sync::mpsc::channel(crate::engine::profile::MAX_QUEUED);
                            let request = node.metadata_peers(key, peers);
                            tokio::pin!(request);
                            loop {
                                tokio::select! {
                                    result = &mut request => {
                                        if let Err(error) = result && error.kind() == io::ErrorKind::BrokenPipe { return Err(error); }
                                        while let Ok(address) = stream.try_recv() { let _ = sender.try_send((key, address)); }
                                        break;
                                    }
                                    Some(address) = stream.recv() => { let _ = sender.try_send((key, address)); }
                                }
                            }
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        } }
                    });
                next_ready(lookups).await
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
                if *stop.borrow() {return Err(io::Error::new(io::ErrorKind::Interrupted,"metadata stopped"));}
                shell.resolve_negotiations(&mut contacts);
                while shell.can_connect() && dials.ready() {
                    let Some(entry)=contacts.candidate(|a|shell.rejects_contact(a))else{dials.withdraw();break};
                    let Some(address)=shell.contact(entry.address)?else{contacts.dial_completed(entry.address,None,false);break};
                    if dials.admit(address, contacts.try_utp(address), contacts.fallback(address), contacts.methods(address)).is_err() {shell.connect_failed(address)?;contacts.dial_completed(address,None,false);break;}
                    dial_keys.insert(address, entry.hash);
                }
                if !shell.can_connect(){dials.withdraw();}
                tokio::select! {
                    biased;
                    _=stop.changed()=>{},
                    _=tokio::time::sleep_until(deadline)=>return Err(io::Error::new(io::ErrorKind::TimedOut,"metadata resolution deadline")),
                    event=announce(managers)=>{
                        let (key, peers) = event?;
                        let _ = contacts.add_tracker(key, &peers);
                    }
                    result=&mut lookup=>{result?;return Err(invalid("metadata lookup ended"));}
                    Some((key,address))=incoming.recv()=>{let _=contacts.add_dht(key,&[address]);}
                    (key,address)=async { match &mut lsd { Some(lsd) => lsd.next().await, None => std::future::pending().await } }=>{
                        let _=contacts.add_lsd(key,&[address]);
                        if resources.peers.len() < crate::engine::profile::MAX_QUEUED && !resources.peers.iter().any(|&(hash, peer, _)| (hash,peer) == (key,address)) {
                            resources.peers.push((key,address,"lsd"));
                        }
                    }
                    event=dials.next(),if dials.has_work()=>{
                        if let Some(event)=event? {
                            let key = dial_keys.remove(&event.address).expect("metadata dial key");
                            contacts.dial_completed(event.address,event.transport(),event.utp_failed);
                            match event.result {
                                Ok(socket)=>{
                                    if let Err(error) = shell.attach_peer(socket,crate::engine::peer_io::Role::Outbound(key, event.methods, event.address), None) {
                                        // Only a per-peer rejection may leave acquisition running.
                                        if crate::engine::core::fault::is_local(&error) {
                                            return Err(error);
                                        }
                                        contacts.negotiated(event.address, crate::engine::session::dial::NegotiationOutcome::Failed);
                                        tracing::trace!(kind=?error.kind(), "metadata_peer_rejected");
                                        shell.connect_failed(event.address)?;
                                    }
                                }
                                Err(_)=>shell.connect_failed(event.address)?,
                            }
                        }
                    }
                    Some(socket)=self.sockets.recv()=>{
                        let (socket, prepared) = match socket {
                            super::Inbound::Tcp(socket, prepared) => match tokio::net::TcpStream::from_std(socket) {
                                Ok(socket) => (crate::engine::peer_io::Stream::Tcp(socket), prepared),
                                Err(error) => { tracing::trace!(%error, "metadata_peer_register_failed"); continue; },
                            },
                            super::Inbound::Utp(socket, prepared) => (crate::engine::peer_io::Stream::Utp(socket), prepared),
                        };
                        if let Err(error) = shell.attach_peer(socket, crate::engine::peer_io::Role::Inbound, Some(prepared)) {
                            if crate::engine::core::fault::is_local(&error) { return Err(error); }
                            tracing::trace!(%error, "metadata_peer_rejected");
                        }
                    }
                    event=shell.next()=>{
                        event?;
                        let resolved = shell.take_metadata();
                        if let Some((hash_bytes, hash_total_bytes)) = shell.take_metadata_progress() {
                            status.send_if_modified(|s| {
                                let progress = &mut s.progress;
                                let changed = (progress.info_validated, progress.hash_bytes, progress.hash_total_bytes)
                                    != (true, hash_bytes, hash_total_bytes);
                                progress.info_validated = true;
                                progress.hash_bytes = hash_bytes;
                                progress.hash_total_bytes = hash_total_bytes;
                                changed
                            });
                        }
                        if let Some(crate::engine::core::ResolvedMetadata { metainfo: meta, peers })=resolved {
                            for (hash, address) in peers {
                                if !resources.peers.iter().any(|&(key, peer, _)| (key,peer) == (hash,address)) {
                                    resources.peers.push((hash,address,"metadata"));
                                }
                            }
                            return retain_trackers(meta,magnet,profile);
                        }
                    }
                    _=maintenance.tick()=>{},
                }
            }
        }.await;
        // Never return stopped/success/failure before all admitted socket work joins.
        let dials = dials.shutdown().await;
        let peers = shell.shutdown().await;
        let mut announces = Ok(());
        for (_, manager) in managers {
            announces = announces.and(manager.shutdown().await);
        }
        dials.and(peers).and(announces)?;
        result
    }
}
async fn announce(
    managers: &mut [([u8; 20], Announcer)],
) -> io::Result<([u8; 20], Vec<SocketAddr>)> {
    next_ready(
        managers
            .iter_mut()
            .map(|(key, manager)| async move { manager.next().await.map(|peers| (*key, peers)) }),
    )
    .await
}
fn retain_trackers(
    meta: OwnedMetainfo,
    magnet: &Magnet,
    profile: Profile,
) -> io::Result<OwnedMetainfo> {
    if magnet.trackers().next().is_none() {
        return Ok(meta);
    }
    let mut bytes = b"d13:announce-listll".to_vec();
    for tracker in magnet.trackers() {
        bytes.extend_from_slice(format!("{}:", tracker.len()).as_bytes());
        bytes.extend_from_slice(tracker);
    }
    bytes.extend_from_slice(b"ee");
    bytes.extend_from_slice(&meta.source_bytes()[1..]);
    profile.parse_owned(&bytes).map_err(Into::into)
}
