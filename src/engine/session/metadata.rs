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
    pub(in crate::engine) lsd_peers: Vec<([u8; 20], SocketAddr)>,
    pub(in crate::engine) shell: Option<Shell>,
    pub(in crate::engine) progress: Option<crate::engine::torrent::observation::Progress>,
    pub(in crate::engine) peer_id: Option<[u8; 20]>,
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
        self.progress = None;
        self.managers.clear();
        dials.and(peers).and(announces)
    }
}
impl crate::engine::torrent::Network {
    pub(in crate::engine) async fn resolve(
        &mut self,
        resources: &mut Resources,
        magnet: &Magnet,
        options: &crate::engine::torrent::TorrentOptions,
        bandwidth: crate::engine::bandwidth::Throttle,
        mut stop: watch::Receiver<bool>,
        status: &watch::Sender<crate::engine::torrent::TorrentStatus>,
    ) -> io::Result<OwnedMetainfo> {
        let settings = &options.settings;
        let profile = options.profile;
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
        resources.peer_id = Some(peer_id);
        resources.progress = Some(crate::engine::torrent::observation::Progress::new(
            status.clone(),
        ));
        let progress = resources.progress.as_mut().expect("metadata progress");
        let identity = magnet.info_hash();
        let mut lsd = self
            .lsd
            .as_ref()
            .filter(|_| settings.dht)
            .map(|lsd| lsd.register(identity));

        let discovery_bytes = if magnet.trackers().next().is_some() {
            20 + magnet
                .trackers()
                .map(|url| url.len().to_string().len() + 3 + url.len())
                .sum::<usize>()
        } else {
            0
        };
        resources.shell = Some(Shell::magnet(identity, peer_id, profile, discovery_bytes)?);
        let shell = resources.shell.as_mut().expect("metadata shell");
        shell.set_bandwidth(bandwidth);
        shell.set_advertised_port(self.advertised_port.clone());
        shell.set_addresses(self, node.as_ref())?;
        shell.set_incoming(self.incoming.clone());
        let (mut contacts, dials) = self.dial_queue.client(profile);
        resources.dials = Some(dials);
        let dials = resources.dials.as_mut().expect("metadata dials");
        let manager = |key| {
            if settings.trackers {
                Announcer::new(
                    magnet.trackers().map(|u| vec![u.to_vec()]).collect(),
                    AnnounceOptions {
                        hash: key,
                        peer: peer_id,
                        port: self.advertised_port.clone(),
                        left: 16 * 1024,
                        private: false,
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
        let mut last_metadata_error = None::<String>;
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
        let mut report = tokio::time::interval(Duration::from_secs(2));
        report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut maintenance = tokio::time::interval(Duration::from_millis(250));
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let result=async {
            loop {
                if *stop.borrow() {return Err(io::Error::new(io::ErrorKind::Interrupted,"metadata stopped"));}
                progress.retain_peers(shell);
                shell.resolve_negotiations(&mut contacts);
                while shell.can_connect() && dials.ready() {
                    let Some(entry)=contacts.candidate(|a, _|shell.rejects_contact(a), |a| shell.contact_priority(a))else{dials.withdraw();break};
                    let Some(address)=shell.contact(entry.address, false)?else{contacts.dial_completed(entry.address,None,false);break};
                    let utp = contacts.try_utp(address);
                    match dials.admit(address, utp, contacts.fallback(address), contacts.methods(address)) {
                        Ok(id) => tracing::trace!(dial=?id,phase="metadata",source=entry.source,transport=dials.transport(utp),endpoint=?options.log_peer_endpoints.then_some(address),"peer_dial_started"),
                        Err(error) => {
                            tracing::trace!(phase="metadata",source=entry.source,kind=?error.kind(),%error,"peer_dial_deferred");
                            shell.connect_failed(address, false)?;contacts.dial_completed(address,None,false);break;
                        }
                    }
                    dial_keys.insert(address, (entry.hash, entry.source));
                }
                if !shell.can_connect(){dials.withdraw();}
                tokio::select! {
                    biased;
                    _=stop.changed()=>{},
                    _=tokio::time::sleep_until(deadline)=>return Err(io::Error::new(io::ErrorKind::TimedOut,"metadata resolution deadline")),
                    event=announce(managers)=>{
                        let (key, event) = event?;
                        let crate::engine::discovery::Event::Peers(peers) = event else {
                            unreachable!("private metadata is rejected");
                        };
                        let _ = contacts.add_tracker(key, &peers);
                    }
                    result=&mut lookup=>{result?;return Err(invalid("metadata lookup ended"));}
                    Some((key,address))=incoming.recv()=>{let _=contacts.add_dht(key,&[address]);}
                    (key,address)=async { match &mut lsd { Some(lsd) => lsd.next().await, None => std::future::pending().await } }=>{
                        let _=contacts.add_lsd(key,&[address]);
                        if resources.lsd_peers.len() < crate::engine::profile::MAX_QUEUED && !resources.lsd_peers.contains(&(key,address)) {
                            resources.lsd_peers.push((key,address));
                        }
                    }
                    event=dials.next(),if dials.has_work()=>{
                        if let Some(event)=event? {
                            let _dial = tracing::trace_span!("dial",dial=?event.id).entered();
                            let (key, source) = dial_keys.remove(&event.address).expect("metadata dial key");
                            contacts.dial_completed(event.address,event.transport(),event.utp_failed);
                            match event.result {
                                Ok(socket)=>{
                                    if !progress.attach_peer(socket, crate::engine::peer_io::Role::Outbound(key, event.methods, event.address), shell, source, options.log_peer_endpoints, None)? {
                                        contacts.negotiated(event.address, crate::engine::session::dial::NegotiationOutcome::Failed);
                                        shell.connect_failed(event.address, false)?;
                                    }
                                }
                                Err(error)=>{
                                    tracing::trace!(dial=?event.id,phase="metadata",source,utp_failed=event.utp_failed,kind=?error.kind(),os_code=error.raw_os_error(),%error,"dial_failed");
                                    shell.connect_failed(event.address, false)?;
                                }
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
                        progress.attach_peer(socket, crate::engine::peer_io::Role::Inbound, shell, "metadata", options.log_peer_endpoints, Some(prepared))?;
                    }
                    event=shell.next()=>{
                        event?;
                        if let Some(error) = shell.metadata_error()
                            && last_metadata_error.as_deref() != Some(error) {
                            tracing::debug!(%error, "metadata_peer_rejected");
                            last_metadata_error = Some(error.to_owned());
                            if options.diagnostics {
                                status.send_modify(|s| {
                                    let diagnostics = s.diagnostics.as_mut().expect("enabled diagnostics");
                                    Arc::make_mut(diagnostics).metadata_error = Some(error.to_owned());
                                });
                            }
                        }
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
                        if let Some(meta)=resolved {
                            return retain_trackers(meta,magnet,profile);
                        }
                    }
                    _=maintenance.tick()=>{},
                    _=report.tick()=>{
                        progress.publish_peers(status);
                        if options.diagnostics {
                            let trackers = managers.iter().flat_map(|(_, a)| a.snapshots()).collect();
                            status.send_modify(|s| {
                                let diagnostics = Arc::make_mut(s.diagnostics.as_mut().expect("enabled diagnostics"));
                                diagnostics.sampled_at = Some(Instant::now().into_std());
                                diagnostics.pending_contacts = contacts.progress();
                                diagnostics.trackers = trackers;
                            });
                        }
                    },
                }
            }
        }.await;
        let dials = dials.shutdown().await;
        // Successful acquisition retains the shell and its bounded socket queues
        // until storage admission; errors and cancellation still join every peer.
        let peers = if result.is_err() {
            shell.shutdown().await
        } else {
            Ok(())
        };
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
) -> io::Result<([u8; 20], crate::engine::discovery::Event)> {
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
    let mut bytes = b"d13:announce-listl".to_vec();
    let mut seen = std::collections::HashSet::new();
    for tracker in magnet.trackers().filter(|tracker| seen.insert(*tracker)) {
        bytes.push(b'l');
        bytes.extend_from_slice(format!("{}:", tracker.len()).as_bytes());
        bytes.extend_from_slice(tracker);
        bytes.push(b'e');
    }
    bytes.push(b'e');
    bytes.extend_from_slice(&meta.source_bytes()[1..]);
    profile.parse_owned(&bytes).map_err(Into::into)
}
