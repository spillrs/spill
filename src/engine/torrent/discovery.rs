//! Discovery resources owned and drained by one torrent runtime.
use super::{Network, RuntimeOptions};
use crate::engine::discovery::{AnnounceOptions, Announcer};
use std::{io, sync::Arc};

#[derive(Default)]
pub(super) struct Discovery {
    pub(super) lsd: Option<crate::engine::session::lsd::Registration>,
    pub(super) dht: Vec<crate::engine::dht::Discovery>,
    pub(super) announces: Vec<Announcer>,
    pub(super) contacts: Option<crate::engine::session::dial::Contacts>,
    pub(super) dials: Option<crate::engine::session::dial::Client>,
    pub(super) web_seeds: Option<crate::engine::webseed::WebSeeds>,
}
impl Discovery {
    pub(super) async fn setup(
        &mut self,
        options: &RuntimeOptions,
        session: &Network,
        left: u64,
    ) -> io::Result<()> {
        for hash in crate::engine::core::swarm_identity(&options.metainfo).wire_keys() {
            if let Some(handle) = &options.dht {
                self.dht.push(crate::engine::dht::Discovery::new(
                    handle.clone(),
                    hash,
                    session.advertised_port.clone(),
                ));
            }
        }
        self.lsd = session
            .lsd
            .as_ref()
            .filter(|_| options.settings.dht)
            .map(|lsd| lsd.register(crate::engine::core::swarm_identity(&options.metainfo)));
        self.web_seeds = if options.settings.web_seeds && left != 0 {
            Some(crate::engine::webseed::WebSeeds::new(
                &options.metainfo,
                session.allow_local_network,
            )?)
        } else {
            None
        };
        if options.settings.trackers {
            let identity = crate::engine::core::swarm_identity(&options.metainfo);
            for hash in identity
                .wire_keys()
                .filter(|hash| !options.metainfo.is_private() || *hash == identity.wire_key())
            {
                if let Some(announcer) = Announcer::new(
                    Announcer::metainfo(&options.metainfo),
                    AnnounceOptions {
                        hash,
                        peer: options.peer_id,
                        port: session.advertised_port.clone(),
                        left,
                        private: options.metainfo.is_private(),
                        allow_local_network: session.allow_local_network,
                        client: session.tracker_http.clone(),
                        udp: session.tracker_udp.clone(),
                        completed_announce: options
                            .completed_announces
                            .iter()
                            .find(|(key, _)| *key == hash)
                            .map(|(_, delivered)| *delivered),
                        checkpoint: {
                            let checkpoint = options.announce_checkpoint.clone();
                            Arc::new(move |delivered| checkpoint(hash, delivered))
                        },
                    },
                )? {
                    self.announces.push(announcer);
                }
            }
        }
        let (mut contacts, dials) = session.dial_queue.client(options.profile);
        contacts.add_direct(options.metainfo.info_hash().wire_key(), &options.peers)?;
        for &(hash, address) in &options.lsd_peers {
            contacts.add_lsd(hash, &[address])?;
        }
        self.contacts = Some(contacts);
        self.dials = Some(dials);
        Ok(())
    }
    pub(super) async fn update_counters(
        &mut self,
        transfer: &mut super::shell::Shell,
    ) -> io::Result<()> {
        let events = transfer.take_announces();
        for manager in &mut self.announces {
            manager.update_counters(super::observation::tracker_counters(transfer)?);
            for event in &events {
                manager.request(*event).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn shutdown(&mut self) -> io::Result<()> {
        self.lsd = None;
        self.dht.clear();
        let mut result = Ok(());
        for a in &mut self.announces {
            result = result.and(a.shutdown().await);
        }
        result.and(self.drain().await)
    }
    pub(super) async fn drain(&mut self) -> io::Result<()> {
        self.lsd = None;
        self.dht.clear();
        let mut announces = Ok(());
        for a in &mut self.announces {
            announces = announces.and(a.drain().await);
        }
        self.announces.clear();
        self.contacts = None;
        let dials = if let Some(d) = &mut self.dials {
            d.shutdown().await
        } else {
            Ok(())
        };
        self.dials = None;
        announces.and(dials)
    }
}
