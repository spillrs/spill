use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot, watch},
};

#[derive(Clone)]
pub(in crate::engine) struct Transport {
    pub(in crate::engine) socket: Arc<UdpSocket>,
    pub(super) routes: Arc<Mutex<Routes>>,
    pub(super) utp: Option<Arc<Mutex<super::utp::Mux>>>,
}
#[derive(Default)]
pub(super) struct Routes {
    waiting: HashMap<(u32, SocketAddr), Waiting>,
    pub(super) dht: Option<mpsc::Sender<(Vec<u8>, SocketAddr)>>,
}
struct Waiting {
    token: Arc<()>,
    sender: oneshot::Sender<Vec<u8>>,
}
pub(in crate::engine) struct Registration {
    transport: Transport,
    key: (u32, SocketAddr),
    token: Arc<()>,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut routes = self.transport.routes.lock().unwrap();
        let waiting = &mut routes.waiting;
        // A delivered transaction's ID may already have been reused.
        if waiting
            .get(&self.key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.token, &self.token))
        {
            waiting.remove(&self.key);
        }
    }
}
impl Transport {
    pub(super) fn bind(address: SocketAddr) -> io::Result<Self> {
        let socket = socket2::Socket::new(
            socket2::Domain::for_address(address),
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;
        if address.is_ipv6() {
            socket.set_only_v6(!address.ip().is_unspecified())?;
        }
        // The default kernel buffer can be smaller than one uTP receive window
        // once per-datagram kernel overhead is included.
        socket.set_recv_buffer_size(2 * 1024 * 1024)?;
        socket.set_nonblocking(true)?;
        socket.bind(&address.into())?;
        Ok(Self {
            socket: Arc::new(UdpSocket::from_std(socket.into())?),
            routes: Arc::default(),
            utp: None,
        })
    }
    pub(in crate::engine) fn register(
        &self,
        endpoint: SocketAddr,
    ) -> io::Result<(u32, Registration, oneshot::Receiver<Vec<u8>>)> {
        let mut routes = self.routes.lock().unwrap();
        let waiting = &mut routes.waiting;
        loop {
            let mut bytes = [0; 4];
            getrandom::fill(&mut bytes).map_err(io::Error::other)?;
            let id = u32::from_ne_bytes(bytes);
            let key = (id, endpoint);
            if let std::collections::hash_map::Entry::Vacant(entry) = waiting.entry(key) {
                let (sender, receiver) = oneshot::channel();
                let token = Arc::new(());
                entry.insert(Waiting {
                    token: token.clone(),
                    sender,
                });
                return Ok((
                    id,
                    Registration {
                        transport: self.clone(),
                        key,
                        token,
                    },
                    receiver,
                ));
            }
        }
    }
    fn route(&self, bytes: &[u8], source: SocketAddr) {
        let source = SocketAddr::new(source.ip().to_canonical(), source.port());
        if bytes.first() == Some(&b'd') {
            if bytes.len() <= crate::engine::dht::DATAGRAM
                && let Some(sender) = self.routes.lock().unwrap().dht.as_ref()
            {
                let _ = sender.try_send((bytes.to_vec(), source));
            }
        } else if bytes.len() >= 20 && bytes[0] & 0x0f == 1 && bytes[0] >> 4 <= 4 {
            if let Some(mux) = &self.utp {
                super::utp::Mux::receive(mux, bytes, source, std::time::Instant::now());
            }
        } else if bytes.len() >= 8 {
            let id = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
            if let Some(entry) = self.routes.lock().unwrap().waiting.remove(&(id, source)) {
                let _ = entry.sender.send(bytes.to_vec());
            }
        }
    }
    pub(super) async fn run(self, mut stop: watch::Receiver<bool>) -> io::Result<()> {
        let mut bytes = vec![0; 65_536];
        loop {
            tokio::select! {
                biased;
                _ = stop.changed() => return Ok(()),
                received = self.socket.recv_from(&mut bytes) => {
                    match received {
                        Ok((length, source)) => self.route(&bytes[..length], source),
                        Err(error) => tracing::warn!(%error, "session UDP receive failed"),
                    }
                }
            }
        }
    }
}
