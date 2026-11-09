//! Session-wide BEP 14 sockets and bounded delivery to active swarms.
use crate::protocol::{identity::TorrentIdentity, lsd};
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{Notify, mpsc, watch},
    time::Instant,
};

const V4: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
const V6: Ipv6Addr = Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0xefc0, 0x988f);
const INTERVAL: Duration = Duration::from_secs(300);
const RATE: Duration = Duration::from_secs(60);
const BATCH: usize = 20;

struct Entry {
    identity: TorrentIdentity,
    sender: mpsc::Sender<([u8; 20], SocketAddr)>,
    due: Instant,
    announced: bool,
}
#[derive(Clone, Default)]
pub(in crate::engine) struct Handle {
    entries: Arc<Mutex<HashMap<[u8; 20], Entry>>>,
    changed: Arc<Notify>,
}
pub(in crate::engine) struct Registration {
    handle: Handle,
    key: [u8; 20],
    incoming: mpsc::Receiver<([u8; 20], SocketAddr)>,
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.handle
            .entries
            .lock()
            .unwrap()
            .remove(&self.key)
            .expect("registered LSD swarm");
        self.handle.changed.notify_one();
    }
}
impl Registration {
    pub(in crate::engine) async fn next(&mut self) -> ([u8; 20], SocketAddr) {
        self.incoming
            .recv()
            .await
            .expect("session owns LSD registration")
    }
}
impl Handle {
    pub(in crate::engine) fn register(&self, identity: TorrentIdentity) -> Registration {
        let key = identity.wire_key();
        let (sender, incoming) = mpsc::channel(crate::engine::profile::MAX_QUEUED);
        let previous = self.entries.lock().unwrap().insert(
            key,
            Entry {
                identity,
                sender,
                due: Instant::now(),
                announced: false,
            },
        );
        assert!(previous.is_none(), "duplicate LSD swarm");
        self.changed.notify_one();
        Registration {
            handle: self.clone(),
            key,
            incoming,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Interface {
    address: IpAddr,
    index: u32,
}
fn interfaces(listen: IpAddr, families: [bool; 2]) -> io::Result<Vec<Interface>> {
    use nix::net::if_::{InterfaceFlags, if_nametoindex};
    let mut result = Vec::new();
    for item in nix::ifaddrs::getifaddrs()? {
        if !item.flags.contains(InterfaceFlags::IFF_UP) {
            continue;
        }
        let Some(address) = item.address else {
            continue;
        };
        let address = if let Some(v4) = address.as_sockaddr_in().filter(|_| families[0]) {
            IpAddr::V4(v4.ip())
        } else if let Some(v6) = address.as_sockaddr_in6().filter(|_| families[1]) {
            IpAddr::V6(v6.ip())
        } else {
            continue;
        };
        if if listen.is_unspecified() {
            item.flags
                .intersects(InterfaceFlags::IFF_LOOPBACK | InterfaceFlags::IFF_POINTOPOINT)
                || !item.flags.contains(InterfaceFlags::IFF_MULTICAST)
        } else {
            address != listen
        } {
            continue;
        }
        // An interface can disappear after getifaddrs returns.
        let index = match if_nametoindex(item.interface_name.as_str()) {
            Ok(index) => index,
            Err(nix::errno::Errno::ENODEV | nix::errno::Errno::ENXIO) => continue,
            Err(error) => return Err(error.into()),
        };
        // One membership per interface and family, even with several local addresses.
        if result
            .iter()
            .any(|i: &Interface| i.index == index && i.address.is_ipv6() == address.is_ipv6())
        {
            continue;
        }
        result.push(Interface { address, index });
    }
    result.sort_unstable();
    Ok(result)
}

pub(super) struct Service {
    handle: Handle,
    sockets: Vec<(UdpSocket, Vec<Interface>)>,
    interfaces: Vec<Interface>,
    rebuild: bool,
    checks: tokio::time::Interval,
    listen: SocketAddr,
    families: [bool; 2],
    status: watch::Sender<super::LsdStatus>,
    cookie: String,
}
impl Service {
    pub(super) async fn new(
        listen: SocketAddr,
        families: [bool; 2],
        status: watch::Sender<super::LsdStatus>,
    ) -> io::Result<Self> {
        let mut cookie = [0; 8];
        getrandom::fill(&mut cookie).map_err(io::Error::other)?;
        let mut service = Self {
            handle: Handle::default(),
            sockets: Vec::new(),
            interfaces: Vec::new(),
            rebuild: true,
            checks: tokio::time::interval_at(Instant::now() + RATE, RATE),
            listen,
            families,
            status,
            cookie: format!("{:016x}", u64::from_ne_bytes(cookie)),
        };
        service
            .checks
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        service.refresh().await;
        Ok(service)
    }
    fn sockets(interfaces: &[Interface]) -> io::Result<Vec<(std::net::UdpSocket, Vec<Interface>)>> {
        let mut sockets = Vec::new();
        let mut last_error = None;
        for ipv6 in [false, true] {
            let selected: Vec<_> = interfaces
                .iter()
                .filter(|i| i.address.is_ipv6() == ipv6)
                .copied()
                .collect();
            if selected.is_empty() {
                continue;
            }
            let domain = if ipv6 {
                socket2::Domain::IPV6
            } else {
                socket2::Domain::IPV4
            };
            let setup = (|| {
                let socket = socket2::Socket::new(
                    domain,
                    socket2::Type::DGRAM,
                    Some(socket2::Protocol::UDP),
                )?;
                socket.set_reuse_address(true)?;
                #[cfg(target_os = "macos")]
                socket.set_reuse_port(true)?;
                if ipv6 {
                    socket.set_only_v6(true)?;
                    socket.set_multicast_hops_v6(1)?;
                    socket.set_multicast_loop_v6(true)?;
                    #[cfg(target_os = "linux")]
                    socket.set_multicast_all_v6(false)?;
                    socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, 6771)).into())?;
                } else {
                    socket.set_multicast_ttl_v4(1)?;
                    socket.set_multicast_loop_v4(true)?;
                    // Darwin cannot send from a socket bound to the multicast group.
                    // Elsewhere the group bind excludes unrelated groups sharing 6771.
                    let bind = if cfg!(target_os = "macos") {
                        Ipv4Addr::UNSPECIFIED
                    } else {
                        V4
                    };
                    socket.bind(&SocketAddr::from((bind, 6771)).into())?;
                    #[cfg(target_os = "linux")]
                    socket.set_multicast_all_v4(false)?;
                }
                socket.set_nonblocking(true)?;
                Ok::<_, io::Error>(socket)
            })();
            let socket = match setup {
                Ok(socket) => socket,
                Err(error) => {
                    tracing::warn!(ipv6, %error, "lsd_bind_failed");
                    last_error = Some(error);
                    continue;
                }
            };
            let joined = selected.into_iter().filter(|interface| {
                let result = match interface.address {
                    IpAddr::V4(address) => socket.join_multicast_v4(&V4, &address),
                    IpAddr::V6(_) => socket.join_multicast_v6(&V6, interface.index),
                };
                match result {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::warn!(interface = interface.index, address = %interface.address, %error, "lsd_join_failed");
                        last_error = Some(error);
                        false
                    }
                }
            }).collect::<Vec<_>>();
            if !joined.is_empty() {
                sockets.push((socket.into(), joined));
            }
        }
        if sockets.is_empty() {
            return Err(last_error.unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    "no LSD multicast interfaces",
                )
            }));
        }
        Ok(sockets)
    }
    pub(super) fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn error(&self, operation: &str, error: &io::Error) {
        tracing::warn!(operation, %error, "lsd_failed");
        self.status
            .send_modify(|status| status.last_error = Some(format!("{operation}: {error}")));
    }
    async fn refresh(&mut self) {
        let listen = self.listen;
        let families = self.families;
        let previous = self.interfaces.clone();
        let rebuild = self.rebuild;
        let setup = tokio::task::spawn_blocking(move || {
            let selected = interfaces(listen.ip(), families)?;
            if !rebuild && selected == previous {
                return Ok(None);
            }
            Self::sockets(&selected).map(|sockets| Some((selected, sockets)))
        })
        .await
        .expect("LSD socket setup task");
        let setup = setup.and_then(|setup| {
            setup
                .map(|(selected, sockets)| {
                    let sockets = sockets
                        .into_iter()
                        .map(|(socket, interfaces)| {
                            UdpSocket::from_std(socket).map(|socket| (socket, interfaces))
                        })
                        .collect::<io::Result<Vec<_>>>()?;
                    Ok((selected, sockets))
                })
                .transpose()
        });
        let was_active = !self.sockets.is_empty();
        match setup {
            Ok(None) => return,
            Ok(Some((selected, sockets))) => {
                self.rebuild = sockets
                    .iter()
                    .map(|(_, joined)| joined.len())
                    .sum::<usize>()
                    != selected.len();
                self.interfaces = selected;
                self.sockets = sockets;
            }
            Err(error) => {
                self.sockets.clear();
                self.rebuild = true;
                self.error("bind/join", &error);
            }
        }
        let active = !self.sockets.is_empty();
        if active && !was_active {
            for entry in self.handle.entries.lock().unwrap().values_mut() {
                entry.due = Instant::now();
            }
        }
        self.status.send_modify(|status| status.active = active);
    }
    pub(super) async fn run(mut self, mut stop: watch::Receiver<bool>) {
        let mut allowed = Instant::now();
        loop {
            let due = self
                .handle
                .entries
                .lock()
                .unwrap()
                .values()
                .map(|e| {
                    if e.announced {
                        e.due.max(allowed)
                    } else {
                        e.due
                    }
                })
                .min();
            let timer = async {
                if let Some(due) = due.filter(|_| {
                    self.sockets
                        .iter()
                        .any(|(_, interfaces)| !interfaces.is_empty())
                }) {
                    tokio::time::sleep_until(due).await;
                } else {
                    std::future::pending().await
                }
            };
            tokio::select! {
                biased;
                _ = stop.changed() => {
                    self.status.send_modify(|status| status.active = false);
                    return;
                },
                _ = self.checks.tick() => self.refresh().await,
                _ = timer => {
                    let now = Instant::now();
                    let hashes = {
                        let mut entries = self.handle.entries.lock().unwrap();
                        let mut due: Vec<_> = entries.iter_mut().filter(|(_, e)| e.due <= now && (!e.announced || allowed <= now)).collect();
                        due.sort_by_key(|(_, e)| e.due);
                        due.into_iter().take(BATCH).map(|(&hash, e)| { e.due = now + INTERVAL; hash }).collect::<Vec<_>>()
                    };
                    if hashes.is_empty() { continue; }
                    let mut succeeded = false;
                    let mut failed = Vec::new();
                    for (socket, interfaces) in &self.sockets {
                        let Some(first) = interfaces.first() else { continue; };
                        let ipv6 = first.address.is_ipv6();
                        let packet = lsd::encode(ipv6, self.listen.port(), &hashes, &self.cookie);
                        for interface in interfaces {
                            let sent = async {
                                let options = socket2::SockRef::from(socket);
                                let destination = match interface.address {
                                    IpAddr::V4(address) => {
                                        options.set_multicast_if_v4(&address)?;
                                        SocketAddr::from((V4, 6771))
                                    }
                                    IpAddr::V6(_) => {
                                        options.set_multicast_if_v6(interface.index)?;
                                        SocketAddr::V6(SocketAddrV6::new(V6, 6771, 0, interface.index))
                                    }
                                };
                                if let (SocketAddr::V6(listen), SocketAddr::V6(destination)) = (self.listen, destination)
                                    && !listen.ip().is_unspecified()
                                {
                                    use nix::sys::socket::{ControlMessage, MsgFlags, SockaddrIn6, sendmsg};
                                    use std::os::fd::AsRawFd;
                                    // An interface can have several IPv6 addresses; replies must
                                    // reach the address our peer listener actually bound.
                                    let source = libc::in6_pktinfo {
                                        ipi6_addr: libc::in6_addr { s6_addr: listen.ip().octets() },
                                        ipi6_ifindex: interface.index,
                                    };
                                    return socket.async_io(tokio::io::Interest::WRITABLE, || {
                                        sendmsg(socket.as_raw_fd(), &[io::IoSlice::new(&packet)],
                                            &[ControlMessage::Ipv6PacketInfo(&source)], MsgFlags::empty(),
                                            Some(&SockaddrIn6::from(destination))).map_err(io::Error::from)
                                    }).await;
                                }
                                socket.send_to(&packet, destination).await
                            }.await;
                            match sent {
                                Ok(_) => succeeded = true,
                                Err(error) => {
                                    failed.push(*interface);
                                    self.error(&format!("send on interface {}", interface.index), &error);
                                },
                            }
                        }
                    }
                    if !failed.is_empty() {
                        self.rebuild = true;
                        for (_, interfaces) in &mut self.sockets {
                            interfaces.retain(|interface| !failed.contains(interface));
                        }
                    }
                    let mut entries = self.handle.entries.lock().unwrap();
                    for hash in hashes {
                        if let Some(entry) = entries.get_mut(&hash) {
                            if succeeded { entry.announced = true; }
                            else { entry.due = now + RATE; }
                        }
                    }
                    if succeeded {
                        self.status.send_modify(|status| {
                            status.last_error = None;
                            status.announces += 1;
                        });
                    }
                    allowed = now + RATE;
                },
                _ = self.handle.changed.notified() => {},
                received = crate::engine::next_ready(self.sockets.iter().map(|(socket, _)| async move {
                    let mut bytes = [0; lsd::MAX_PACKET + 1];
                    match socket.recv_from(&mut bytes).await {
                        Ok((length, source)) => Ok((bytes, length, source)),
                        Err(error) => {
                            // Persistent socket errors must not create a hot receive loop.
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            Err(error)
                        }
                    }
                })) => {
                    let (bytes, length, mut source) = match received {
                        Ok(packet) => packet,
                        Err(error) => { self.rebuild = true; self.error("receive", &error); continue; },
                    };
                    let Some(announce) = lsd::decode(&bytes[..length]) else { continue; };
                    if announce.cookie == Some(self.cookie.as_str()) { continue; }
                    source.set_port(announce.port);
                    if !crate::engine::network::peer(source, true) { continue; }
                    self.status.send_modify(|status| status.received += 1);
                    let entries = self.handle.entries.lock().unwrap();
                    for hash in announce.hashes {
                        for entry in entries.values().filter(|e| e.identity.wire_keys().any(|key| key == hash)) {
                            let _ = entry.sender.try_send((hash, source));
                        }
                    }
                }
            }
        }
    }
}
