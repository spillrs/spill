//! Session-owned uTP multiplexer on the tracker/DHT UDP socket.
mod connection;
mod stream;
use super::{HandshakePermit, Pending};
use connection::Connection;
use protocol::utp::{Header, Kind, Packet};
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    task::Waker,
    time::{Duration, Instant},
};
pub(crate) use stream::Stream;
use tokio::{
    net::UdpSocket,
    sync::{Notify, mpsc, watch},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Key {
    address: SocketAddr,
    id: u16,
}
struct Entry {
    connection: Connection,
    syn: Option<(u16, u16)>,
    reader: Option<Waker>,
    writer: Option<Waker>,
    connector: Option<Waker>,
    owned: bool,
}
pub(crate) struct Mux {
    socket: Arc<UdpSocket>,
    ipv6: bool,
    timer: Option<Instant>,
    entries: HashMap<Key, Entry>,
    epoch: Instant,
    changed: Arc<Notify>,
    incoming: mpsc::Sender<(Stream, HandshakePermit)>,
    pending: Pending,
    cap: usize,
    enabled: bool,
    accepting: bool,
}
impl Mux {
    pub(super) fn new(
        socket: Arc<UdpSocket>,
        pending: Pending,
        cap: usize,
        enabled: bool,
    ) -> (Arc<Mutex<Self>>, mpsc::Receiver<(Stream, HandshakePermit)>) {
        let (incoming, receiver) = mpsc::channel(cap);
        (
            Arc::new(Mutex::new(Self {
                ipv6: socket.local_addr().expect("bound UDP socket").is_ipv6(),
                socket,
                timer: None,
                entries: HashMap::new(),
                epoch: Instant::now(),
                changed: Arc::new(Notify::new()),
                incoming,
                pending,
                cap,
                enabled,
                accepting: true,
            })),
            receiver,
        )
    }
    fn stamp(&self, now: Instant) -> u32 {
        now.duration_since(self.epoch).as_micros() as u32
    }
    fn random() -> io::Result<u16> {
        let mut bytes = [0; 2];
        getrandom::getrandom(&mut bytes).map_err(io::Error::other)?;
        Ok(u16::from_ne_bytes(bytes))
    }
    fn send(&self, mut address: SocketAddr, header: Header, sack: &[u8], payload: &[u8]) {
        if self.ipv6
            && let SocketAddr::V4(v4) = address
        {
            address = SocketAddr::new(v4.ip().to_ipv6_mapped().into(), v4.port());
        }
        let mut bytes = Vec::with_capacity(20 + payload.len());
        Packet {
            header,
            sack: (!sack.is_empty()).then_some(sack),
            payload,
        }
        .encode(&mut bytes);
        if let Err(error) = self.socket.try_send_to(&bytes, address)
            && error.kind() != io::ErrorKind::WouldBlock
        {
            tracing::debug!(%error, "utp_send_failed");
        }
    }
    fn reset(&self, address: SocketAddr, id: u16, seq: u16, ack: u16, now: Instant) {
        self.send(
            address,
            Header {
                kind: Kind::Reset,
                connection_id: id,
                timestamp_us: self.stamp(now),
                timestamp_diff_us: 0,
                wnd_size: 0,
                seq_nr: seq,
                ack_nr: ack,
            },
            &[],
            &[],
        );
    }
    fn flush(&mut self, key: Key) {
        let entry = self.entries.get_mut(&key).expect("uTP connection");
        if let Some(w) = entry.reader.take() {
            w.wake();
        }
        if let Some(w) = entry.writer.take() {
            w.wake();
        }
        if let Some(w) = entry.connector.take() {
            w.wake();
        }
        self.output(key);
    }
    fn output(&mut self, key: Key) {
        let out = self
            .entries
            .get_mut(&key)
            .expect("uTP connection")
            .connection
            .take_output();
        for packet in out {
            self.send(key.address, packet.header, &packet.sack, &packet.payload);
        }
        let deadline = self.entries[&key].connection.deadline();
        if deadline.is_some_and(|at| self.timer.is_none_or(|timer| at < timer)) {
            self.timer = deadline;
            self.changed.notify_one();
        }
    }
    fn reap(&mut self, key: Key) {
        let entry = &self.entries[&key];
        if !entry.owned && (entry.connection.failed || entry.connection.finished) {
            self.entries.remove(&key);
            self.changed.notify_one();
        }
    }
    pub(super) fn receive(
        handle: &Arc<Mutex<Self>>,
        bytes: &[u8],
        address: SocketAddr,
        now: Instant,
    ) {
        let Ok(packet) = Packet::decode(bytes) else {
            return;
        };
        let mut mux = handle.lock().unwrap();
        let stamp = mux.stamp(now);
        let h = packet.header;
        if h.kind == Kind::Syn {
            let key = Key {
                address,
                id: h.connection_id.wrapping_add(1),
            };
            if let Some(entry) = mux.entries.get_mut(&key) {
                if entry.syn == Some((h.connection_id, h.seq_nr)) {
                    entry.connection.reply_syn(stamp, h.timestamp_us);
                    mux.flush(key);
                } else {
                    mux.reset(address, h.connection_id, 0, h.seq_nr, now);
                }
                return;
            }
            if !mux.enabled || !mux.accepting || mux.entries.len() >= mux.cap {
                mux.reset(address, h.connection_id, 0, h.seq_nr, now);
                return;
            }
            let Some(permit) = mux.pending.acquire(address.ip()) else {
                mux.reset(address, h.connection_id, 0, h.seq_nr, now);
                return;
            };
            let seq = match Self::random() {
                Ok(seq) => seq,
                Err(error) => {
                    tracing::warn!(%error, "utp_entropy_failed");
                    return;
                }
            };
            let mut connection = Connection::incoming(now, h.connection_id, seq, h.seq_nr);
            connection.reply_syn(stamp, h.timestamp_us);
            mux.entries.insert(
                key,
                Entry {
                    connection,
                    syn: Some((h.connection_id, h.seq_nr)),
                    reader: None,
                    writer: None,
                    connector: None,
                    owned: true,
                },
            );
            mux.flush(key);
            let stream = Stream::new(handle.clone(), key);
            // Capacity follows the shared pending cap, so only a stopped router can reject this.
            if let Err(error) = mux.incoming.try_send((stream, permit)) {
                mux.entries.remove(&key);
                mux.reset(address, h.connection_id, seq, h.seq_nr, now);
                // Dropping a stream takes the mux lock.
                drop(mux);
                drop(error);
            }
            return;
        }
        let mut key = Key {
            address,
            id: h.connection_id,
        };
        // A RESET may echo the unknown packet's ID instead of using our receive ID.
        if h.kind == Kind::Reset && !mux.entries.contains_key(&key) {
            for id in [
                h.connection_id.wrapping_add(1),
                h.connection_id.wrapping_sub(1),
            ] {
                let candidate = Key { address, id };
                if mux
                    .entries
                    .get(&candidate)
                    .is_some_and(|e| e.connection.send_id() == h.connection_id)
                {
                    key = candidate;
                    break;
                }
            }
        }
        if let Some(entry) = mux.entries.get_mut(&key) {
            entry.connection.on_packet(now, stamp, packet);
            mux.flush(key);
            mux.reap(key);
        } else if matches!(h.kind, Kind::Data | Kind::Fin) {
            mux.reset(address, h.connection_id, 0, h.seq_nr, now);
        }
    }
    pub(crate) fn stop_accepting(&mut self) {
        self.accepting = false;
    }
    pub(crate) async fn connect(
        handle: Arc<Mutex<Self>>,
        address: SocketAddr,
    ) -> io::Result<Stream> {
        let key = {
            let mut mux = handle.lock().unwrap();
            if !mux.enabled || mux.entries.len() >= mux.cap {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let key = loop {
                let id = Self::random()?;
                let key = Key {
                    address: SocketAddr::new(address.ip().to_canonical(), address.port()),
                    id,
                };
                // Reserve both directions so incoming SYN lookup cannot alias an outgoing connection.
                if !mux.entries.contains_key(&key)
                    && !mux.entries.contains_key(&Key {
                        address: key.address,
                        id: id.wrapping_add(1),
                    })
                {
                    break key;
                }
            };
            let now = Instant::now();
            let connection = Connection::outgoing(now, mux.stamp(now), key.id, Self::random()?);
            mux.entries.insert(
                key,
                Entry {
                    connection,
                    syn: None,
                    reader: None,
                    writer: None,
                    connector: None,
                    owned: true,
                },
            );
            mux.flush(key);
            key
        };
        let stream = Stream::new(handle.clone(), key);
        tokio::time::timeout(
            Duration::from_secs(3),
            std::future::poll_fn(|cx| {
                let mut mux = handle.lock().unwrap();
                let entry = mux.entries.get_mut(&key).expect("owned uTP connection");
                if entry.connection.failed {
                    return std::task::Poll::Ready(Err(io::Error::from(
                        io::ErrorKind::ConnectionReset,
                    )));
                }
                if entry.connection.connected {
                    return std::task::Poll::Ready(Ok(()));
                }
                entry.connector = Some(cx.waker().clone());
                std::task::Poll::Pending
            }),
        )
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
        Ok(stream)
    }
}
pub(super) async fn run(handle: Arc<Mutex<Mux>>, mut stop: watch::Receiver<bool>) {
    let mut drain_until = None;
    loop {
        let (deadline, changed) = {
            let mut mux = handle.lock().unwrap();
            let now = Instant::now();
            let stamp = mux.stamp(now);
            if *stop.borrow() && drain_until.is_none() {
                drain_until = Some(now + Duration::from_secs(1));
            }
            let keys: Vec<_> = mux
                .entries
                .iter()
                .filter(|(_, e)| {
                    !e.connection.failed
                        && !e.connection.finished
                        && e.connection.deadline().is_some_and(|at| at <= now)
                })
                .map(|(&key, _)| key)
                .collect();
            for key in keys {
                mux.entries
                    .get_mut(&key)
                    .expect("uTP timer entry")
                    .connection
                    .on_timeout(now, stamp);
                mux.flush(key);
            }
            mux.entries
                .retain(|_, e| e.owned || (!e.connection.failed && !e.connection.finished));
            if drain_until.is_some_and(|at| now >= at || mux.entries.is_empty()) {
                break;
            }
            mux.timer = mux
                .entries
                .values()
                .filter_map(|e| e.connection.deadline())
                .chain(drain_until)
                .min();
            (mux.timer, mux.changed.clone())
        };
        tokio::select! {
            biased;
            _ = stop.changed(), if drain_until.is_none() => {},
            _ = changed.notified() => {},
            _ = async { if let Some(at) = deadline { tokio::time::sleep_until(at.into()).await; }
                else { std::future::pending::<()>().await; } } => {},
        }
    }
    let mut mux = handle.lock().unwrap();
    let stamp = mux.stamp(Instant::now());
    let keys: Vec<_> = mux.entries.keys().copied().collect();
    for key in keys {
        if !mux.entries[&key].connection.failed && !mux.entries[&key].connection.finished {
            mux.entries
                .get_mut(&key)
                .expect("uTP stop entry")
                .connection
                .abort(stamp);
        }
        mux.entries
            .get_mut(&key)
            .expect("uTP stop entry")
            .connection
            .failed = true;
        mux.flush(key);
    }
}
