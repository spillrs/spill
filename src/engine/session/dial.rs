//! Shared dial budget, fair torrent turns and owner-local contact priority.
use crate::engine::{
    bandwidth::Bucket,
    peer_io::{Stream, Transport},
    profile::Profile,
};
use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{net::TcpStream, sync::Notify, task::JoinSet, time::Instant};
use tracing::Instrument;

// Reuse the payload bucket's 16 KiB minimum burst: eight 2 KiB dial credits,
// replenished at four credits/second. Completed attempts retain pending permits.
const COST: u32 = 2048;
const PUNCH_LIFETIME: Duration = Duration::from_secs(3);
#[derive(Clone)]
pub(in crate::engine) struct Queue(Arc<Mutex<State>>);
struct State {
    allow_local_network: bool,
    encryption: super::PeerEncryption,
    utp: Option<Arc<Mutex<super::utp::Mux>>>,
    next: u64,
    entries: VecDeque<Entry>,
    bucket: Bucket,
    pending: usize,
    cap: usize,
    waiting: VecDeque<Weak<Notify>>,
}
pub(in crate::engine) struct Entry {
    owner: u64,
    pub(in crate::engine) address: SocketAddr,
    due: Instant,
    punch_until: Option<Instant>,
    restore: Option<&'static str>,
    pub(in crate::engine) source: &'static str,
    pub(in crate::engine) hash: [u8; 20],
}
impl State {
    fn refresh(&mut self) {
        self.bucket.refill(Instant::now().into_std());
        self.waiting.retain(|w| w.strong_count() != 0);
    }
    fn notify(&self) {
        if let Some(front) = self.waiting.front().and_then(Weak::upgrade) {
            front.notify_one();
        }
    }
}
impl Queue {
    pub(in crate::engine) fn new(
        cap: usize,
        allow_local_network: bool,
        encryption: super::PeerEncryption,
    ) -> Self {
        assert!(cap > 0);
        Self(Arc::new(Mutex::new(State {
            allow_local_network,
            encryption,
            utp: None,
            next: 0,
            entries: VecDeque::new(),
            bucket: Bucket::new(u64::from(COST) * 4, Instant::now().into_std()),
            pending: 0,
            cap,
            waiting: VecDeque::new(),
        })))
    }
    pub(super) fn snapshot(&self) -> (usize, usize, usize) {
        let state = self.0.lock().unwrap();
        (state.pending, state.cap, state.entries.len())
    }
    pub(super) fn set_utp(&self, mux: Option<Arc<Mutex<super::utp::Mux>>>) {
        self.0.lock().unwrap().utp = mux;
    }
    pub(in crate::engine) fn client(&self, profile: Profile) -> (Contacts, Client) {
        let mut state = self.0.lock().unwrap();
        state.next += 1;
        let contacts = Contacts {
            queue: self.clone(),
            owner: state.next,
            allow_local_network: state.allow_local_network,
            encryption: state.encryption,
            known: HashMap::new(),
            punches_queued: 0,
            punches_expired: 0,
        };
        let turn = Turn {
            queue: self.clone(),
            identity: Arc::new(Notify::new()),
        };
        (
            contacts,
            Client {
                peers: profile.peers,
                utp: state.utp.clone(),
                allow_local_network: state.allow_local_network,
                tasks: JoinSet::new(),
                turn,
                next: 0,
            },
        )
    }
}
#[derive(Clone)]
pub(in crate::engine) struct Turn {
    queue: Queue,
    identity: Arc<Notify>,
}
impl Turn {
    fn ready_in(&self, state: &mut State) -> bool {
        state.refresh();
        let identity = Arc::downgrade(&self.identity);
        if !state.waiting.iter().any(|w| w.ptr_eq(&identity)) {
            state.waiting.push_back(identity.clone());
        }
        state.pending < state.cap
            && state.bucket.delay(COST) == 0
            && state.waiting.front().is_some_and(|w| w.ptr_eq(&identity))
    }
    fn ready(&self) -> bool {
        self.ready_in(&mut self.queue.0.lock().unwrap())
    }
    fn acquire(&self) -> Option<Permit> {
        let mut state = self.queue.0.lock().unwrap();
        if !self.ready_in(&mut state) {
            return None;
        }
        state.waiting.pop_front();
        state.bucket.debit(COST);
        state.pending += 1;
        state.notify();
        Some(Permit(self.queue.clone()))
    }
    fn withdraw(&self) {
        let identity = Arc::downgrade(&self.identity);
        let mut state = self.queue.0.lock().unwrap();
        let front = state.waiting.front().is_some_and(|w| w.ptr_eq(&identity));
        state.waiting.retain(|w| !w.ptr_eq(&identity));
        if front {
            state.refresh();
            state.notify();
        }
    }
    pub(in crate::engine) async fn changed(&self) {
        let due = {
            let mut state = self.queue.0.lock().unwrap();
            state.refresh();
            let identity = Arc::downgrade(&self.identity);
            (state.pending < state.cap
                && state.waiting.front().is_some_and(|w| w.ptr_eq(&identity)))
            .then(|| Instant::now() + Duration::from_nanos(state.bucket.delay(COST)))
        };
        tokio::select! {
            _ = self.identity.notified() => {},
            _ = async { if let Some(due) = due { tokio::time::sleep_until(due).await; }
                else { std::future::pending::<()>().await; } } => {},
        }
    }
}
struct Permit(Queue);
impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.0.0.lock().unwrap();
        state.pending -= 1;
        state.refresh();
        state.notify();
    }
}
pub(in crate::engine) struct Contacts {
    queue: Queue,
    owner: u64,
    allow_local_network: bool,
    encryption: super::PeerEncryption,
    known: HashMap<SocketAddr, Known>,
    pub(in crate::engine) punches_queued: u64,
    pub(in crate::engine) punches_expired: u64,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ContactState {
    Queued,
    Negotiating,
    Idle,
}

pub(in crate::engine) enum NegotiationOutcome {
    Success,
    Closed,
    Failed,
}

struct Known {
    source: &'static str,
    hash: [u8; 20],
    utp_failed: bool,
    punch: bool,
    state: ContactState,
    encrypted: bool,
    retried: bool,
    transport: Option<Transport>,
}
impl Contacts {
    pub(in crate::engine) fn clear(&mut self) {
        self.queue
            .0
            .lock()
            .unwrap()
            .entries
            .retain(|entry| entry.owner != self.owner);
        self.known.clear();
    }
    pub(in crate::engine) fn try_utp(&self, address: SocketAddr) -> bool {
        let known = &self.known[&address];
        known.punch
            || known
                .transport
                .map_or(!known.utp_failed, |transport| transport == Transport::Utp)
    }
    pub(in crate::engine) fn fallback(&self, address: SocketAddr) -> bool {
        let known = &self.known[&address];
        !known.punch && !known.retried
    }
    pub(in crate::engine) fn methods(
        &self,
        address: SocketAddr,
    ) -> Option<crate::protocol::peer::mse::Methods> {
        self.known[&address]
            .encrypted
            .then(|| self.encryption.methods())
    }
    pub(in crate::engine) fn add(
        &mut self,
        hash: [u8; 20],
        peers: &[SocketAddr],
        source: &'static str,
    ) -> io::Result<()> {
        let mut state = self.queue.0.lock().unwrap();
        let punch = source == "holepunch";
        for &address in peers {
            if !crate::engine::network::peer(address, self.allow_local_network) {
                continue;
            }
            let address = crate::engine::network::canonical(address);
            if let Some(known) = self.known.get_mut(&address)
                && known.state != ContactState::Idle
            {
                if punch && !known.punch && known.state == ContactState::Queued {
                    if let Some(entry) = state
                        .entries
                        .iter_mut()
                        .find(|e| e.owner == self.owner && e.address == address)
                    {
                        entry.restore = Some(entry.source);
                        entry.source = "holepunch";
                        entry.punch_until = Some(Instant::now() + PUNCH_LIFETIME);
                        known.punch = true;
                        self.punches_queued += 1;
                    } else {
                        tracing::trace!("holepunch_dial_in_flight");
                    }
                }
                continue;
            }
            if !self.known.contains_key(&address) && self.known.len() == 4096 {
                let retired = self.known.iter().find_map(|(&address, known)| {
                    (known.state == ContactState::Idle).then_some(address)
                });
                let Some(retired) = retired else {
                    continue;
                };
                self.known.remove(&retired);
            }
            let known = self.known.entry(address).or_insert(Known {
                source,
                hash,
                utp_failed: false,
                punch: false,
                state: ContactState::Idle,
                encrypted: false,
                retried: false,
                transport: None,
            });
            if !punch {
                known.source = source;
                known.hash = hash;
            }
            known.punch = punch;
            self.punches_queued += u64::from(punch);
            known.state = ContactState::Queued;
            known.encrypted = self.encryption != super::PeerEncryption::Allowed;
            known.retried = false;
            known.transport = None;
            state.entries.push_back(Entry {
                owner: self.owner,
                address,
                due: Instant::now(),
                punch_until: punch.then(|| Instant::now() + PUNCH_LIFETIME),
                restore: None,
                source,
                hash: known.hash,
            });
        }
        Ok(())
    }
    pub(in crate::engine) fn add_direct(
        &mut self,
        hash: [u8; 20],
        peers: &[SocketAddr],
    ) -> io::Result<()> {
        self.add(hash, peers, "direct")
    }
    pub(in crate::engine) fn add_tracker(
        &mut self,
        hash: [u8; 20],
        peers: &[SocketAddr],
    ) -> io::Result<()> {
        self.add(hash, peers, "tracker")
    }
    pub(in crate::engine) fn add_dht(
        &mut self,
        hash: [u8; 20],
        peers: &[SocketAddr],
    ) -> io::Result<()> {
        self.add(hash, peers, "dht")
    }
    pub(in crate::engine) fn add_lsd(
        &mut self,
        hash: [u8; 20],
        peers: &[SocketAddr],
    ) -> io::Result<()> {
        self.add(hash, peers, "lsd")
    }
    pub(in crate::engine) fn add_pex(
        &mut self,
        hash: [u8; 20],
        peers: &[SocketAddr],
    ) -> io::Result<()> {
        self.add(hash, peers, "pex")
    }
    pub(in crate::engine) fn add_holepunch(
        &mut self,
        hash: [u8; 20],
        address: SocketAddr,
    ) -> io::Result<()> {
        self.expire_punches(Instant::now());
        self.add(hash, &[address], "holepunch")
    }
    fn expire_punches(&mut self, now: Instant) -> bool {
        if !self.known.values().any(|known| known.punch) {
            return false;
        }
        let mut state = self.queue.0.lock().unwrap();
        let mut due = false;
        state.entries.retain_mut(|entry| {
            if entry.owner != self.owner {
                return true;
            }
            let Some(until) = entry.punch_until else {
                return true;
            };
            if until > now {
                due = true;
                return true;
            }
            let known = self
                .known
                .get_mut(&entry.address)
                .expect("queued punch contact");
            known.punch = false;
            self.punches_expired += 1;
            entry.punch_until = None;
            if let Some(source) = entry.restore.take() {
                entry.source = source;
                return true;
            }
            if known.utp_failed {
                known.state = ContactState::Idle;
            } else {
                self.known.remove(&entry.address);
            }
            false
        });
        due
    }
    pub(in crate::engine) fn has_due_punch(&mut self) -> bool {
        self.expire_punches(Instant::now())
    }
    pub(in crate::engine) fn candidate(
        &mut self,
        rejects: impl Fn(SocketAddr, &'static str) -> bool,
        priority: impl Fn(SocketAddr) -> Option<u32>,
    ) -> Option<Entry> {
        let now = Instant::now();
        self.expire_punches(now);
        let mut state = self.queue.0.lock().unwrap();
        for punch in [true, false] {
            if let Some((i, _)) = state
                .entries
                .iter()
                .enumerate()
                .filter(|(_, entry)| {
                    entry.owner == self.owner
                        && (punch || entry.due <= now)
                        && (entry.source == "holepunch") == punch
                        && !rejects(entry.address, entry.source)
                })
                .max_by_key(|(i, entry)| {
                    (
                        if punch {
                            0
                        } else {
                            priority(entry.address).unwrap_or(0)
                        },
                        std::cmp::Reverse(*i),
                    )
                })
            {
                return Some(state.entries.remove(i).expect("selected dial entry"));
            }
        }
        None
    }
    pub(in crate::engine) fn dial_completed(
        &mut self,
        address: SocketAddr,
        transport: Option<Transport>,
        utp_failed: bool,
    ) {
        let known = self.known.get_mut(&address).expect("dialed contact");
        known.utp_failed |= utp_failed && !known.punch;
        known.retried |= known.punch;
        known.punch = false;
        if let Some(transport) = transport {
            known.state = ContactState::Negotiating;
            known.transport = Some(transport);
        } else {
            self.retire(address);
        }
    }
    pub(in crate::engine) fn negotiated(
        &mut self,
        address: SocketAddr,
        outcome: NegotiationOutcome,
    ) {
        let known = self.known.get_mut(&address).expect("negotiating contact");
        assert!(known.state == ContactState::Negotiating);
        if matches!(outcome, NegotiationOutcome::Closed)
            && !known.retried
            && self.encryption != super::PeerEncryption::Required
        {
            known.retried = true;
            known.encrypted = !known.encrypted;
            known.state = ContactState::Queued;
            self.queue.0.lock().unwrap().entries.push_back(Entry {
                owner: self.owner,
                address,
                due: Instant::now(),
                punch_until: None,
                restore: None,
                source: known.source,
                hash: known.hash,
            });
        } else {
            self.retire(address);
        }
    }
    fn retire(&mut self, address: SocketAddr) {
        let known = self.known.get_mut(&address).expect("completed contact");
        if known.source == "direct" {
            known.state = ContactState::Queued;
            known.encrypted = self.encryption != super::PeerEncryption::Allowed;
            known.retried = false;
            known.transport = None;
            self.queue.0.lock().unwrap().entries.push_back(Entry {
                owner: self.owner,
                address,
                due: Instant::now() + Duration::from_secs(30),
                punch_until: None,
                restore: None,
                source: "direct",
                hash: known.hash,
            });
        } else if known.utp_failed {
            known.state = ContactState::Idle;
        } else {
            self.known.remove(&address);
        }
    }
    pub(in crate::engine) fn progress(&self) -> usize {
        self.known
            .values()
            .filter(|k| k.state != ContactState::Idle)
            .count()
    }
}
impl Drop for Contacts {
    fn drop(&mut self) {
        self.queue
            .0
            .lock()
            .unwrap()
            .entries
            .retain(|e| e.owner != self.owner);
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(in crate::engine) struct DialId(u64);
pub(in crate::engine) struct ConnectionEvent {
    pub(in crate::engine) id: DialId,
    pub(in crate::engine) address: SocketAddr,
    pub(in crate::engine) result: io::Result<crate::engine::peer_io::Stream>,
    pub(in crate::engine) utp_failed: bool,
    pub(in crate::engine) methods: Option<crate::protocol::peer::mse::Methods>,
}
impl ConnectionEvent {
    pub(in crate::engine) fn transport(&self) -> Option<Transport> {
        self.result.as_ref().ok().map(Stream::transport)
    }
}
pub(in crate::engine) struct Client {
    peers: usize,
    utp: Option<Arc<Mutex<super::utp::Mux>>>,
    allow_local_network: bool,
    tasks: JoinSet<(ConnectionEvent, Permit)>,
    pub(in crate::engine) turn: Turn,
    next: u64,
}
impl Client {
    pub(in crate::engine) fn ready(&self) -> bool {
        self.tasks.len() < self.peers && self.turn.ready()
    }
    pub(in crate::engine) fn withdraw(&self) {
        self.turn.withdraw();
    }
    pub(in crate::engine) fn has_work(&self) -> bool {
        !self.tasks.is_empty()
    }
    pub(in crate::engine) fn transport(&self, try_utp: bool) -> &'static str {
        if try_utp && self.utp.is_some() {
            "utp"
        } else {
            "tcp"
        }
    }
    pub(in crate::engine) fn admit(
        &mut self,
        address: SocketAddr,
        try_utp: bool,
        fallback: bool,
        methods: Option<crate::protocol::peer::mse::Methods>,
    ) -> io::Result<DialId> {
        if self.tasks.len() >= self.peers
            || !crate::engine::network::peer(address, self.allow_local_network)
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let permit = self.turn.acquire().ok_or(io::ErrorKind::WouldBlock)?;
        self.next += 1;
        let id = DialId(self.next);
        let utp = self.utp.clone().filter(|_| try_utp);
        let span = tracing::trace_span!("dial", dial=?id);
        let dial = async move {
            let mut utp_failed = false;
            let result = async {
                if let Some(mux) = utp {
                    match super::utp::Mux::connect(mux, address).await {
                        Ok(stream) => return Ok(crate::engine::peer_io::Stream::Utp(stream)),
                        Err(error) => {
                            tracing::trace!(fallback,kind=?error.kind(),%error,"utp_dial_failed");
                            utp_failed = matches!(
                                error.kind(),
                                io::ErrorKind::TimedOut | io::ErrorKind::ConnectionReset
                            );
                            if !fallback {
                                return Err(error);
                            }
                        }
                    }
                }
                tokio::time::timeout(
                    crate::engine::profile::CONNECT_TIMEOUT,
                    TcpStream::connect(address),
                )
                .await
                .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
                .map(crate::engine::peer_io::Stream::Tcp)
            }
            .await;
            (
                ConnectionEvent {
                    id,
                    address,
                    result,
                    utp_failed,
                    methods,
                },
                permit,
            )
        };
        self.tasks.spawn(dial.instrument(span));
        Ok(id)
    }
    pub(in crate::engine) async fn next(&mut self) -> io::Result<Option<ConnectionEvent>> {
        self.tasks
            .join_next()
            .await
            .transpose()
            .map(|event| event.map(|(event, _permit)| event))
            .map_err(io::Error::other)
    }
    pub(in crate::engine) async fn shutdown(&mut self) -> io::Result<()> {
        self.withdraw();
        self.tasks.abort_all();
        let mut failure = None;
        while let Some(result) = self.tasks.join_next().await {
            if let Err(error) = result
                && !error.is_cancelled()
            {
                failure.get_or_insert_with(|| io::Error::other(error));
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.withdraw();
    }
}
