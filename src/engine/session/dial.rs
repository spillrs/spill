//! One FIFO contact queue and token bucket shared by every session owner.
use crate::engine::{bandwidth::Bucket, profile::Profile};
use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{net::TcpStream, sync::Notify, task::JoinSet, time::Instant};

// Reuse the payload bucket's 16 KiB minimum burst: eight 2 KiB dial credits,
// replenished at four credits/second. Completed attempts retain pending permits.
const COST: u32 = 2048;
#[derive(Clone)]
pub(in crate::engine) struct Queue(Arc<Mutex<State>>);
struct State {
    allow_local_network: bool,
    utp: Option<Arc<Mutex<super::utp::Mux>>>,
    next: u64,
    entries: VecDeque<Entry>,
    bucket: Bucket,
    pending: usize,
    cap: usize,
    waiting: VecDeque<Weak<Notify>>,
}
struct Entry {
    owner: u64,
    address: SocketAddr,
    due: Instant,
    source: &'static str,
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
    pub(in crate::engine) fn new(cap: usize, allow_local_network: bool) -> Self {
        assert!(cap > 0);
        Self(Arc::new(Mutex::new(State {
            allow_local_network,
            utp: None,
            next: 0,
            entries: VecDeque::new(),
            bucket: Bucket::new(u64::from(COST) * 4, Instant::now().into_std()),
            pending: 0,
            cap,
            waiting: VecDeque::new(),
        })))
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
            known: HashMap::new(),
            selected_source: "tracker",
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
    known: HashMap<SocketAddr, Known>,
    pub(in crate::engine) selected_source: &'static str,
}
struct Known {
    source: &'static str,
    utp_failed: bool,
    queued: bool,
}
impl Contacts {
    pub(in crate::engine) fn try_utp(&self, address: SocketAddr) -> bool {
        !self.known[&address].utp_failed
    }
    fn add(&mut self, peers: &[SocketAddr], source: &'static str) -> io::Result<()> {
        let mut state = self.queue.0.lock().unwrap();
        for &address in peers {
            if !crate::engine::network::peer(address, self.allow_local_network)
                || self.known.get(&address).is_some_and(|k| k.queued)
            {
                continue;
            }
            if !self.known.contains_key(&address) && self.known.len() == 4096 {
                let retired = self
                    .known
                    .iter()
                    .find_map(|(&address, known)| (!known.queued).then_some(address));
                let Some(retired) = retired else {
                    continue;
                };
                self.known.remove(&retired);
            }
            let known = self.known.entry(address).or_insert(Known {
                source,
                utp_failed: false,
                queued: false,
            });
            known.source = source;
            known.queued = true;
            state.entries.push_back(Entry {
                owner: self.owner,
                address,
                due: Instant::now(),
                source,
            });
        }
        Ok(())
    }
    pub(in crate::engine) fn add_direct(&mut self, peers: &[SocketAddr]) -> io::Result<()> {
        self.add(peers, "direct")
    }
    pub(in crate::engine) fn add_tracker(&mut self, peers: &[SocketAddr]) -> io::Result<()> {
        self.add(peers, "tracker")
    }
    pub(in crate::engine) fn add_dht(
        &mut self,
        _: crate::protocol::identity::InfoHashV1,
        peers: &[SocketAddr],
    ) -> io::Result<()> {
        self.add(peers, "dht")
    }
    pub(in crate::engine) fn add_pex(
        &mut self,
        _: crate::engine::peer_io::ConnectionId,
        peers: &[SocketAddr],
    ) -> io::Result<()> {
        self.add(peers, "pex")
    }
    pub(in crate::engine) fn candidate(
        &mut self,
        rejects: impl Fn(SocketAddr) -> bool,
    ) -> io::Result<Option<SocketAddr>> {
        let mut state = self.queue.0.lock().unwrap();
        for _ in 0..state.entries.len() {
            let entry = state.entries.pop_front().unwrap();
            if entry.owner != self.owner || entry.due > Instant::now() || rejects(entry.address) {
                state.entries.push_back(entry);
            } else {
                self.selected_source = entry.source;
                return Ok(Some(entry.address));
            }
        }
        Ok(None)
    }
    pub(in crate::engine) fn dial_completed(
        &mut self,
        address: SocketAddr,
        _: bool,
        utp_failed: bool,
    ) {
        let known = self.known.get_mut(&address).expect("dialed contact");
        known.utp_failed |= utp_failed;
        if known.source == "direct" {
            self.queue.0.lock().unwrap().entries.push_back(Entry {
                owner: self.owner,
                address,
                due: Instant::now() + Duration::from_secs(30),
                source: "direct",
            });
        } else if known.utp_failed {
            known.queued = false;
        } else {
            self.known.remove(&address);
        }
    }
    pub(in crate::engine) fn progress(&self) -> usize {
        self.known.values().filter(|k| k.queued).count()
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
        self.tasks.spawn(async move {
            let mut utp_failed = false;
            let result = async {
                if let Some(mux) = utp {
                    match super::utp::Mux::connect(mux, address).await {
                        Ok(stream) => return Ok(crate::engine::peer_io::Stream::Utp(stream)),
                        Err(error) => {
                            utp_failed = matches!(
                                error.kind(),
                                io::ErrorKind::TimedOut | io::ErrorKind::ConnectionReset
                            );
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
                },
                permit,
            )
        });
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
