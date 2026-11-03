use super::InboundStatus;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Default)]
struct Counters {
    active: usize,
    active_public: usize,
    total: u64,
    public_total: u64,
    last_public: Option<Instant>,
}
impl Counters {
    fn snapshot(&self) -> InboundStatus {
        InboundStatus {
            active: self.active,
            active_public: self.active_public,
            total: self.total,
            public_total: self.public_total,
            last_public_ago_secs: self.last_public.map(|at| at.elapsed().as_secs()),
        }
    }
}
#[derive(Default)]
pub(in crate::engine) struct Incoming {
    tcp: Counters,
    utp: Counters,
}
impl Incoming {
    fn transport(&mut self, tcp: bool) -> &mut Counters {
        if tcp { &mut self.tcp } else { &mut self.utp }
    }
    pub(super) fn snapshot(&self) -> (InboundStatus, InboundStatus) {
        (self.tcp.snapshot(), self.utp.snapshot())
    }
    pub(in crate::engine) fn connected(
        incoming: &Arc<Mutex<Self>>,
        tcp: bool,
        address: SocketAddr,
    ) -> Connection {
        let public = crate::engine::network::source(address, false);
        let mut state = incoming.lock().unwrap();
        let counters = state.transport(tcp);
        counters.active += 1;
        counters.total += 1;
        if public {
            counters.active_public += 1;
            counters.public_total += 1;
            counters.last_public = Some(Instant::now());
        }
        Connection {
            incoming: incoming.clone(),
            tcp,
            public,
        }
    }
}
// A canceled or panicking transport must release its active count too.
pub(in crate::engine) struct Connection {
    incoming: Arc<Mutex<Incoming>>,
    tcp: bool,
    public: bool,
}
impl Drop for Connection {
    fn drop(&mut self) {
        let mut state = self.incoming.lock().unwrap();
        let counters = state.transport(self.tcp);
        counters.active -= 1;
        if self.public {
            counters.active_public -= 1;
        }
    }
}
