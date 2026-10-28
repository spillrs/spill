//! Clock-driven BEP 29 reliability, flow control and LEDBAT. No socket access.
use protocol::utp::{Header, Kind, Packet, seq_diff, seq_less, timestamp_diff};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) const PAYLOAD: usize = 1200;
pub(super) const SEND_CAP: usize = 256 * 1024;
const RECEIVE_CAP: usize = 256 * 1024;
// Leave one maximum-size datagram available to fill a hole in reassembly.
const RECEIVE_WINDOW: usize = RECEIVE_CAP - 65536;
const MIN_WINDOW: f64 = 2.0 * PAYLOAD as f64;
const ACK_DELAY: Duration = Duration::from_millis(100);
const LINGER: Duration = Duration::from_secs(10);

struct Sent {
    seq: u16,
    kind: Kind,
    payload: Vec<u8>,
    at: Instant,
    retransmitted: bool,
    acked: bool,
    flying: bool,
    fast_at: Option<Instant>,
}
pub(super) struct Datagram {
    pub header: Header,
    pub sack: Vec<u8>,
    pub payload: Vec<u8>,
}
pub(super) struct Connection {
    send_id: u16,
    next: u16,
    ack: u16,
    last_ack: u16,
    pub connected: bool,
    pub failed: bool,
    pub finished: bool,
    pub retransmits: Arc<AtomicU64>,
    remote_fin: bool,
    closing: Option<Instant>,
    fin_sent: bool,
    queued: VecDeque<u8>,
    sent: VecDeque<Sent>,
    pub received: VecDeque<u8>,
    reordered: HashMap<u16, (Kind, Vec<u8>)>,
    receive_bytes: usize,
    read_since_update: usize,
    remote_window: usize,
    cwnd: f64,
    srtt: Option<f64>,
    rttvar: f64,
    rto: Duration,
    retry: Instant,
    last_send: Instant,
    timeouts: u8,
    probe_pending: bool,
    duplicates: u8,
    recovery_end: Option<u16>,
    reduced_at: Option<Instant>,
    delayed_ack: Option<Instant>,
    ack_packets: u8,
    echo: u32,
    base: [Option<u32>; 13],
    base_slot: usize,
    base_at: Instant,
    out: Vec<Datagram>,
}
impl Connection {
    pub(super) fn outgoing(now: Instant, stamp: u32, send_id: u16, seq: u16) -> Self {
        let mut c = Self::new(now, send_id, seq, 0);
        c.send(now, stamp, Kind::Syn, Vec::new());
        c
    }
    pub(super) fn incoming(now: Instant, send_id: u16, seq: u16, syn_seq: u16) -> Self {
        let mut c = Self::new(now, send_id, seq, syn_seq);
        c.connected = true;
        c
    }
    fn new(now: Instant, send_id: u16, seq: u16, ack: u16) -> Self {
        Self {
            send_id,
            next: seq,
            ack,
            last_ack: seq.wrapping_sub(1),
            connected: false,
            failed: false,
            finished: false,
            retransmits: Arc::new(AtomicU64::new(0)),
            remote_fin: false,
            closing: None,
            fin_sent: false,
            queued: VecDeque::new(),
            sent: VecDeque::new(),
            received: VecDeque::new(),
            reordered: HashMap::new(),
            receive_bytes: 0,
            read_since_update: 0,
            remote_window: RECEIVE_WINDOW,
            cwnd: MIN_WINDOW,
            srtt: None,
            rttvar: 0.0,
            rto: Duration::from_secs(1),
            retry: now + Duration::from_secs(1),
            last_send: now,
            timeouts: 0,
            probe_pending: false,
            duplicates: 0,
            recovery_end: None,
            reduced_at: None,
            delayed_ack: None,
            ack_packets: 0,
            echo: 0,
            base: [None; 13],
            base_slot: 0,
            base_at: now,
            out: Vec::new(),
        }
    }
    fn window(&self) -> usize {
        RECEIVE_WINDOW.saturating_sub(self.receive_bytes)
    }
    fn header(&self, stamp: u32, kind: Kind, seq: u16) -> Header {
        Header {
            kind,
            connection_id: self.send_id,
            timestamp_us: stamp,
            timestamp_diff_us: self.echo,
            wnd_size: self.window() as u32,
            seq_nr: seq,
            ack_nr: self.ack,
        }
    }
    pub(super) fn send_id(&self) -> u16 {
        self.send_id
    }
    pub(super) fn reply_syn(&mut self, stamp: u32, remote_stamp: u32) {
        self.echo = timestamp_diff(stamp, remote_stamp);
        self.state(stamp);
    }
    pub(super) fn state(&mut self, stamp: u32) {
        let mut sack = Vec::new();
        for &seq in self.reordered.keys() {
            let bit = (seq_diff(seq, self.ack) - 2) as usize;
            sack.resize(sack.len().max((bit / 32 + 1) * 4), 0);
            sack[bit / 8] |= 1 << (bit % 8);
        }
        self.read_since_update = 0;
        self.out.push(Datagram {
            header: self.header(stamp, Kind::State, self.next),
            sack,
            payload: Vec::new(),
        });
        self.delayed_ack = None;
        self.ack_packets = 0;
    }
    fn send(&mut self, now: Instant, stamp: u32, kind: Kind, payload: Vec<u8>) {
        self.read_since_update = 0;
        self.last_send = now;
        let seq = self.next;
        self.next = self.next.wrapping_add(1);
        self.out.push(Datagram {
            header: self.header(stamp, kind, seq),
            sack: Vec::new(),
            payload: payload.clone(),
        });
        if self.sent.is_empty() {
            self.retry = now + self.rto;
        }
        self.sent.push_back(Sent {
            seq,
            kind,
            payload,
            at: now,
            retransmitted: false,
            acked: false,
            flying: true,
            fast_at: None,
        });
    }
    fn retransmit(&mut self, index: usize, now: Instant, stamp: u32) {
        let p = &mut self.sent[index];
        p.at = now;
        p.retransmitted = true;
        p.flying = true;
        let (kind, seq, payload) = (p.kind, p.seq, p.payload.clone());
        self.retransmits.fetch_add(1, Ordering::Relaxed);
        self.read_since_update = 0;
        self.last_send = now;
        self.out.push(Datagram {
            header: self.header(stamp, kind, seq),
            sack: Vec::new(),
            payload,
        });
    }
    fn fast_retransmit(&mut self, index: usize, now: Instant, stamp: u32) -> bool {
        let rtt = Duration::from_secs_f64(self.srtt.unwrap_or(0.5).max(0.001));
        if self.sent[index].fast_at.is_some_and(|at| now < at + rtt) {
            return false;
        }
        self.sent[index].fast_at = Some(now);
        if self.reduced_at.is_none_or(|at| now >= at + rtt) {
            self.cwnd = (self.cwnd / 2.0).max(MIN_WINDOW);
            self.reduced_at = Some(now);
        }
        self.recovery_end.get_or_insert(self.next.wrapping_sub(1));
        // A partial ACK must repair the next hole even if the reduced cwnd is full.
        self.retransmit(index, now, stamp);
        if index == 0 {
            self.retry = now + self.rto;
        }
        true
    }
    fn flight(&self) -> usize {
        self.sent
            .iter()
            .filter(|p| !p.acked && p.flying)
            .map(|p| p.payload.len().max(1))
            .sum()
    }
    pub(super) fn on_packet(&mut self, now: Instant, stamp: u32, packet: Packet<'_>) {
        if self.failed || self.finished {
            return;
        }
        let h = packet.header;
        if h.kind == Kind::Reset {
            self.failed = true;
            return;
        }
        if h.kind == Kind::Syn {
            return;
        }
        if !self.connected {
            if h.kind != Kind::State || h.ack_nr != self.next.wrapping_sub(1) {
                return;
            }
            self.connected = true;
            self.send_id = self.send_id.wrapping_add(1);
            self.ack = h.seq_nr.wrapping_sub(1);
        }
        if self.probe_pending {
            self.probe_pending = false;
            self.timeouts = 0;
        }
        self.remote_window = h.wnd_size as usize;
        self.echo = timestamp_diff(stamp, h.timestamp_us);
        while now.duration_since(self.base_at) >= Duration::from_secs(10) {
            self.base_at += Duration::from_secs(10);
            self.base_slot = (self.base_slot + 1) % self.base.len();
            self.base[self.base_slot] = None;
        }
        if h.timestamp_diff_us != 0 {
            let slot = &mut self.base[self.base_slot];
            *slot = Some(slot.map_or(h.timestamp_diff_us, |v| {
                if (h.timestamp_diff_us.wrapping_sub(v) as i32) < 0 {
                    h.timestamp_diff_us
                } else {
                    v
                }
            }));
        }
        if !seq_less(h.ack_nr, self.last_ack) && seq_less(h.ack_nr, self.next) {
            let advanced = h.ack_nr != self.last_ack;
            let used = self.flight();
            let mut bytes_acked = 0;
            let mut sample = None;
            let mut any = false;
            for p in &mut self.sent {
                let delta = seq_diff(p.seq, h.ack_nr);
                let selective = delta >= 2
                    && packet.sack.is_some_and(|s| {
                        let bit = (delta - 2) as usize;
                        s.get(bit / 8).is_some_and(|b| b & (1 << (bit % 8)) != 0)
                    });
                if !p.acked && (!seq_less(h.ack_nr, p.seq) || selective) {
                    p.acked = true;
                    p.flying = false;
                    any = true;
                    bytes_acked += p.payload.len();
                    p.payload = Vec::new();
                    if !p.retransmitted {
                        sample = Some(now.duration_since(p.at).as_secs_f64());
                    }
                    if p.kind == Kind::Fin {
                        self.finished = true;
                    }
                }
            }
            while self.sent.front().is_some_and(|p| p.acked) {
                self.sent.pop_front();
            }
            if any {
                self.timeouts = 0;
                if let Some(rtt) = sample {
                    if let Some(srtt) = self.srtt {
                        self.rttvar = 0.75 * self.rttvar + 0.25 * (srtt - rtt).abs();
                        self.srtt = Some(0.875 * srtt + 0.125 * rtt);
                    } else {
                        self.srtt = Some(rtt);
                        self.rttvar = rtt / 2.0;
                    }
                }
                self.rto = Duration::from_secs_f64(
                    (self.srtt.unwrap_or(1.0) + 4.0 * self.rttvar).clamp(0.5, 60.0),
                );
                self.retry = now + self.rto;
                if h.timestamp_diff_us != 0 {
                    let base = self
                        .base
                        .iter()
                        .flatten()
                        .copied()
                        .min_by(|a, b| (a.wrapping_sub(*b) as i32).cmp(&0))
                        .expect("delay sample");
                    let mut delay = h.timestamp_diff_us.wrapping_sub(base);
                    if delay > i32::MAX as u32 {
                        self.base.fill(None);
                        self.base[self.base_slot] = Some(h.timestamp_diff_us);
                        delay = 0;
                    }
                    let off = (100_000.0 - delay as f64) / 100_000.0;
                    if off < 0.0 || used as f64 >= self.cwnd / 2.0 {
                        self.cwnd = (self.cwnd + 3000.0 * off * bytes_acked as f64 / self.cwnd)
                            .clamp(MIN_WINDOW, SEND_CAP as f64);
                    }
                }
            }
            if advanced {
                self.duplicates = 0;
            } else if h.kind == Kind::State && self.remote_window != 0 && !self.sent.is_empty() {
                self.duplicates += 1;
            }
            let partial = advanced && self.recovery_end.is_some_and(|end| seq_less(h.ack_nr, end));
            if self
                .recovery_end
                .is_some_and(|end| !seq_less(h.ack_nr, end))
            {
                self.recovery_end = None;
            }
            let duplicate_loss = self.duplicates >= 3;
            if duplicate_loss {
                self.duplicates = 0;
            }
            let mut later_acked = 0;
            let mut holes = Vec::new();
            for (index, p) in self.sent.iter().enumerate().rev() {
                if p.acked {
                    later_acked += 1;
                } else if later_acked >= 3 || (index == 0 && (partial || duplicate_loss)) {
                    holes.push(index);
                }
            }
            if self.remote_window != 0 {
                let mut retransmitted = 0;
                for index in holes.into_iter().rev() {
                    retransmitted += usize::from(self.fast_retransmit(index, now, stamp));
                    // One ACK must not release a whole congestion window of repairs.
                    if retransmitted == 3 {
                        break;
                    }
                }
            }
            self.last_ack = h.ack_nr;
        }
        if matches!(h.kind, Kind::Data | Kind::Fin) {
            let distance = seq_diff(h.seq_nr, self.ack);
            let buffered = self.receive_bytes + packet.payload.len();
            let fits = buffered <= RECEIVE_CAP && (distance == 1 || buffered <= RECEIVE_WINDOW);
            if !self.remote_fin
                && distance > 0
                && distance <= 256
                && !self.reordered.contains_key(&h.seq_nr)
                && fits
                && (h.kind == Kind::Fin || !packet.payload.is_empty())
            {
                self.receive_bytes += packet.payload.len();
                self.reordered
                    .insert(h.seq_nr, (h.kind, packet.payload.to_vec()));
                let mut filled_hole = false;
                while let Some((kind, payload)) = self.reordered.remove(&self.ack.wrapping_add(1)) {
                    self.ack = self.ack.wrapping_add(1);
                    self.received.extend(payload);
                    if kind == Kind::Fin {
                        self.remote_fin = true;
                        self.receive_bytes -= self
                            .reordered
                            .values()
                            .map(|(_, bytes)| bytes.len())
                            .sum::<usize>();
                        self.reordered.clear();
                        break;
                    }
                    filled_hole |= self.ack != h.seq_nr;
                }
                self.ack_packets += 1;
                if distance != 1 || filled_hole || self.ack_packets >= 2 || h.kind == Kind::Fin {
                    self.state(stamp);
                } else {
                    self.delayed_ack.get_or_insert(now + ACK_DELAY);
                }
            } else {
                self.state(stamp);
            }
        }
        self.pump(now, stamp, false);
    }
    pub(super) fn pump(&mut self, now: Instant, stamp: u32, probe: bool) {
        if !self.connected || self.failed || self.finished {
            return;
        }
        let window = (self.cwnd as usize).min(self.remote_window);
        let mut flight = self.flight();
        for index in 0..self.sent.len() {
            let p = &self.sent[index];
            if p.acked || p.flying {
                continue;
            }
            let count = p.payload.len().max(1);
            if flight + count > window && !(probe && flight == 0) {
                break;
            }
            self.retransmit(index, now, stamp);
            flight += count;
        }
        if self.sent.iter().any(|p| !p.acked && !p.flying) {
            return;
        }
        while !self.queued.is_empty()
            && seq_diff(self.next, self.last_ack) <= 256
            && (flight < window || (probe && flight == 0))
        {
            let count = if probe && window == 0 {
                1
            } else {
                PAYLOAD.min(window - flight)
            }
            .min(self.queued.len());
            let mut payload = vec![0; count];
            read_buffer(&mut self.queued, &mut payload);
            self.send(now, stamp, Kind::Data, payload);
            flight += count;
        }
        if self.closing.is_some()
            && self.queued.is_empty()
            && self.sent.is_empty()
            && !self.fin_sent
        {
            self.fin_sent = true;
            self.send(now, stamp, Kind::Fin, Vec::new());
        }
    }
    pub(super) fn on_timeout(&mut self, now: Instant, stamp: u32) {
        if self.closing.is_some_and(|at| now >= at + LINGER) {
            self.finished = true;
            return;
        }
        if self.delayed_ack.is_some_and(|at| now >= at) {
            self.state(stamp);
        }
        if now >= self.retry && (!self.sent.is_empty() || !self.queued.is_empty()) {
            if self.remote_window != 0 || self.probe_pending {
                self.timeouts += 1;
            }
            if self.timeouts >= 7 {
                self.failed = true;
                return;
            }
            if self.remote_window == 0 {
                self.rto = (self.rto * 2).min(Duration::from_secs(60));
                self.probe_pending = true;
                if let Some(index) = self.sent.iter().position(|p| !p.acked) {
                    self.retransmit(index, now, stamp);
                } else {
                    self.pump(now, stamp, true);
                }
            } else {
                self.cwnd = PAYLOAD as f64;
                self.rto = if self.connected {
                    (self.rto * 2).min(Duration::from_secs(60))
                } else {
                    Duration::from_secs(1)
                };
                for p in &mut self.sent {
                    p.flying = false;
                }
                self.recovery_end = None;
                if self.connected {
                    self.pump(now, stamp, true);
                } else {
                    self.retransmit(0, now, stamp);
                }
            }
            self.retry = now + self.rto;
        }
        self.pump(now, stamp, false);
    }
    pub(super) fn deadline(&self) -> Option<Instant> {
        if self.failed || self.finished {
            return None;
        }
        let retry = (!self.sent.is_empty() || (!self.queued.is_empty() && self.remote_window == 0))
            .then_some(self.retry);
        [retry, self.delayed_ack, self.closing.map(|at| at + LINGER)]
            .into_iter()
            .flatten()
            .min()
    }
    pub(super) fn take_output(&mut self) -> Vec<Datagram> {
        std::mem::take(&mut self.out)
    }
    pub(super) fn writable(&self) -> usize {
        SEND_CAP
            - self.queued.len()
            - self
                .sent
                .iter()
                .filter(|p| !p.acked)
                .map(|p| p.payload.len())
                .sum::<usize>()
    }
    pub(super) fn write(&mut self, now: Instant, bytes: &[u8]) -> usize {
        if self.queued.is_empty() && self.sent.is_empty() {
            self.retry = now + self.rto;
            if now >= self.last_send + self.rto {
                self.cwnd = self.cwnd.min(MIN_WINDOW);
            }
        }
        let n = self.writable().min(bytes.len());
        self.queued.extend(&bytes[..n]);
        n
    }
    pub(super) fn read(&mut self, stamp: u32, bytes: &mut [u8]) -> usize {
        let full = self.window() == 0;
        let n = read_buffer(&mut self.received, bytes);
        self.receive_bytes -= n;
        self.read_since_update += n;
        if n != 0 && (full || self.read_since_update >= RECEIVE_CAP / 8) {
            self.state(stamp);
        }
        n
    }
    pub(super) fn abort(&mut self, stamp: u32) {
        self.out.push(Datagram {
            header: self.header(stamp, Kind::Reset, self.next),
            sack: Vec::new(),
            payload: Vec::new(),
        });
        self.failed = true;
    }
    pub(super) fn eof(&self) -> bool {
        self.remote_fin
    }
    pub(super) fn flushed(&self) -> bool {
        self.queued.is_empty() && self.sent.iter().all(|p| p.acked)
    }
    pub(super) fn close(&mut self, now: Instant) {
        if !self.connected {
            self.failed = true;
        } else {
            self.closing.get_or_insert(now);
        }
    }
}
fn read_buffer(buffer: &mut VecDeque<u8>, output: &mut [u8]) -> usize {
    let count = output.len().min(buffer.len());
    let (first, second) = buffer.as_slices();
    let split = first.len().min(count);
    output[..split].copy_from_slice(&first[..split]);
    output[split..count].copy_from_slice(&second[..count - split]);
    buffer.drain(..count);
    count
}
