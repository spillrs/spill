//! Shared payload accounting and async capacity notifications. Core supplies its
//! clock to take(); transports wait on the same budgets without an adapter layer.
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::watch;

const SCALE: i128 = 1_000_000_000;
const BLOCK: u64 = 16 * 1024;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Limits {
    pub down: u64,
    pub up: u64,
}
#[derive(Clone, Copy)]
pub(crate) enum Direction {
    Down,
    Up,
}
#[derive(Default)]
pub(crate) struct Bucket {
    pub(crate) rate: u64,
    credit: i128,
    at: Option<Instant>,
}
impl Bucket {
    pub(crate) fn new(rate: u64, now: Instant) -> Self {
        Self {
            rate,
            credit: if rate == 0 {
                0
            } else {
                i128::from(rate.max(BLOCK)) * SCALE
            },
            at: Some(now),
        }
    }
    fn capacity(&self) -> i128 {
        i128::from(self.rate.max(BLOCK)) * SCALE
    }
    pub(crate) fn refill(&mut self, now: Instant) {
        if self.rate != 0 {
            let missing = self.capacity() - self.credit;
            let nanos = self
                .at
                .map_or(Duration::ZERO, |at| now.saturating_duration_since(at))
                .as_nanos()
                .min((missing / i128::from(self.rate) + 1) as u128);
            self.credit =
                (self.credit + nanos as i128 * i128::from(self.rate)).min(self.capacity());
        }
        self.at = Some(now);
    }
    pub(crate) fn delay(&self, bytes: u32) -> u64 {
        if self.rate == 0 {
            return 0;
        }
        let needed = (i128::from(bytes) * SCALE).min(self.capacity()) - self.credit;
        if needed <= 0 {
            0
        } else {
            ((needed + i128::from(self.rate) - 1) / i128::from(self.rate)) as u64
        }
    }
    pub(crate) fn debit(&mut self, bytes: u32) {
        if self.rate != 0 {
            self.credit -= i128::from(bytes) * SCALE;
        }
    }
    fn refund(&mut self, bytes: u32) {
        if self.rate != 0 {
            self.credit = (self.credit + i128::from(bytes) * SCALE).min(self.capacity());
        }
    }
    fn set(&mut self, rate: u64, now: Instant) {
        self.refill(now);
        if self.rate == 0 && rate != 0 {
            self.credit = i128::from(rate.max(BLOCK)) * SCALE;
        }
        self.rate = rate;
        self.credit = if rate == 0 {
            0
        } else {
            self.credit.min(self.capacity())
        };
    }
}
#[derive(Default)]
struct State {
    down: Bucket,
    up: Bucket,
}
impl State {
    fn bucket(&mut self, direction: Direction) -> &mut Bucket {
        match direction {
            Direction::Down => &mut self.down,
            Direction::Up => &mut self.up,
        }
    }
}
struct Inner {
    state: Mutex<State>,
    changed: watch::Sender<()>,
}
#[derive(Clone)]
pub(crate) struct Budget(Arc<Inner>);
impl Default for Budget {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}
impl Budget {
    pub(crate) fn new(limits: Limits) -> Self {
        let now = tokio::time::Instant::now().into_std();
        Self(Arc::new(Inner {
            state: Mutex::new(State {
                down: Bucket::new(limits.down, now),
                up: Bucket::new(limits.up, now),
            }),
            changed: watch::channel(()).0,
        }))
    }
    fn set(&self, now: Instant, limits: Limits) {
        let mut state = self.0.state.lock().unwrap();
        if state.down.rate == limits.down && state.up.rate == limits.up {
            return;
        }
        state.down.set(limits.down, now);
        state.up.set(limits.up, now);
        drop(state);
        self.0.changed.send_replace(());
    }
}
#[derive(Clone, Default)]
pub(crate) struct Throttle {
    global: Budget,
    local: Budget,
}
impl Throttle {
    pub(crate) fn new(global: Budget, local: Budget) -> Self {
        assert!(!Arc::ptr_eq(&global.0, &local.0));
        Self { global, local }
    }
    pub(crate) fn limited(&self, direction: Direction) -> bool {
        self.global.0.state.lock().unwrap().bucket(direction).rate != 0
            || self.local.0.state.lock().unwrap().bucket(direction).rate != 0
    }
    pub(crate) fn take(
        &self,
        now: Instant,
        direction: Direction,
        bytes: u32,
    ) -> Result<Permit, Instant> {
        if bytes == 0 || !self.limited(direction) {
            return Ok(Permit(None));
        }
        let mut global = self.global.0.state.lock().unwrap();
        let mut local = self.local.0.state.lock().unwrap();
        let a = global.bucket(direction);
        let b = local.bucket(direction);
        a.refill(now);
        b.refill(now);
        let delay = a.delay(bytes).max(b.delay(bytes));
        if delay != 0 {
            return Err(now + Duration::from_nanos(delay));
        }
        let charged = (a.rate != 0, b.rate != 0);
        a.debit(bytes);
        b.debit(bytes);
        Ok(Permit(Some((self.clone(), direction, bytes, charged))))
    }
    pub(crate) fn set_local(&self, now: Instant, limits: Limits) {
        self.local.set(now, limits);
    }
    pub(crate) fn changes(&self) -> Changes {
        Changes {
            global: self.global.0.changed.subscribe(),
            local: self.local.0.changed.subscribe(),
        }
    }
    pub(crate) async fn acquire(&self, direction: Direction, bytes: u32) -> Permit {
        let mut changes = self.changes();
        loop {
            match self.take(tokio::time::Instant::now().into_std(), direction, bytes) {
                Ok(permit) => return permit,
                Err(deadline) => tokio::select! {
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {},
                    _ = changes.changed() => {},
                },
            }
        }
    }
}
pub(crate) struct Changes {
    global: watch::Receiver<()>,
    local: watch::Receiver<()>,
}
impl Changes {
    pub(crate) async fn changed(&mut self) {
        tokio::select! { _ = self.global.changed() => {}, _ = self.local.changed() => {} }
    }
}
pub(crate) struct Permit(Option<(Throttle, Direction, u32, (bool, bool))>);
impl Permit {
    pub(crate) fn commit(mut self) {
        self.0 = None;
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        if let Some((throttle, direction, bytes, charged)) = self.0.take() {
            if charged.0 {
                throttle
                    .global
                    .0
                    .state
                    .lock()
                    .unwrap()
                    .bucket(direction)
                    .refund(bytes);
                throttle.global.0.changed.send_replace(());
            }
            if charged.1 {
                throttle
                    .local
                    .0
                    .state
                    .lock()
                    .unwrap()
                    .bucket(direction)
                    .refund(bytes);
                throttle.local.0.changed.send_replace(());
            }
        }
    }
}
