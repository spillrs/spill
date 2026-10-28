//! Ten-second rate ordering with one optimistic slot rotated every thirty seconds.
//! Slot grants are policy only; the transfer owner handles wire backpressure.
use std::time::{Duration, Instant};

pub(crate) struct Candidate<K> {
    pub(crate) id: K,
    pub(crate) eligible: bool,
    pub(crate) downloaded: u64,
    pub(crate) uploaded: u64,
}

pub(crate) struct Choker<K> {
    last: Option<Instant>,
    optimistic_at: Option<Instant>,
    optimistic: Option<K>,
    grants: Vec<K>,
    samples: Vec<(K, u64, u64)>,
    seeding: bool,
}
impl<K: Copy + Eq> Choker<K> {
    pub(crate) fn new() -> Self {
        Self {
            last: None,
            optimistic_at: None,
            optimistic: None,
            grants: Vec::new(),
            samples: Vec::new(),
            seeding: false,
        }
    }
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.last
            .map(|at| at + Duration::from_secs(10))
            .into_iter()
            .chain(
                self.optimistic_at
                    .filter(|_| self.optimistic.is_some())
                    .map(|at| at + Duration::from_secs(30)),
            )
            .min()
    }
    /// Eligibility changes are signalled by the owner; rates need a timer only.
    pub(crate) fn due(&self, now: Instant, seeding: bool) -> bool {
        self.last.is_none()
            || seeding != self.seeding
            || self.deadline().is_some_and(|at| now >= at)
    }
    pub(crate) fn granted(&self, id: K) -> bool {
        self.grants.contains(&id)
    }
    pub(crate) fn update(
        &mut self,
        now: Instant,
        seeding: bool,
        slots: usize,
        peers: &[Candidate<K>],
    ) {
        // Fill newly available slots promptly; otherwise rates change grants only
        // at rechoke boundaries. Lost interest/closed peers cannot retain slots.
        let eligible = peers.iter().filter(|p| p.eligible).count();
        let invalid = self
            .grants
            .iter()
            .any(|id| !peers.iter().any(|p| p.id == *id && p.eligible));
        if self.deadline().is_some_and(|at| now < at)
            && seeding == self.seeding
            && !invalid
            && self.grants.len() == slots.min(eligible)
        {
            return;
        }
        let mut ranked: Vec<_> = peers
            .iter()
            .filter(|p| p.eligible)
            .map(|p| {
                let old = self.samples.iter().find(|s| s.0 == p.id);
                let bytes = if seeding { p.uploaded } else { p.downloaded };
                let previous = old.map_or(0, |s| if seeding { s.2 } else { s.1 });
                (p.id, bytes.saturating_sub(previous))
            })
            .collect();
        // All deltas span the same rechoke interval; ordering is rate ordering.
        ranked.sort_by_key(|p| std::cmp::Reverse(p.1));
        self.grants.clear();
        let regular = if ranked.len() > slots {
            slots.saturating_sub(1)
        } else {
            slots
        };
        self.grants.extend(ranked.iter().take(regular).map(|p| p.0));
        let rotate = self
            .optimistic_at
            .is_none_or(|at| now.duration_since(at) >= Duration::from_secs(30));
        if self.grants.len() < slots && ranked.len() > regular {
            let remaining: Vec<_> = peers
                .iter()
                .filter(|p| p.eligible && !self.grants.contains(&p.id))
                .map(|p| p.id)
                .collect();
            if rotate || self.optimistic.is_none_or(|id| !remaining.contains(&id)) {
                let next = self
                    .optimistic
                    .and_then(|id| remaining.iter().position(|p| *p == id))
                    .map_or(0, |position| (position + 1) % remaining.len());
                self.optimistic = Some(remaining[next]);
                self.optimistic_at = Some(now);
            }
            self.grants
                .push(self.optimistic.expect("optimistic candidate"));
        } else {
            self.optimistic = None;
        }
        self.samples = peers
            .iter()
            .map(|p| (p.id, p.downloaded, p.uploaded))
            .collect();
        self.last = Some(now);
        self.seeding = seeding;
    }
}
