//! Bounded BEP 5 peer storage, rotating IP/info-hash tokens and datagram admission.
use super::Limits;
use sha1::{Digest, Sha1};
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::time::Instant;

const ROTATE: Duration = Duration::from_secs(300);
const PEER_AGE: Duration = Duration::from_secs(45 * 60);
const SOURCE_AGE: Duration = Duration::from_secs(300);

pub(super) struct Tokens {
    secrets: [[u8; 20]; 2],
    rotated: Instant,
}
impl Tokens {
    pub(in crate::engine) fn new(now: Instant) -> io::Result<Self> {
        let mut secrets = [[0; 20]; 2];
        for secret in &mut secrets {
            getrandom::fill(secret).map_err(io::Error::other)?;
        }
        Ok(Self {
            secrets,
            rotated: now,
        })
    }
    pub(in crate::engine) fn rotate(&mut self, now: Instant) -> io::Result<()> {
        let age = now.duration_since(self.rotated);
        if age >= ROTATE {
            self.secrets[1] = self.secrets[0];
            getrandom::fill(&mut self.secrets[0]).map_err(io::Error::other)?;
            if age >= ROTATE * 2 {
                getrandom::fill(&mut self.secrets[1]).map_err(io::Error::other)?;
            }
            self.rotated += ROTATE * (age.as_secs() / ROTATE.as_secs()) as u32;
        }
        Ok(())
    }
    fn hash(secret: &[u8; 20], ip: IpAddr, hash: &[u8; 20]) -> [u8; 4] {
        let mut digest = Sha1::new();
        match ip {
            IpAddr::V4(ip) => digest.update(ip.octets()),
            IpAddr::V6(ip) => digest.update(ip.octets()),
        }
        digest.update(secret);
        digest.update(hash);
        digest.finalize()[..4].try_into().expect("four-byte token")
    }
    pub(in crate::engine) fn issue(&self, ip: IpAddr, hash: &[u8; 20]) -> [u8; 4] {
        Self::hash(&self.secrets[0], ip, hash)
    }
    pub(in crate::engine) fn valid(&self, token: &[u8], ip: IpAddr, hash: &[u8; 20]) -> bool {
        token.len() == 4
            && self.secrets.iter().any(|secret| {
                let expected = Self::hash(secret, ip, hash);
                token
                    .iter()
                    .zip(expected)
                    .fold(0u8, |diff, (a, b)| diff | (a ^ b))
                    == 0
            })
    }
}

pub(super) struct Peers {
    torrents: HashMap<[u8; 20], Vec<(SocketAddr, Instant)>>,
    limits: Limits,
}
impl Peers {
    pub(in crate::engine) fn new(limits: Limits) -> Self {
        Self {
            torrents: HashMap::new(),
            limits,
        }
    }
    pub(in crate::engine) fn expire(&mut self, now: Instant) {
        self.torrents.retain(|_, peers| {
            peers.retain(|(_, seen)| now.duration_since(*seen) < PEER_AGE);
            !peers.is_empty()
        });
    }
    pub(in crate::engine) fn put(
        &mut self,
        hash: [u8; 20],
        peer: SocketAddr,
        now: Instant,
    ) -> bool {
        if peer.port() == 0 {
            return false;
        }
        if !self.torrents.contains_key(&hash) && self.torrents.len() >= self.limits.max_torrents {
            return false;
        }
        let peers = self.torrents.entry(hash).or_default();
        if let Some((_, seen)) = peers.iter_mut().find(|(p, _)| *p == peer) {
            *seen = now;
        } else if peers.len() < self.limits.max_peers_per_torrent {
            peers.push((peer, now));
        } else {
            return false;
        }
        true
    }
    pub(in crate::engine) fn get(
        &self,
        hash: &[u8; 20],
        ipv6: bool,
        now: Instant,
    ) -> Vec<SocketAddr> {
        self.torrents
            .get(hash)
            .into_iter()
            .flatten()
            .filter(|(peer, seen)| peer.is_ipv6() == ipv6 && now.duration_since(*seen) < PEER_AGE)
            .take(self.limits.max_peers_reply)
            .map(|(p, _)| *p)
            .collect()
    }
    pub(in crate::engine) fn counts(&self) -> (usize, usize) {
        (
            self.torrents.len(),
            self.torrents.values().map(Vec::len).sum(),
        )
    }
}

struct Credit {
    tokens: f64,
    updated: Instant,
}
impl Credit {
    fn take(&mut self, amount: usize, rate: usize, now: Instant) -> bool {
        self.tokens = (self.tokens + now.duration_since(self.updated).as_secs_f64() * rate as f64)
            .min(rate as f64);
        self.updated = now;
        if self.tokens < amount as f64 {
            return false;
        }
        self.tokens -= amount as f64;
        true
    }
}
pub(super) struct Rate {
    sources: HashMap<IpAddr, Credit>,
    output: Credit,
    limits: Limits,
}
impl Rate {
    pub(in crate::engine) fn new(limits: Limits, now: Instant) -> Self {
        Self {
            sources: HashMap::new(),
            output: Credit {
                tokens: limits.response_bytes_per_second as f64,
                updated: now,
            },
            limits,
        }
    }
    pub(in crate::engine) fn expire(&mut self, now: Instant) {
        self.sources
            .retain(|_, c| now.duration_since(c.updated) < SOURCE_AGE);
    }
    pub(in crate::engine) fn query(&mut self, ip: IpAddr, now: Instant) -> bool {
        // A /64 must not gain more query credit by rotating interface IDs.
        let ip = match ip {
            IpAddr::V6(ip) => IpAddr::V6(Ipv6Addr::from(u128::from(ip) & (u128::MAX << 64))),
            ip => ip,
        };
        if !self.sources.contains_key(&ip) && self.sources.len() >= self.limits.max_rate_sources {
            return false;
        }
        self.sources
            .entry(ip)
            .or_insert(Credit {
                tokens: self.limits.queries_per_second as f64,
                updated: now,
            })
            .take(1, self.limits.queries_per_second, now)
    }
    pub(in crate::engine) fn reply(&mut self, bytes: usize, now: Instant) -> bool {
        self.output
            .take(bytes, self.limits.response_bytes_per_second, now)
    }
    pub(in crate::engine) fn len(&self) -> usize {
        self.sources.len()
    }
}
