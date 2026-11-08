//! Egress policy, applied to numeric endpoints after resolution and before I/O.
pub(in crate::engine) use std::net::{IpAddr, SocketAddr};
use std::{io, sync::Arc, time::Duration};
use url::Url;

pub(in crate::engine) fn canonical(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V6(a) => a
            .ip()
            .to_ipv4_mapped()
            .map_or(address, |ip| SocketAddr::new(IpAddr::V4(ip), a.port())),
        _ => address,
    }
}

pub(in crate::engine) fn source(address: SocketAddr, allow_local: bool) -> bool {
    let ip = address.ip().to_canonical();
    if address.port() == 0 || ip.is_unspecified() || ip.is_multicast() {
        return false;
    }
    if let SocketAddr::V6(address) = address
        && (address.scope_id() != 0 || address.flowinfo() != 0)
    {
        return false;
    }
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_broadcast()
                && (allow_local
                    || !(ip.is_loopback()
                        || ip.is_private()
                        || ip.is_link_local()
                        || u32::from(ip) & 0xffc0_0000 == 0x6440_0000
                        || ip.octets()[0] == 0))
        }
        IpAddr::V6(ip) => {
            allow_local || !(ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local())
        }
    }
}

// Filter the actual connection destinations, with no secondary unfiltered lookup.
#[derive(Debug)]
struct Resolver(bool);
impl reqwest::dns::Resolve for Resolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let allow_local_network = self.0;
        Box::pin(async move {
            let addresses = tokio::time::timeout(
                crate::engine::profile::DNS_TIMEOUT,
                tokio::net::lookup_host((name.as_str(), 80)),
            )
            .await??;
            let allowed: Vec<_> = addresses
                .filter(|a| source(*a, allow_local_network))
                .map(|mut address| {
                    // A nonzero DNS port overrides the URL's default HTTPS port.
                    address.set_port(0);
                    address
                })
                .collect();
            if allowed.is_empty() {
                return Err(
                    io::Error::new(io::ErrorKind::InvalidData, "no usable HTTP address").into(),
                );
            }
            Ok(Box::new(allowed.into_iter()) as reqwest::dns::Addrs)
        })
    }
}
pub(in crate::engine) fn http_client(allow_local_network: bool) -> reqwest::ClientBuilder {
    crate::http::client()
        .user_agent(crate::engine::identity::CLIENT_NAME)
        .referer(false)
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            let url = attempt.url();
            if attempt.previous().len() > 5 {
                return attempt.error("HTTP redirect limit");
            }
            if !matches!(url.scheme(), "http" | "https")
                || (url.scheme() == "http"
                    && attempt
                        .previous()
                        .last()
                        .is_some_and(|url| url.scheme() == "https"))
                || !http_destination_allowed(url, allow_local_network)
            {
                return attempt.error("HTTP redirect destination not allowed");
            }
            attempt.follow()
        }))
        .retry(reqwest::retry::never())
        .pool_idle_timeout(Duration::from_secs(90))
        .dns_resolver(Arc::new(Resolver(allow_local_network)))
        .connect_timeout(crate::engine::profile::CONNECT_TIMEOUT)
}
pub(in crate::engine) fn http_destination_allowed(url: &Url, allow_local_network: bool) -> bool {
    let Some(port) = url.port_or_known_default() else {
        return false;
    };
    if port == 0 {
        return false;
    }
    match url.host() {
        Some(url::Host::Ipv4(ip)) => source(SocketAddr::new(ip.into(), port), allow_local_network),
        Some(url::Host::Ipv6(ip)) => source(SocketAddr::new(ip.into(), port), allow_local_network),
        Some(url::Host::Domain(_)) => true,
        None => false,
    }
}

pub(in crate::engine) fn peer(mut address: SocketAddr, allow_local: bool) -> bool {
    // LSD learns the interface scope from the kernel for link-local IPv6 peers.
    if allow_local
        && let SocketAddr::V6(v6) = &mut address
        && v6.ip().is_unicast_link_local()
    {
        v6.set_scope_id(0);
    }
    source(address, allow_local) && (allow_local || address.port() >= 1024)
}

pub(in crate::engine) fn eligible(address: SocketAddr) -> bool {
    source(address, true)
        && match address.ip() {
            IpAddr::V6(ip) => ip.to_ipv4_mapped().is_none() && !ip.is_unicast_link_local(),
            _ => true,
        }
}

pub(in crate::engine) async fn resolve_dht(
    host: &str,
    port: u16,
    ipv6: bool,
    allow_local_network: bool,
) -> Option<SocketAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        let address = SocketAddr::new(ip, port);
        return (address.is_ipv6() == ipv6
            && super::dht::valid_address(address, false)
            && peer(address, allow_local_network))
        .then_some(address);
    }
    tokio::time::timeout(
        crate::engine::profile::DNS_TIMEOUT,
        tokio::net::lookup_host((host, port)),
    )
    .await
    .ok()?
    .ok()?
    .find(|a| {
        a.is_ipv6() == ipv6 && super::dht::valid_address(*a, false) && peer(*a, allow_local_network)
    })
}
