//! Egress policy, applied to numeric endpoints after resolution and before I/O.
pub(in crate::engine) use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
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
    crate::http::source(address, allow_local)
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
        .dns_resolver(crate::http::resolver(allow_local_network))
        .connect_timeout(crate::engine::profile::CONNECT_TIMEOUT)
}
pub(in crate::engine) fn http_destination_allowed(url: &Url, allow_local_network: bool) -> bool {
    crate::http::destination_allowed(url, allow_local_network)
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
