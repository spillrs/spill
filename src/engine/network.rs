//! Egress policy, applied to numeric endpoints after resolution and before I/O.
pub(in crate::engine) use std::net::{IpAddr, SocketAddr};

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
