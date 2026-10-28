//! Egress policy, applied to numeric endpoints after resolution and before I/O.
pub(in crate::engine) use std::net::{IpAddr, SocketAddr};

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

pub(in crate::engine) fn peer(address: SocketAddr, allow_local: bool) -> bool {
    source(address, allow_local) && (allow_local || address.port() >= 1024)
}

pub(in crate::engine) fn eligible(address: SocketAddr) -> bool {
    source(address, true)
        && match address.ip() {
            IpAddr::V6(ip) => ip.to_ipv4_mapped().is_none() && !ip.is_unicast_link_local(),
            _ => true,
        }
}

pub(in crate::engine) async fn resolve_v4(
    host: &str,
    port: u16,
    allow_local_network: bool,
) -> Option<std::net::SocketAddrV4> {
    tokio::time::timeout(
        crate::engine::profile::DNS_TIMEOUT,
        tokio::net::lookup_host((host, port)),
    )
    .await
    .ok()?
    .ok()?
    .find_map(|a| match a {
        SocketAddr::V4(a) if peer(a.into(), allow_local_network) => Some(a),
        _ => None,
    })
}
