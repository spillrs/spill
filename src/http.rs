//! Shared HTTP transport defaults and egress filtering for the engine and daemon URL ingestion.
use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use url::Url;

const DNS_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn source(address: SocketAddr, allow_local: bool) -> bool {
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

pub(crate) fn destination_allowed(url: &Url, allow_local_network: bool) -> bool {
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

// Filter the actual connection destinations, with no secondary unfiltered lookup.
#[derive(Debug)]
struct Resolver(bool);
impl reqwest::dns::Resolve for Resolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let allow_local_network = self.0;
        Box::pin(async move {
            let addresses =
                tokio::time::timeout(DNS_TIMEOUT, tokio::net::lookup_host((name.as_str(), 80)))
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

pub(crate) fn resolver(allow_local_network: bool) -> Arc<dyn reqwest::dns::Resolve> {
    Arc::new(Resolver(allow_local_network))
}

pub(crate) fn client() -> reqwest::ClientBuilder {
    // Explicit configuration avoids process-global providers and system trust stores.
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("ring supports default TLS versions")
    .with_root_certificates(rustls::RootCertStore::from_iter(
        webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
    ))
    .with_no_client_auth();
    reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .http1_only()
        .no_proxy()
}
