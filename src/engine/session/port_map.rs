//! Gateway traffic deliberately bypasses the torrent endpoint LAN filter: the
//! destination is our own gateway, including SSDP multicast, not remote metainfo.
use crate::protocol::{
    nat_pmp::{self, Reply},
    upnp,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::UdpSocket,
    sync::watch,
    time::{sleep, timeout},
};

#[derive(Clone, Debug)]
pub enum Gateway {
    Discover,
    NatPmp(SocketAddr),
    Upnp(reqwest::Url),
}
impl Gateway {
    pub fn parse(value: &str) -> io::Result<Self> {
        if value.contains("://") {
            return http_url(value).map(Self::Upnp);
        }
        let address = value
            .parse::<IpAddr>()
            .map(|ip| SocketAddr::new(ip, 5351))
            .or_else(|_| value.parse::<SocketAddr>())
            .map_err(io::Error::other)?;
        if !address.is_ipv4() || !unicast(address.ip()) || address.port() == 0 {
            return Err(io::Error::other(
                "port_mapping_gateway requires a unicast IPv4 address and nonzero port",
            ));
        }
        Ok(Self::NatPmp(address))
    }
}
fn unicast(ip: IpAddr) -> bool {
    let ip = ip.to_canonical();
    !ip.is_unspecified() && !ip.is_multicast() && ip != Ipv4Addr::BROADCAST
}
fn http_url(value: &str) -> io::Result<reqwest::Url> {
    let url = reqwest::Url::parse(value).map_err(io::Error::other)?;
    let valid_host = match url.host() {
        Some(url::Host::Ipv4(ip)) => unicast(ip.into()),
        Some(url::Host::Ipv6(ip)) => unicast(ip.into()),
        Some(url::Host::Domain(_)) => true,
        None => false,
    };
    if url.scheme() != "http"
        || !valid_host
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port_or_known_default() == Some(0)
        || url.fragment().is_some()
    {
        return Err(io::Error::other(
            "UPnP description requires an http URL with a unicast host",
        ));
    }
    Ok(url)
}
#[derive(Clone, Default, Serialize)]
pub struct Status {
    pub enabled: bool,
    pub protocol: Option<&'static str>,
    pub external_ip: Option<IpAddr>,
    pub external_tcp_port: Option<u16>,
    pub external_udp_port: Option<u16>,
    pub last_error: Option<String>,
    /// Unix timestamp in seconds.
    pub next_renewal: Option<u64>,
}
struct Nat {
    gateway: SocketAddr,
    source: IpAddr,
    internal: u16,
    tcp: bool,
    udp: bool,
}
impl Nat {
    async fn exchange(&self, request: &[u8]) -> io::Result<Reply> {
        let socket = UdpSocket::bind((self.source, 0)).await?;
        socket.connect(self.gateway).await?;
        let mut bytes = [0; 64];
        for millis in [250, 500, 1000] {
            socket.send(request).await?;
            let received = timeout(Duration::from_millis(millis), async {
                loop {
                    let n = socket.recv(&mut bytes).await?;
                    if let Some(reply) = nat_pmp::reply(&bytes[..n], request[1]) {
                        return match reply {
                            Reply::Refused(code) => {
                                Err(io::Error::other(format!("NAT-PMP result {code}")))
                            }
                            reply => Ok(reply),
                        };
                    }
                }
            })
            .await;
            if let Ok(result) = received {
                return result;
            }
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "NAT-PMP gateway did not answer",
        ))
    }
    async fn map(&self, opcode: u8, lifetime: u32) -> io::Result<(u16, u32)> {
        match self
            .exchange(&nat_pmp::mapping_request(
                opcode,
                self.internal,
                self.internal,
                lifetime,
            ))
            .await?
        {
            Reply::Mapping {
                internal,
                external,
                lifetime: granted,
            } if internal == self.internal && (lifetime == 0 || external != 0 && granted != 0) => {
                Ok((external, granted))
            }
            _ => Err(io::Error::other("invalid NAT-PMP mapping reply")),
        }
    }
    async fn renew(&mut self) -> io::Result<(IpAddr, u16, Duration)> {
        let ip = match self.exchange(&nat_pmp::address_request()).await? {
            Reply::Address(ip) => ip,
            _ => return Err(io::Error::other("invalid NAT-PMP address reply")),
        };
        let (tcp, tcp_lease) = self.map(2, 7200).await?;
        self.tcp = true;
        let (udp, udp_lease) = self.map(1, 7200).await?;
        self.udp = true;
        if tcp != udp {
            return Err(io::Error::other(
                "gateway granted different TCP and UDP ports",
            ));
        }
        Ok((
            ip.into(),
            tcp,
            Duration::from_secs(u64::from(tcp_lease.min(udp_lease))) / 2,
        ))
    }
}
struct Igd {
    client: reqwest::Client,
    service: String,
    control: reqwest::Url,
    local: SocketAddr,
    lease: u32,
    tcp: bool,
    udp: bool,
}
async fn body(mut response: reqwest::Response) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(io::Error::other)? {
        if bytes.len() + chunk.len() > 256 * 1024 {
            return Err(io::Error::other("UPnP response exceeds 256 KiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
fn soap_error(code: u16) -> io::Error {
    io::Error::other(format!("UPnP error {code}"))
}
impl Igd {
    async fn soap(
        &self,
        action: &str,
        args: &[(&str, &str)],
    ) -> io::Result<Result<BTreeMap<String, String>, u16>> {
        let (header, xml) = upnp::request(&self.service, action, args);
        let response = self
            .client
            .post(self.control.clone())
            .header("SOAPAction", header)
            .header("Content-Type", "text/xml; charset=\"utf-8\"")
            .body(xml)
            .send()
            .await
            .map_err(io::Error::other)?;
        let status = response.status();
        let parsed = upnp::response(&body(response).await?);
        match parsed {
            Err(0) => Err(io::Error::other(format!(
                "invalid UPnP SOAP response (HTTP {status})"
            ))),
            Ok(_) if !status.is_success() => Err(io::Error::other(format!("UPnP HTTP {status}"))),
            result => Ok(result),
        }
    }
    async fn add(&mut self, protocol: &str) -> io::Result<()> {
        loop {
            let port = self.local.port().to_string();
            let client = self.local.ip().to_string();
            let lease = self.lease.to_string();
            let result = self
                .soap(
                    "AddPortMapping",
                    &[
                        ("NewRemoteHost", ""),
                        ("NewExternalPort", &port),
                        ("NewProtocol", protocol),
                        ("NewInternalPort", &port),
                        ("NewInternalClient", &client),
                        ("NewEnabled", "1"),
                        ("NewPortMappingDescription", "spill"),
                        ("NewLeaseDuration", &lease),
                    ],
                )
                .await?;
            match result {
                Err(725) if self.lease != 0 => {
                    tracing::warn!("UPnP error 725: retrying with a permanent lease");
                    self.lease = 0;
                }
                result => return result.map(|_| ()).map_err(soap_error),
            }
        }
    }
    async fn renew(&mut self) -> io::Result<(IpAddr, u16, Duration)> {
        let fields = self
            .soap("GetExternalIPAddress", &[])
            .await?
            .map_err(soap_error)?;
        let ip = fields
            .get("NewExternalIPAddress")
            .ok_or_else(|| io::Error::other("missing UPnP external IP"))?
            .parse::<Ipv4Addr>()
            .map_err(io::Error::other)?;
        self.add("TCP").await?;
        self.tcp = true;
        self.add("UDP").await?;
        self.udp = true;
        Ok((ip.into(), self.local.port(), Duration::from_secs(3600)))
    }
    async fn delete(&self, protocol: &str, enabled: bool) -> io::Result<()> {
        if enabled {
            self.soap(
                "DeletePortMapping",
                &[
                    ("NewRemoteHost", ""),
                    ("NewExternalPort", &self.local.port().to_string()),
                    ("NewProtocol", protocol),
                ],
            )
            .await?
            .map_err(soap_error)?;
        }
        Ok(())
    }
}
enum Mapping {
    Nat(Nat),
    Upnp(Igd),
}
impl Mapping {
    async fn renew(&mut self) -> io::Result<(IpAddr, u16, Duration)> {
        match self {
            Self::Nat(nat) => nat.renew().await,
            Self::Upnp(igd) => igd.renew().await,
        }
    }
    async fn remove(&self, status: &watch::Sender<Status>) {
        let result = timeout(Duration::from_secs(2), async {
            match self {
                Self::Nat(nat) => {
                    let remove = async |opcode, enabled: bool| -> io::Result<()> {
                        if enabled {
                            nat.exchange(&nat_pmp::mapping_request(opcode, nat.internal, 0, 0))
                                .await?;
                        }
                        Ok(())
                    };
                    let (tcp, udp) = tokio::join!(remove(2, nat.tcp), remove(1, nat.udp));
                    tcp.and(udp)
                }
                Self::Upnp(igd) => {
                    let (tcp, udp) =
                        tokio::join!(igd.delete("TCP", igd.tcp), igd.delete("UDP", igd.udp));
                    tcp.and(udp)
                }
            }
        })
        .await;
        if let Err(error) = result.map_err(io::Error::other).and_then(|r| r) {
            tracing::warn!(%error, "port_mapping_removal_failed");
            status.send_modify(|s| s.last_error = Some(format!("mapping removal: {error}")));
        }
    }
}
async fn gateway() -> io::Result<SocketAddr> {
    #[cfg(target_os = "linux")]
    {
        let routes = tokio::fs::read_to_string("/proc/net/route").await?;
        let mut best = None;
        for line in routes.lines().skip(1) {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() < 8 || fields[1] != "00000000" {
                continue;
            }
            let flags = u32::from_str_radix(fields[3], 16).map_err(io::Error::other)?;
            if flags & 3 != 3 {
                continue;
            }
            let metric = fields[6].parse::<u32>().map_err(io::Error::other)?;
            let ip = u32::from_str_radix(fields[2], 16).map_err(io::Error::other)?;
            if best.is_none_or(|(m, _)| metric < m) {
                best = Some((metric, Ipv4Addr::from(ip.to_le_bytes())));
            }
        }
        best.map(|(_, ip)| SocketAddr::new(ip.into(), 5351))
            .ok_or_else(|| io::Error::other("no IPv4 default gateway"))
    }
    #[cfg(target_os = "macos")]
    {
        let output = tokio::process::Command::new("/sbin/route")
            .args(["-n", "get", "default"])
            .kill_on_drop(true)
            .output()
            .await?;
        let text = String::from_utf8(output.stdout).map_err(io::Error::other)?;
        let ip = text
            .lines()
            .find_map(|line| line.trim().strip_prefix("gateway:"))
            .ok_or_else(|| io::Error::other("no default gateway"))?
            .trim()
            .parse::<Ipv4Addr>()
            .map_err(io::Error::other)?;
        Ok(SocketAddr::new(ip.into(), 5351))
    }
}
async fn nat(address: SocketAddr, override_gateway: Option<SocketAddr>) -> io::Result<Mapping> {
    let gateway = match override_gateway {
        Some(gateway) => gateway,
        None => gateway().await?,
    };
    Ok(Mapping::Nat(Nat {
        gateway,
        source: address.ip(),
        internal: address.port(),
        tcp: false,
        udp: false,
    }))
}
async fn upnp(address: SocketAddr, description: Option<&reqwest::Url>) -> io::Result<Mapping> {
    let socket = UdpSocket::bind(SocketAddr::new(address.ip(), 0)).await?;
    let mut locations = Vec::new();
    if let Some(url) = description {
        locations.push(url.clone());
    } else {
        for request in upnp::search_request() {
            socket
                .send_to(request.as_bytes(), "239.255.255.250:1900")
                .await?;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut bytes = [0; 8192];
        while let Ok(received) =
            tokio::time::timeout_at(deadline, socket.recv_from(&mut bytes)).await
        {
            let (n, _) = received?;
            if let Some(location) = upnp::search_reply(&bytes[..n]).and_then(|s| http_url(&s).ok())
                && !locations.contains(&location)
            {
                locations.push(location);
            }
            if locations.len() == 32 {
                break;
            }
        }
    }
    // This client has no torrent endpoint resolver/filter: gateway LAN access is intentional.
    let client = crate::http::client()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(3))
        .local_address(address.ip())
        .build()
        .map_err(io::Error::other)?;
    let mut error = io::Error::other("no UPnP IGD answered SSDP discovery");
    for location in locations {
        let description = async {
            let response = client
                .get(location.clone())
                .send()
                .await
                .map_err(io::Error::other)?
                .error_for_status()
                .map_err(io::Error::other)?;
            let (service, control, base) = upnp::control_url(&body(response).await?)
                .ok_or_else(|| io::Error::other("no UPnP WAN connection service"))?;
            let base = match base {
                Some(base) => http_url(&base)?,
                None => location,
            };
            let control = http_url(base.join(&control).map_err(io::Error::other)?.as_str())?;
            let host = control.host_str().expect("validated HTTP host");
            let gateway = tokio::net::lookup_host((
                host,
                control.port_or_known_default().expect("HTTP port"),
            ))
            .await?
            .find(|a| a.is_ipv4() && unicast(a.ip()))
            .ok_or_else(|| io::Error::other("UPnP gateway has no unicast IPv4 address"))?;
            socket.connect(gateway).await?;
            Ok(Mapping::Upnp(Igd {
                client: client.clone(),
                service,
                control,
                local: SocketAddr::new(socket.local_addr()?.ip(), address.port()),
                lease: 7200,
                tcp: false,
                udp: false,
            }))
        }
        .await;
        match description {
            Ok(mapping) => return Ok(mapping),
            Err(failure) => error = failure,
        }
    }
    Err(error)
}
pub(super) async fn run(
    address: SocketAddr,
    gateway: Gateway,
    port: watch::Sender<u16>,
    status: watch::Sender<Status>,
    mut stop: watch::Receiver<bool>,
) {
    let mut mapping = None;
    let mut backoff = Duration::from_secs(60);
    loop {
        let attempt = async {
            if let Some(active) = &mut mapping {
                return timeout(Duration::from_secs(8), Mapping::renew(active))
                    .await
                    .map_err(io::Error::other)?;
            }
            let mut errors = Vec::new();
            let protocols: &[&str] = if matches!(gateway, Gateway::Upnp(_)) {
                &["upnp"]
            } else {
                &["nat-pmp", "upnp"]
            };
            for &protocol in protocols {
                let create = async {
                    match (&gateway, protocol) {
                        (Gateway::Upnp(url), _) => upnp(address, Some(url)).await,
                        (Gateway::NatPmp(gateway), "nat-pmp") => nat(address, Some(*gateway)).await,
                        (_, "nat-pmp") => nat(address, None).await,
                        _ => upnp(address, None).await,
                    }
                };
                let result = async {
                    mapping = Some(create.await?);
                    mapping.as_mut().expect("mapping created").renew().await
                };
                match timeout(Duration::from_secs(8), result)
                    .await
                    .map_err(io::Error::other)
                    .and_then(|r| r)
                {
                    Ok(mapped) => return Ok(mapped),
                    Err(error) => {
                        tracing::warn!(protocol, %error, "port_mapping_failed");
                        errors.push(format!("{protocol}: {error}"));
                        status.send_modify(|s| s.last_error = Some(errors.join("; ")));
                        if let Some(active) = &mapping {
                            active.remove(&status).await;
                        }
                        mapping = None;
                    }
                }
            }
            Err(io::Error::other(errors.join("; ")))
        };
        let result = tokio::select! {
            biased;
            _ = stop.changed() => break,
            result = attempt => result,
        };
        let delay = match result {
            Ok((ip, external, renewal)) => {
                port.send_replace(external);
                let protocol = if matches!(mapping, Some(Mapping::Nat(_))) {
                    "nat-pmp"
                } else {
                    "upnp"
                };
                status.send_replace(Status {
                    enabled: true,
                    protocol: Some(protocol),
                    external_ip: Some(ip),
                    external_tcp_port: Some(external),
                    external_udp_port: Some(external),
                    last_error: None,
                    next_renewal: Some(
                        (SystemTime::now() + renewal)
                            .duration_since(UNIX_EPOCH)
                            .expect("current epoch")
                            .as_secs(),
                    ),
                });
                tracing::info!(protocol, external_port = external, "port_mapping_active");
                backoff = Duration::from_secs(60);
                renewal
            }
            Err(error) => {
                tracing::warn!(%error, "port_mapping_failed");
                port.send_replace(address.port());
                if let Some(active) = mapping.take() {
                    active.remove(&status).await;
                }
                status.send_replace(Status {
                    enabled: true,
                    last_error: Some(error.to_string()),
                    ..Status::default()
                });
                let delay = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(1800));
                delay
            }
        };
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = sleep(delay) => {},
        }
    }
    if let Some(active) = mapping {
        active.remove(&status).await;
    }
}
