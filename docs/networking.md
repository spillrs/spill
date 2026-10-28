# Networking

## One port

Spill uses a single port number, `listen` (default `6881`), for everything
inbound:

- **TCP**: incoming peer connections.
- **UDP**: uTP peer connections, the DHT and replies from UDP trackers, on the same socket.

Both must be free when the daemon starts, even with the DHT disabled. Allow
inbound TCP and UDP through the host firewall. Without inbound reachability,
Spill still downloads and uploads over connections it makes itself.

## Peer transports

Spill accepts TCP and uTP (BEP 29) peer connections. Outbound connections try
uTP first, then TCP if uTP does not answer within three seconds. A failed uTP
attempt is remembered for that contact while its torrent runs; subsequent dials
use TCP directly. TCP and uTP are not raced.

uTP uses LEDBAT delay-based congestion control, intended to yield bandwidth to
other traffic. Selective acknowledgements and retransmission recover reordered
or lost datagrams. Vectored peer writes are packed together into datagrams.
Zero-window probes use exponential backoff, capped at 60 seconds between probes.
Both transports use the same peer and pending-handshake limits.
Peer diagnostics identify each connection's `transport` as `"tcp"`, `"utp"` or
`"http"` (web seed).

Set daemon config `utp = false` to dial only TCP and reject incoming uTP.
UDP is still required for trackers and the DHT. Forward and allow both TCP and
UDP on the listen port to accept both peer transports. IPv6 uTP requires an
IPv6 or dual-stack listener; an IPv4-only listener cannot send IPv6 uTP.
A dual-stack (`[::]`) listener supports both IPv4 and IPv6 uTP.

A full local uTP connection budget causes that dial to use TCP, without marking
the remote as uTP-incompatible. Only a uTP connect timeout or reset is remembered
as a remote failure. During clean daemon shutdown, uTP gets up to one second to
acknowledge FINs before remaining connections are reset.

## Automatic port mapping

With `port_mapping = true` (the default), Spill asks the IPv4 default gateway
for TCP and UDP mappings, first using NAT-PMP, then UPnP IGD. PCP is not yet
supported. The system-service example disables mapping; user and macOS
examples leave it enabled. An IPv6 listener does not use automatic mapping.
An IP-valued `port_mapping_gateway` overrides NAT-PMP gateway discovery. An
HTTP description URL instead skips NAT-PMP and SSDP, using that UPnP IGD directly.

Spill requests the listen port as the external port for both protocols. If the
gateway grants a different number, trackers, new extension handshakes and DHT
announces advertise that external port. A port change does not trigger extra
announces: the next regular announce carries it. Different TCP and UDP external
ports are rejected, since Spill advertises one port. When disabled or after a
failed mapping attempt, the advertised port remains the listen port.

NAT-PMP requests a two-hour lease and renews at half the granted lifetime.
UPnP also requests a two-hour lease and renews halfway through it. If the
router returns error 725 (only permanent leases supported), Spill retries with
lease 0 and reasserts the mapping hourly. Failed attempts retry after 60 seconds,
doubling up to 30 minutes. Clean shutdown tries to remove mappings with a
two-second timeout.
Failures are logged and exposed in `spill ctl health` and the web Health panel;
they never fail a torrent or stop the daemon.

Gateway requests and SSDP multicast deliberately bypass `allow_local_network`:
these destinations are our own gateway, not endpoints supplied by torrents.
See [Troubleshooting](troubleshooting.md#nobody-can-connect-to-me) if mapping fails.

## Finding peers

| Source | What it is | Setting |
|---|---|---|
| Trackers | HTTP, HTTPS and UDP trackers listed in the torrent or magnet. | `trackers` |
| DHT | The distributed hash table (BEP 5), IPv4 only. | `dht` |
| PEX | Peer lists exchanged with connected peers (BEP 11). | `pex` |
| Web seeds | Plain HTTP(S) servers holding the files (BEP 19). | `web_seeds` |

All four are on by default, configurable per torrent. Private torrents always
have DHT and PEX off.

### Trackers

- Trackers are grouped in tiers (BEP 12). Spill announces to one working
  tracker at a time, trying the next in the tier, then the next tier, when one
  fails. A tracker that answers moves to the front of its tier.
- Announces follow the tracker's `interval` and `min interval`.
  `spill ctl reannounce` asks for an early announce within those limits.
- UDP trackers share the session's UDP socket. A lost request is resent after
  15 seconds, then 30, within a 45-second limit. A silent tracker only delays
  its own torrent.
- HTTP trackers resolve DNS on every announce, never follow redirects, and
  ignore system proxies.
- Tracker failure reasons appear as the torrent's `last_error`; warnings appear
  as `tracker_message`. Both are shown in `get` and on the web page.

### DHT

The DHT needs an IPv4 `listen` address. Its routing table is saved in the
catalog and reused on the next start; `dht_bootstrap` lists the routers used to
join when no saved nodes answer. If the DHT cannot start (for example, after
switching `listen` to IPv6), Spill logs it and runs without the DHT.

## Local addresses

By default Spill never connects out to private, loopback or link-local
addresses, whether it got them from a tracker, the DHT, PEX or a web seed URL.
This keeps a malicious torrent from making Spill probe your local network.
Set `allow_local_network = true` to use LAN peers or trackers. Multicast,
broadcast, unspecified addresses and port 0 are always refused. Incoming
connections are accepted from any address.

## IPv4 and IPv6

- `listen = "0.0.0.0:6881"` (default): IPv4 only.
- `listen = "[::]:6881"`: IPv6 and IPv4 for peers and UDP trackers. The DHT is
  unavailable, because it needs an IPv4 listen address.
- Peers from trackers and PEX can be IPv4 or IPv6; Spill connects to whichever
  your host can reach.
- IPv6 UDP trackers need an IPv6 or dual-stack `listen`.

## Peer connections

- Up to `peers` connections per torrent (default 50), and at most 8 peers are
  uploaded to at once per torrent.
- Spill identifies as `-sP0500-` in its peer ID (`sP` plus base-36 version
  digits) and as `Spill/VERSION` in extension handshakes and HTTP requests.
- If the same peer connects twice (for example, both sides dialed each other),
  one connection is dropped.
- Peers that send bad data are banned for the torrent; unhelpful peers are
  disconnected to make room for others.

## Privacy notes

- Trackers, DHT nodes and peers see your IP address and what you download. This
  is inherent to BitTorrent.
- A magnet link announces its info hash (to the DHT and its trackers) before
  Spill knows whether the torrent is private. Use a `.torrent` file for private
  trackers; private metadata from magnets is rejected.
- Spill does not encrypt peer connections.
