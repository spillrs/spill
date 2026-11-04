# Networking

## One port

spill uses a single port number, `listen` (default `6881`), for peer and tracker
traffic and the DHT:

- **TCP**: incoming peer connections.
- **UDP**: uTP peer connections, the DHT and replies from UDP trackers, on the same socket.

Both must be free when the daemon starts, even with the DHT disabled. Allow
inbound TCP and UDP through the host firewall. Without inbound reachability,
spill still downloads and uploads over connections it makes itself.

Local service discovery additionally shares multicast UDP port `6771` with other
clients when `allow_local_network = true`. Allow that port on the local network
if using LSD; it does not need router port forwarding.

## Peer transports

spill accepts TCP and uTP (BEP 29) peer connections. Outbound connections try
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

With `port_mapping = true` (the default), spill asks the IPv4 default gateway
for TCP and UDP mappings, first using NAT-PMP, then UPnP IGD. PCP is not yet
supported. The system-service example disables mapping; user and macOS
examples leave it enabled. An IPv6 listener does not use automatic mapping.
An IP-valued `port_mapping_gateway` overrides NAT-PMP gateway discovery. An
HTTP description URL instead skips NAT-PMP and SSDP, using that UPnP IGD directly.

spill requests the listen port as the external port for both protocols. If the
gateway grants a different number, trackers, new extension handshakes and DHT
announces advertise that external port. A port change does not trigger extra
announces: the next regular announce carries it. Different TCP and UDP external
ports are rejected, since spill advertises one port. When disabled or after a
failed mapping attempt, the advertised port remains the listen port.

NAT-PMP requests a two-hour lease and renews at half the granted lifetime.
UPnP also requests a two-hour lease and renews halfway through it. If the
router returns error 725 (only permanent leases supported), spill retries with
lease 0 and reasserts the mapping hourly. Failed attempts retry after 60 seconds,
doubling up to 30 minutes. Clean shutdown tries to remove mappings with a
two-second timeout.
Failures are logged and exposed in `spill health`;
they never fail a torrent or stop the daemon.

Gateway requests and SSDP multicast deliberately bypass `allow_local_network`:
these destinations are our own gateway, not endpoints supplied by torrents.
See [Troubleshooting](troubleshooting.md#nobody-can-connect-to-me) if mapping fails.

## Finding peers

| Source | What it is | Setting |
|---|---|---|
| Trackers | HTTP, HTTPS and UDP trackers listed in the torrent or magnet. | `trackers` |
| DHT | The distributed hash table (BEP 5/32), over IPv4 and IPv6. | `dht` |
| PEX | Peer lists exchanged with connected peers (BEP 11). | `pex` |
| LSD | Local service discovery on the LAN (BEP 14). | `allow_local_network` and per-torrent `dht` |
| Web seeds | Plain HTTP(S) servers holding the files (BEP 19). | `web_seeds` |

Trackers, DHT, PEX and web seeds are on by default, configurable per torrent.
LSD is off unless LAN access is enabled and follows the torrent's `dht` setting.
Private torrents always have DHT, PEX and LSD off.

HTTP connections use HTTP/1 without system proxies. HTTPS verifies certificates
against bundled Mozilla roots, not the OS trust store; this also applies to
[adding a torrent by URL](torrents.md).

### Trackers

- Trackers are grouped in tiers (BEP 12). Each valid magnet `tr=` gets its own
  tier in link order; duplicate URLs keep only their first occurrence, and the
  retained tracker list has no empty duplicate tiers.
  `.torrent` tiers retain their listed order without shuffling. UDP is preferred
  over an earlier HTTP(S) tracker on the same hostname, even across tiers.
- Working trackers use sequential fallback: try the next in the tier, then the
  next tier, on failure. Failed trackers move to the back of their tier.
  When a failed tracker's backoff expires, its retry can run alongside the next
  eligible working tracker, so the retry's timeout does not delay that tracker.
  Each tracker has at most one exchange in flight. Selection starts from the
  beginning again; a working tracker that is not due blocks lower-priority
  trackers. This is not an announce-to-all-tiers or all-trackers policy.
- Failed trackers wait before retrying and are skipped while waiting. Consecutive
  failures use `min(3600, 5 + failures² × 12.5)` seconds, rounded down: 17, 55,
  117, 205… seconds. A tracker error reply's retry interval can extend that wait.
  Success resets consecutive failures; diagnostic failure totals remain cumulative.
  HTTP 410 disables that tracker for the current announce lifecycle.
- Successful announce intervals have a 300-second minimum and no upper clamp.
  HTTP replies default to a 1800-second interval and 30-second `min interval`;
  UDP uses a 60-second minimum interval. Explicit HTTP minimum intervals are
  respected, including zero. `spill reannounce` can shorten the regular interval,
  but not a failure backoff or minimum interval. A pending completion event may
  bypass the successful tracker's minimum interval.
- Announces have a 30-second completion deadline; UDP connect and announce
  exchanges each have a 10-second inactivity deadline, without in-exchange
  retransmissions. UDP trackers share the session's UDP socket. A silent tracker
  only delays its own torrent. Stopped announces are best-effort within five
  seconds and select among trackers that acknowledged `started`.
- HTTP requests use compact peers, `no_peer_id=1`, `supportcrypto=1`, and
  `corrupt` (hash-failed payload bytes); `downloaded` excludes hash-failed bytes
  and is floored at zero. Tracker IDs are returned on later requests.
  Peer requests use `numwant=200`, or zero for stopped announces.
- HTTP trackers resolve DNS on every announce and ignore system proxies.
  Announces follow up to five HTTP(S) redirects, subject to the same local-network
  restrictions as the original URL. HTTPS-to-HTTP redirects are refused.
  Redirects use the supplied `Location` without copying the original query or
  sending a `Referer` header; stored tracker URLs are unchanged.
- Tracker failure reasons appear as the torrent's `last_error`; warnings appear
  as `tracker_message`. Both are shown in `get` and on the web page.

These choices follow the audited libtorrent 2.1.2 defaults for tracker tiers,
backoff and request scheduling, not full tracker parity. spill does not announce
separately per listen address or send private-torrent `ipv4`/`ipv6` overrides.
Metadata resolution starts a separate announce lifecycle, so failure history,
tracker IDs and keys are not carried into payload download. Keys are random per
announcer rather than derived from torrent/session/storage identity. spill does
not report `redundant` bytes or subtract them from `downloaded`. Input budgets,
URL validation and destination/redirect restrictions remain spill's own policy;
HTTP connection pooling, DNS and endpoint failover are not identical to
libtorrent's transports.

### DHT

The DHT uses the address families supported by `listen`, with separate IPv4
and IPv6 routing tables sharing one node ID. Good contacts are saved in the
catalog and reverified on the next start; `dht_bootstrap` lists the routers used to
join when no saved nodes answer. It starts lazily for public transfers using DHT
or magnet metadata resolution, never for private torrents or metainfo-only
rechecks. Router outages remain ordinary discovery failures and are retried.
Initialization failures fail affected executions and are retained until the
session is recreated (restart the daemon to retry).

The library's one-shot `download()` permits DHT with `default_routers()`:
`dht.libtorrent.org:25401`, `dht.transmissionbt.com:6881`,
`router.bittorrent.com:6881` and `router.utorrent.com:6881`. Eligible executions
therefore perform router DNS lookups and UDP discovery. Reusable library sessions
permit DHT only when their `NetworkOptions::dht` explicitly supplies capability.

IPv6-only listeners use IPv6 DHT contacts; dual-stack listeners use both families.
On multihomed IPv6 hosts, prefer a specific stable global `listen` address so
outgoing DHT queries keep the same source address.
Dual-stack bootstrap and refresh queries request both IPv4 and IPv6 referrals;
normal peer lookups request their transport's family. Contacts learned across
families are verified over their own transport before being used or saved.
Router names are resolved separately for each enabled family. Unresponsive
bootstrap attempts back off up to 15 minutes, independently for each family. Numeric IPv6 routers
use bracketed addresses, for example `"[2001:db8::1]:6881"` (replace this
example with a reachable router). Link-local and scoped DHT endpoints are not
supported. Outgoing DHT datagrams are limited to 1024 bytes, with at most 40
peers per reply. Query rate limits group IPv6 sources by /64.

### Local service discovery

BEP 14 finds nearby peers without a tracker or DHT bootstrap, including peers
that can resolve a trackerless magnet. The daemon enables it with
`allow_local_network = true`; library sessions must explicitly set both
`NetworkOptions::lsd` and `allow_local_network`. The per-torrent `dht` setting
gates LSD independently of whether the session has DHT capability.

spill joins `239.192.152.143:6771` and, for IPv6 or dual-stack listeners,
`[ff15::efc0:988f]:6771`. A specific listen address selects its interface;
an unspecified address selects up, multicast-capable interfaces, excluding
loopback and point-to-point links (such as PPP and many VPN tunnels). A specific
listen address explicitly selects its interface, including a point-to-point link.
Announcements go out on each selected interface with TTL/hop limit 1, advertising
the local listen port rather than a router-mapped port. A specific IPv6 listen
address is also used as the multicast source, even when that interface has
other IPv6 addresses. Interface checks run
every minute off the async executor. Sockets and memberships are rebuilt only
when the selected interfaces change or after setup or I/O errors. A failed
join skips only that interface and is retried at the next check; other
interfaces remain usable. A failed sending interface is left out until the
rebuild; successful interfaces keep their normal announce cadence. Socket failures are logged; `spill health` under
`network.lsd` reports errors unless an announce batch succeeds on at least one
interface. These failures do not stop torrents or make session shutdown fail.

New registrations announce immediately, bypassing the session-wide minute
limit so a magnet's first announce is not delayed past its metadata deadline.
Repeat announcements run every five minutes, with at most one repeat batch per
minute. Bulk registration can send several initial batches back to back, with
up to 20 hashes per packet. Repeat batches are round-robin scheduled. Stopping
removes the registration; BEP 14 has no stop message. Peer JSON identifies outbound LSD contacts with `source: "lsd"`.
Connections opened during metadata acquisition retain their discovery source
when handed to payload transfer. Incoming metadata peers report
`source: "metadata"` with `direction: "inbound"`.
Magnets accept inbound peers while resolving metadata, so a LAN peer can answer
an announcement by connecting immediately. spill advertises its listening port
in the metadata-phase extension handshake. Live connections are kept through
storage admission, without requiring the peer's listening port or another
BitTorrent handshake. Metadata discovery retains its
45-second deadline. A peer that neither connects nor announces within that
window may only be found on a later attempt.

## Local addresses

By default spill never connects out to private, loopback or link-local
addresses, whether it got them from a tracker, the DHT, PEX or a web seed URL.
This keeps a malicious torrent from making spill probe your local network.
Set `allow_local_network = true` to use LAN peers or trackers. Multicast,
broadcast, unspecified peer addresses and port 0 are always refused. LSD is the
multicast peer-discovery exception, only when enabled (router mapping also uses
SSDP as described above). Scoped link-local IPv6 peers learned by LSD retain
their interface scope. Incoming connections are accepted from any address.

## IPv4 and IPv6

- `listen = "0.0.0.0:6881"` (default): IPv4 only.
- `listen = "[::]:6881"`: IPv6 and IPv4 for peers, UDP trackers and the DHT.
- A specific IPv6 `listen` address: IPv6-only incoming peers, UDP trackers and DHT.
- Peers from trackers and PEX can be IPv4 or IPv6; spill connects to whichever
  your host can reach.
- IPv6 UDP trackers need an IPv6 or dual-stack `listen`.

## Peer connections

- Up to `peers` connections per torrent (default 50), and at most 8 peers are
  uploaded to at once per torrent.
- spill derives its peer ID prefix from its package version: `-sP` followed by
  base-36 major, minor and patch digits, a reserved `0`, and `-`.
  Extension handshakes and HTTP requests identify as `spill/VERSION`; this
  case-sensitive wire identifier is not the prose brand casing.
- If the same peer connects twice (for example, both sides dialed each other),
  one connection is dropped.
- Peers that send bad data are banned for the torrent; unhelpful peers are
  disconnected to make room for others.
- Malformed known peer messages disconnect the peer, including malformed BEP 52
  hash messages on v1 torrents. V2 connections follow stricter rules; see
  [v2 and hybrid torrents](v2.md#peer-protocol).

## Peer encryption

spill supports Message Stream Encryption / Protocol Encryption (MSE/PE) on
both TCP and uTP. It negotiates before the ordinary BitTorrent handshake and
supports RC4 streams and negotiated plaintext application traffic.

Set session-wide `peer_encryption` in daemon configuration:

- `"allowed"`: initiate plaintext first, accepting MSE from incoming peers.
- `"preferred"` (default): initiate MSE first.
- `"required"`: reject plaintext and require RC4 for the whole application stream.

`allowed` and `preferred` retry once in the alternate mode when the peer
closes or resets during negotiation. The retry uses the transport that connected;
malformed negotiation and timeouts do not trigger a retry. Once MSE succeeds,
a close during the subsequent BitTorrent handshake does not trigger plaintext
fallback. This is separate from the initial uTP-to-TCP connection fallback.
`required` never downgrades. When responding to MSE, both `allowed` and `preferred`
select RC4 if the peer offers it.

Peer JSON reports `encryption` as `"rc4"` or `"plaintext"`, independently of
`transport`. Negotiated plaintext reports `"plaintext"`. Web seeds report `null`,
not their HTTP/TLS security. A peer also reports `null` before its handshake or
when its live I/O observation is no longer available.

MSE is legacy traffic obfuscation, not modern secure transport: its DH/RC4
parameters are required for interoperability, it has no peer authentication or
authenticated encryption, and it does not protect against active interception.
It does not encrypt tracker or DHT traffic or hide IP addresses.

## Privacy notes

- Trackers, DHT nodes and peers see your IP address and what you download. This
  is inherent to BitTorrent.
- A magnet link announces its info hash (to the DHT, its trackers and, when
  enabled, local service discovery) before
  spill knows whether the torrent is private. Use a `.torrent` file for private
  trackers; private metadata from magnets is rejected.
- Peer encryption is obfuscation, not anonymity or authenticated transport; see
  [Peer encryption](#peer-encryption).
- Detailed tracker and web-seed observations expose full URLs, which may contain
  passkeys or other secrets, even when transport errors redact them.
