# Interoperability tests

These are opt-in, dev-machine-only tests. Never run tier 2 or tier 3 in CI or a
cloud agent session. Use the machine's actual LAN IPv4 address in place of
`LAN_IP`. The harnesses never install packages. Public discovery is disabled;
trackers, DHT nodes, peers and HTTP sources are local fixtures.

Requires Python 3.11+, `findmnt`, a built spill daemon, and Transmission **4.1+**
(`transmission-daemon` on PATH). The libtorrent harnesses also require system
libtorrent-rasterbar **2.1+** Python bindings. Recorded client versions are
Transmission 4.1.3 and libtorrent 2.1.2.0.

## Validation records

`baselines/` contains compact records with source revisions, binary/helper hashes
and artifact paths. Each describes its recorded tree, not necessarily the
current checkout. The LSD record includes a working-tree source manifest and
local feature/regression checks; it does not revalidate every older matrix.
Tier 3 remains unrun for v2/hybrid support, encrypted swarms and multicast load,
and is a release gate under `AGENTS.md`.

| Scope | Record |
|---|---|
| V2 direct transfers, proofs, wire policy and recovery | [v2-direct.json](baselines/v2-direct.json) |
| Hybrid direct transfers in both wire modes | [hybrid-direct.json](baselines/hybrid-direct.json) |
| Trackers, DHT, PEX and daemon lifecycle | [discovery-daemon.json](baselines/discovery-daemon.json) |
| Magnet topics, acquisition, rebuild and restart | [magnets.json](baselines/magnets.json) |
| HTTP web seeds and mixed peer/HTTP downloads | [webseeds.json](baselines/webseeds.json) |
| Created torrents and padding compatibility | [creation.json](baselines/creation.json) |
| MSE/PE modes, fallback, rejection, magnets and recovery (17 checks) | [encryption.json](baselines/encryption.json) |
| LSD IPv4/IPv6 magnets and transfers, Transmission batching, discovery/recovery regressions | [lsd.json](baselines/lsd.json) |
| Live metadata handoff, IPv6 LSD rerun, full libtorrent matrices and Transmission TCP/uTP/encrypted recovery | [metadata-handoff.json](baselines/metadata-handoff.json) |

The magnet topic record includes both btmh-only hybrid fixtures passing with
full-identity handshake acceptance, plus btih-only and dual-topic cases. That
run used an uncommitted working tree; its exact provenance is retained in the
record. It is not a substitute for validation of the final merged tree.

Full logs and payloads remain under `target/interop/`. Use short artifact paths:
long daemon control-socket paths can exceed the Unix-socket path limit. Delete
run directories when their payloads are no longer needed. These are correctness
checks, not throughput, latency or public-swarm capacity claims.

Raw plaintext fixtures in the shared daemon harness and protocol-focused engine
suites explicitly select `allowed`. The default `preferred` negotiation and
plaintext fallback are exercised in `tests/engine/encryption.rs`; the encryption
record adds independent Transmission/libtorrent evidence, not general coverage
of the default policy in every scenario.

## Tier 2: Transmission

```sh
# Transfer in both directions and compare SHA-256.
python3 tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release

# Observe uTP in both directions; TCP fallback alone cannot pass.
python3 tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --transport utp --size 1GiB --piece-length 1MiB

# Require RC4 in both clients, recording encryption from peer diagnostics.
# Repeat with --transport utp; also exercise --magnet and --kill-at 0.5.
python3 tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --peer-encryption required --transmission-encryption required \
  --size 16MiB --rate-limit 1MiB

# Hybrid magnet, background layer rebuild and source-free restart.
python3 tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --hybrid --magnet --http-tracker --restart-check

# Private torrent and completed-payload recovery.
python3 tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --private --http-tracker --restart-check

# SIGKILL during download, then recover and seed back.
python3 tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --size 1GiB --piece-length 1MiB --kill-at 0.5

python3 tests/interop/transmission_smoke.py --help
```

TCP is the default. uTP runs require observing uTP peers in spill's diagnostics
in both directions. Use a large enough fixture to span diagnostic sampling;
`--rate-limit 1MiB` caps spill's upload and download payload rates for small
fixtures. Leave the cap unset for throughput comparisons.

`--magnet` uses tracker-only metadata discovery. `--http-tracker` selects a local
HTTP tracker instead of UDP. `--hybrid` generates a single-file hybrid with
independently computed SHA-256 layers. Transmission supplies v1 data; the test
waits for background layer publication before checking the catalog and
restarting. `--private` requires HTTP tracking and cannot be used with a magnet.
`--restart-check` checks completed-payload restart and mtime-triggered
revalidation with source discovery disabled. `--download-only` skips reverse
seeding. Pairing spill `required` with Transmission `allowed` requires this flag:
Transmission may initiate plaintext, which spill must reject. Use Transmission
`preferred` or `required` for bidirectional encrypted transfers. Transfer direction
does not determine which client initiates the connection.

DHT, PEX, LPD and port mapping are disabled. Encryption defaults to `allowed` in
both clients so plaintext baselines remain available. Automatic public-IP HTTP
probes are blocked through a closed loopback proxy; LAN and loopback traffic
bypass it. TCP-only runs disable uTP in both clients. The harness requires
Transmission 4.1's `--preferred-transports` option.

`transport-samples.jsonl` records elapsed time, observed transports, payload
progress and spill's outgoing uTP retransmit count about once per second.
`result.json` retains direction summaries, including on timeout. Retransmit
counts are spill's, not Transmission's, and samples are not packet traces.
Generated payloads are removed after success unless `--keep` is supplied.
Tmpfs requires `--allow-tmpfs` and is unsuitable for disk measurements.

## Tier 2: libtorrent direct peers

```sh
python3 -c 'import libtorrent as lt; print(lt.__version__)'
python3 tests/interop/libtorrent_v2.py --allow-network --host LAN_IP --release

# Select a group; --fixture NAME can further restrict fixtures.
python3 tests/interop/libtorrent_v2.py --allow-network --host LAN_IP --release --scenario smoke
python3 tests/interop/libtorrent_v2.py --allow-network --host LAN_IP --release --scenario proofs
python3 tests/interop/libtorrent_v2.py --allow-network --host LAN_IP --release --scenario restart
python3 tests/interop/libtorrent_v2.py --allow-network --host LAN_IP --release --scenario corruption
python3 tests/interop/libtorrent_v2.py --allow-network --host LAN_IP --release --scenario hybrid
python3 tests/interop/libtorrent_v2.py --allow-network --host LAN_IP --release --scenario policy
```

The public-library adapter is the `interop-engine` Cargo example in `engine.rs`.
It does not depend on the daemon or test-only engine APIs. TCP is plaintext for
wire capture; uTP, encryption, trackers, DHT, PEX, LPD and port mapping are
disabled. Payloads follow the committed fixtures' deterministic byte pattern.
Native paths are checked separately for both clients, including spill's nested
file-tree placement. A one-byte zero file may verify from provisioning without
network transfer; counters distinguish that from an actual download.

The matrix covers:

- V2 and hybrid transfers in both directions, file tails, 16 KiB and 16 MiB
  pieces, selective files sharing roots, and both hybrid wire modes.
- Independent byte-for-byte hash-response comparisons, including left/right
  ranges and lengths 2, 4 and 512. Spill's verifier is not the proof oracle.
- Queued proofs across Choke, unsolicited-piece disconnection, pending-request
  rejects and choked-peer upload-budget isolation.
- Payload mutation after verification: libtorrent asks for leaf hashes, spill
  rejects unauthenticated data, and an honest source repairs the download.
  The relay closes the leaf probe after forwarding its correlated reject to
  bound immediate retries; neither changed spill seed may fail.
- Partial SIGKILL/recovery, source-free completed restart, reverse seeding and
  offline recheck.

Hybrid short-tail requests may include BEP 47 padding zeros in either wire mode;
these bytes are covered by SHA-1. Pure-v2 requests remain limited to file data.
Libtorrent's [`torrent::to_req`](https://github.com/arvidn/libtorrent/blob/v2.1.2/src/torrent.cpp#L1604)
uses [`piece_size_for_req`](https://github.com/arvidn/libtorrent/blob/v2.1.2/include/libtorrent/torrent_info.hpp#L539),
which selects v1 piece size for hybrids regardless of wire mode. Upgraded
hybrids retain all other v2 duties.

### Recorded limitations and compatibility policy

- Libtorrent 2.1.2.0 rejects the all-empty fixture with `invalid length of
  torrent [libtorrent:9]`. It is recorded as unsupported, not a passed transfer.
- Without Fast, libtorrent 2.1.2.0 leaves eight of nine requests unanswered after
  Choke. The check pins this divergence to that version. Spill answers requests
  in both modes; spill and libtorrent negotiate Fast with each other.
  [`write_reject_request`](https://github.com/arvidn/libtorrent/blob/v2.1.2/src/bt_peer_connection.cpp#L389)
  returns without Fast; both
  [`send_choke`](https://github.com/arvidn/libtorrent/blob/v2.1.2/src/peer_connection.cpp#L3821)
  and [`incoming_cancel`](https://github.com/arvidn/libtorrent/blob/v2.1.2/src/peer_connection.cpp#L3304)
  call it. Other versions or changed observations are not exempted.
- A non-Fast peer leaving requests unanswered is disconnected at
  `REQUEST_TIMEOUT`. Authorized responses after Cancel or Choke remain valid
  until answered or disconnected; retention is bounded. Tier 1 covers omitted
  rejects, late pieces, duplicate/cancel handling and upload-budget accounting.

Artifacts live under `target/interop/libtorrent-v2-*` or `--artifacts PATH`.
`result.json` records source and binary provenance, checks and failures.
`*-wire.jsonl`, `proofs.json`, `policies.json`, `duties-*.json`, client alerts and
engine samples retain evidence even when a check fails.

## Tier 2: discovery and daemon lifecycle

```sh
python3 tests/interop/libtorrent_discovery.py --allow-network --host LAN_IP --release
python3 tests/interop/libtorrent_discovery.py --allow-network --host LAN_IP --release --scenario trackers
python3 tests/interop/libtorrent_discovery.py --allow-network --host LAN_IP --release --scenario dht
python3 tests/interop/libtorrent_discovery.py --allow-network --host LAN_IP --release --scenario pex
python3 tests/interop/libtorrent_discovery.py --allow-network --host LAN_IP --release --scenario restart
```

This matrix uses the daemon's public control API. HTTP and UDP tracker transfers
isolate each hybrid swarm and check per-swarm completion delivery across restart.
DHT transfers use a real libtorrent node, a local bootstrap responder and an
opaque UDP relay recording actual swarm keys. Reverse discovery must find spill
while the original seed is paused. Public bootstrap is disabled.

The PEX case uses a libtorrent intermediary to advertise a raw v1-only seed over
an upgraded connection. The relay withholds intermediary payload, so spill must
reach the advertised seed. Native outgoing spill PEX must share the endpoint
back over the v2 connection. Libtorrent is the PEX oracle; the legacy seed is a
raw peer, not Transmission.

Lifecycle checks cover aliases, duplicate add, pause, restart, resume, recheck,
reverse seeding and removal. Generated v2 and hybrid downloads are SIGKILLed
partway through, recovered to matching checksums, and restarted without source
discovery. Hash responses must match libtorrent byte-for-byte.

LPD, port mapping, encryption and uTP are disabled. Local-address and per-IP
restrictions in libtorrent are relaxed for the isolated same-machine network.
Artifacts are under `target/interop/lt-discovery-*` or `--artifacts PATH`.
`result.json` and per-case `case.json` retain provenance and partial results.

## Tier 2: local service discovery

```sh
python3 tests/interop/libtorrent_lsd.py --allow-network --host LAN_IP --release
```

Runs v1, v2-only and hybrid trackerless magnets and payload transfers in both
directions, with libtorrent binding port 6771 first. Trackers, DHT bootstrap,
PEX and port mapping cannot supply peers. Records peer provenance (`lsd`,
`metadata` or `inbound`), LSD counters and payload hashes under `target/interop/`.
The reverse direction pauses and resumes the seed after registering the magnet
to trigger a fresh announce. Use `--format v1`, `v2` or `hybrid` to narrow the run.

`--host` also accepts a numeric LAN IPv6 address. Repeat `--peer-host` to bind
libtorrent to several local addresses while spill listens on one, for example:

```sh
python3 tests/interop/libtorrent_lsd.py --allow-network --host SPILL_IPV6 \
  --peer-host PRIMARY_IPV6 --peer-host SPILL_IPV6
```

The peer addresses share one port. This covers source-address selection on a
multi-address interface. On hosts with a DHCPv6 /128 alongside a SLAAC /64,
bind libtorrent to both: it filters incoming LSD by each listener's subnet,
while its outgoing connections can use the kernel's preferred source address.
The [LSD record](baselines/lsd.json) covers libtorrent 2.1.2 in both IP families
and Transmission 4.1.3 over IPv4, including all 20 headers in one announcement.
In the recorded IPv6 topology, metadata resolved in about 50 ms but first
payload waited about 121 seconds. The saved `lsd-confirmed-libtorrent-v6` artifacts
predate live-connection handoff; checksums passed. Reviewing those artifacts
shows the same sequence for v1, v2 and hybrid:

- The metadata core disconnected its peers on resolution. libtorrent's extended
  handshake omitted `p`, so the inbound connection supplied no redial endpoint.
- `daemon/state/debug.log` shows the payload runtime exiting with `NoProgress`
  after 60 seconds. `libtorrent-seed/alerts.jsonl` shows a retry at about
  60.4 seconds reset by spill, before the replacement runtime starts. The next
  retry at about 120.4 seconds connects; `daemon/observations.jsonl` records
  payload at about 121 seconds.
- The final health snapshots report received LSD packets, but do not identify
  matching contacts. The saved daemon filters exclude trace-level dial events.
  They cannot establish whether spill dialed libtorrent's LSD endpoint, which
  source address it carried, or how that dial failed. Do not infer an IPv6
  routing failure from the delay alone.

To validate the handoff in that topology, repeat the IPv6 command above with
`RUST_LOG=warn,spill=debug,spill::engine::torrent=trace` and retain the daemon
logs alongside the observations and libtorrent alerts. `peer_dial_started`
records contact provenance and `dial_failed` records the error kind and OS
code; correlate them by dial ID. `RUST_LOG` affects the captured console log,
not `state/debug.log`. Endpoint logging is off in the daemon, so determining
an alternate source address also requires a local packet capture.

The engine now preserves live metadata connections, their negotiated extensions
and early piece/choke state through storage admission into payload transfer.
The engine regression uses an inbound peer that omits `p`, has no listener and
never reconnects, with and without the fast extension. The
[handoff record](baselines/metadata-handoff.json) reruns the original multi-address
IPv6 matrix: all three formats completed both directions with matching hashes.
First payload appeared in status samples about one second after the first sample,
with one inbound connection per format, rather than the earlier 121-second wait.
These are local observations including polling delay, not a general latency claim.

The `NoProgress` restart gap remains a separate issue: inbound connections can
be reset between runtime exit and the supervisor starting its replacement.
Connection handoff does not remove that general reconnect window.

A tier 3 dev-machine check must also hold destination preparation/recovery open
on a large existing payload for several minutes, spanning spill's 180-second
peer idle timeout. Check both connections that survive preparation and peers
that time out: discovery or subsequent inbound retries must still allow payload
completion, with matching checksums and clean stop/shutdown during preparation.
This slow-preparation check has not been run.

macOS, Transmission IPv6 LSD, multiple physical interfaces, cross-user socket
sharing and interface churn still need separate dev-machine checks.

## Tier 2: magnets

```sh
python3 tests/interop/libtorrent_magnets.py --allow-network --host LAN_IP --release
python3 tests/interop/libtorrent_magnets.py --allow-network --host LAN_IP --release --scenario magnets
python3 tests/interop/libtorrent_magnets.py --allow-network --host LAN_IP --release --scenario retry
python3 tests/interop/libtorrent_magnets.py --allow-network --host LAN_IP --release --scenario acquisition
python3 tests/interop/libtorrent_magnets.py --allow-network --host LAN_IP --release --scenario restart
python3 tests/interop/libtorrent_magnets.py --allow-network --host LAN_IP --release --scenario layers
```

The matrix covers v2-only btmh magnets and both hybrid fixtures with btih-only,
btmh-only and dual topics, in both directions. A pending hybrid checks SHA-1,
announces only in v1, and accepts either handshake key without offering an
upgrade. It downloads without hash requests, rebuilds layers locally and serves
them after restart. Btmh-only bootstrap may use a v2 metadata connection.

V2-only magnets authenticate layers before payload preparation. Before forwarding
a hash request, the relay injects Interested/NotInterested if needed and waits
for libtorrent's native Choke. It forges neither Choke nor hash responses.
Checks include complete downloads, stable IDs, aliases, source-free restart and
byte-for-byte proofs. Reverse libtorrent starts with a magnet, not metainfo.

Fault cases reject, corrupt or withhold hash traffic before discovery supplies a
separate honest source. The stalled route remains until spill closes it at
roughly 30 seconds; tracker updates respect spill's 30-second minimum interval.
Independent source sessions avoid duplicate-peer rejection. A mixed case uses a
v1-key-only relay and must complete without v2 peers or hash exchange. The
Transmission hybrid case supplies an independent v1-only client.

Acquisition pause/SIGKILL must create no payload, and restart must resolve again.
Generated v2 and hybrid downloads are also killed after 1 MiB verified, recovered
to matching checksums, and restarted to serve proofs without source discovery.

**Singleton compatibility:** BEP 52 specifies a minimum request length of two.
Spill requests at least two, but serves valid singletons for libtorrent.
Libtorrent's [`pick_hashes`](https://github.com/arvidn/libtorrent/blob/v2.1.2/src/hash_picker.cpp#L210-L214)
rounds the final range to a power of two without imposing the minimum. Its
[`on_hashes` check](https://github.com/arvidn/libtorrent/blob/v2.1.2/src/bt_peer_connection.cpp#L1268-L1275)
expects `proof_layers + 1` siblings for a singleton (`proof_layers + 2` hashes
including the requested hash). Range, layer, correlation, upload-grant and
resource limits still apply.

The generated 1025-piece fixture uses 16 KiB pieces. Spill requests 512, 512 and a
padded pair; libtorrent requests 512, 512 and one. Its final `(0, 1024, 1, 9)`
reply has eleven hashes, anchored in an earlier response. A standalone full-root
singleton needs proof count ten and twelve hashes. After restart, singletons at
indices 0, 17 and 1024 are compared byte-for-byte with libtorrent's oracle.
Artifacts are under `target/interop/lt-magnets-*` or `--artifacts PATH`.

## Tier 2: web seeds and torrent creation

```sh
python3 tests/interop/libtorrent_webseeds.py --allow-network --host LAN_IP --release
python3 tests/interop/libtorrent_padding.py --allow-network --host LAN_IP --release
```

Web-seed cases cover v2-only and both hybrid fixtures, short tails, padding
exclusion, UTF-8 and spaces. Spill and libtorrent must download matching payloads
through the same percent-encoded HTTP URLs. Escape casing may differ. A peer-only
case uses the HTTP-fed libtorrent seed. A generated 2 MiB + 123 byte mixed case
requires HTTP requests, positive libtorrent peer uploads and more total received
bytes than peer uploads. This proves both sources contribute, not necessarily to
the same piece. Results are under `target/interop/lt-webseeds-*/result.json`.

Creation records cover v2/hybrid directory and single-file torrents, independent
identity checks and transfers in both directions. The padding harness compares
spill and libtorrent generators with three equal-length files. Both produce
repeated `.pad/32645` paths; Transmission 4.1.3 rejects both with `duplicate path`.
That comparison confirms a compatibility limitation, not successful transfer.
Transmission seed verification uses its native v1 layout, with zero-filled
padding files materialized where required.

## Tier 3: disk and swarm workloads

Tier 3 remains unrun for v2/hybrid support. Before release, exercise many
simultaneous torrents, tiny files/roots, large layers, leaf-proof pressure under
rate caps, restart/recheck memory bounds, background rebuild I/O and cancellation,
partial selection and long seeding. Compare matching workloads with v1 records.

```sh
# 33 torrents, local HTTP sources, completed soak and seeds-only restart.
python3 tests/interop/scale.py multi-torrent --allow-network --release
python3 tests/interop/scale.py multi-torrent --allow-network --release \
  --piece-length 16777216 --dead-udp-tracker

# Provisioning/checkpoint workloads: default 60,001 files or the admitted limit.
python3 tests/interop/scale.py many-files --allow-network --release
python3 tests/interop/scale.py many-files --allow-network --release --files 100000

# Peer-wire swarm in either transport.
python3 tests/interop/swarm.py --allow-network --host LAN_IP --release
python3 tests/interop/swarm.py --allow-network --host LAN_IP --release --transport utp
python3 tests/interop/swarm.py --allow-network --host LAN_IP --leecher transmission

# RC4 workload for the pending release check; repeat with --transport utp.
python3 tests/interop/swarm.py --allow-network --host LAN_IP --release \
  --peer-encryption preferred --size 1GiB --timeout 600

# Optional isolated Transmission reference timing.
python3 tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --size 1GiB --piece-length 1MiB --download-only --reference
```

`scale.py` needs no Transmission. It uses only local HTTP range sources and
measures storage, scheduling, catalog and control behavior, not peer-wire policy.
`many-files` creates empty files plus one payload, checks a `40 × file_count`
fingerprint, and recovers with sources disabled. Multi-torrent defaults are one
256 MiB torrent plus 32 × 16 MiB, four download slots, 4 MiB/s per torrent and a
60-second completed soak. A silent UDP sink exercises independent tracker
transactions while downloads and control requests continue. Use `--files`,
`--piece-length`, `--soak-seconds`, `--timeout` and `--keep` to select workloads.
Results retain checksums, timings, HTTP requests, RSS, descriptors, control
latency, restart and cleanup evidence.

`swarm.py` uses one Transmission seed and 1–32 leechers behind a local HTTP
tracker. Web seeds, DHT, PEX, LPD and port mapping are disabled. Peer encryption
defaults to `allowed` for plaintext baseline comparisons. `--peer-encryption`
selects the policy for every member; `preferred` and `required` runs must observe
RC4 for every leecher. The
default is eight spill leechers, each downloading 256 MiB with 1 MiB pieces and
no bandwidth caps. More than one leecher requires observed multi-peer
connections; uTP requires an observed uTP peer for every spill leecher.
`peer-samples.jsonl` retains live diagnostics, including choke state, requests,
upload grants and writer/read queues. Samples refresh about every two seconds;
they are not packet traces or measurements of request-pipeline depth.

Before release, the encrypted-swarm gate still requires sustained `preferred`
TCP/uTP runs with reconnect churn, recording CPU, throughput and memory. The
swarm command above enables the encrypted workload; it is not itself a completed
soak or handshake-flood measurement. Keep this gate open until those dev-machine
results are recorded.

Use fresh artifact directories and otherwise idle machines for comparisons.
`result.json` retains workload, source/binary provenance and measurement details.
Tmpfs requires explicit permission. Neither runner drops filesystem caches.
Spill completion includes durability; Transmission's `percentDone` is not the
same guarantee. Keep full elapsed timings distinct from sampled payload rates.
Historical disk/swarm records in `baselines/` describe their own workloads, not
validation of the current v2/hybrid tree.
