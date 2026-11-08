# Configuration

spill reads a TOML 1.1 file only when started with `spill serve --config PATH`.
Without it, every key below takes its default. The file is limited to 64 KiB.

Rules that apply to the whole file:

- Paths must be absolute. TOML does not expand `~` or environment variables.
- Unknown keys are errors, so typos are caught at startup.
- Invalid values stop the daemon at startup with an error.
- `[defaults]` is copied into each torrent when it is added. Changing it later
  does not affect torrents that already exist; use `spill set` for those.

## State directory compatibility

Schema version **1** is the supported baseline for automatic startup migrations.
Pending migrations run in a transaction before torrents are loaded; failure
rolls back the changes and stops startup. Catalogs newer than the binary supports
and nonempty unversioned catalogs are rejected. See
[Upgrading](install.md#upgrading).

Keep the state directory: it contains the authority to reopen partial downloads.
If you choose a fresh `state_dir`, re-add torrents. Complete payload can be
adopted from the original download directory; partial files are not adopted into
a fresh catalog. The HTTP/control JSON changes are described in
[Command line](cli.md).

## Daemon keys

The table shows Linux path defaults. On macOS, `state_dir` defaults to
`~/Library/Application Support/spill` and `control_socket` to
`~/Library/Application Support/spill/spill.sock` (ignoring `XDG_RUNTIME_DIR`).
Downloads default to `~/Downloads` on both platforms. If you override
`state_dir`, set `control_socket` too; its default is independent of that override.

| Key | Default | Meaning |
|---|---|---|
| `state_dir` | `~/.local/state/spill` | Catalog (`catalog.sqlite`), lock file and debug log (`debug.log`). Created with mode `0700`. Two daemons cannot share one state directory. |
| `download_dir` | `~/Downloads` | Where new torrents are saved unless `add` gives an output directory. |
| `control_socket` | `$XDG_RUNTIME_DIR/spill.sock`, else `~/.local/state/spill/spill.sock` | Unix socket for `spill`. Created with mode `0600`. |
| `listen` | `"0.0.0.0:6881"` | Address and port for TCP peers and for uTP peers, the DHT and UDP trackers (UDP, same port). Both must be free. `"[::]:6881"` listens on IPv6 and IPv4 (dual-stack). |
| `web_listen` | unset (off) | Address for the web interface. Must be a loopback address, for example `"127.0.0.1:8080"`. |
| `allow_local_network` | `false` | Allow outbound connections to private, loopback and link-local addresses (LAN peers, a tracker on your network), and enable BEP 14 local service discovery for torrents with `dht = true`. LSD uses multicast UDP port 6771; peer destinations still cannot be multicast, unspecified, broadcast or port 0. Inbound connections are not filtered. |
| `utp` | `true` | Accept uTP peers and try uTP before TCP when dialing. Set `false` for TCP-only peers; UDP trackers and the DHT remain available. |
| `peer_encryption` | `"preferred"` | MSE/PE for TCP and uTP: `"allowed"` tries plaintext first, `"preferred"` tries MSE first, `"required"` permits only RC4 application streams. The first two accept plaintext and allow one alternate-mode retry after a negotiation close/reset. See [Peer encryption](networking.md#peer-encryption). |
| `port_mapping` | `true` | Ask the IPv4 gateway to forward the listen port for TCP and UDP, using NAT-PMP then UPnP IGD. Failures never stop torrents. The system-service example disables it. |
| `port_mapping_gateway` | unset (auto-discover) | An IPv4 address (port 5351) or `"ip:port"` overrides NAT-PMP discovery. An `"http://host:port/rootDesc.xml"` UPnP description URL skips NAT-PMP and SSDP and uses that IGD directly. Hosts must be unicast, ports nonzero; URL overrides accept HTTP only, without credentials or fragments. |
| `peers` | `50` | Maximum connections per torrent, `1`–`4096`. |
| `cache_bytes` | `16777216` (16 MiB) | Upload read cache per torrent, 16 KiB–1 GiB. |
| `write_queue_bytes` | `16777216` (16 MiB) | Received data waiting to be written, per torrent, 16 KiB–1 GiB. |
| `active_downloads` | `4` | How many torrents may download at once. |
| `active_seeds` | `16` | How many complete torrents may seed at once. |
| `dht_bootstrap` | four public routers | `host:port` or `[IPv6]:port` routers used to join the DHT, at most 16. Names resolve separately for each enabled address family. `[]` disables bootstrapping from routers (saved DHT nodes are still used); it does not disable the DHT. |

`active_downloads` and `active_seeds` are separate slot pools. Each is at most
64, their sum must be between 1 and 64. Torrents beyond the limits wait as
`queued`. See [Torrents](torrents.md#queueing).

The default DHT routers are `dht.libtorrent.org:25401`,
`dht.transmissionbt.com:6881`, `router.bittorrent.com:6881` and
`router.utorrent.com:6881`.

### Memory

Each running torrent can use up to `cache_bytes + write_queue_bytes` for
payload buffers (32 MiB by default) plus its peer connections. The shared UDP
socket requests a 2 MiB kernel receive buffer for uTP bursts; the OS may cap or
adjust that size. Each uTP peer has at most 256 KiB of queued/unacknowledged
send data and 256 KiB of receive/reassembly data. Of the receive budget,
192 KiB is advertised and 64 KiB is reserved to fill reassembly gaps.

Encrypted peer writes stage at most 64 KiB of ciphertext per connection.
Transport buffers fill on demand and are additional to the torrent cache/writer
reservations. At the default aggregate limit of 1,000 peers, uTP buffer allowances
alone can reach about 500 MiB, before peer queues, bookkeeping and kernel buffers.
The roughly 35 MiB measurement for 33 torrents in [Limits](limits.md) is a
web-seed workload, not a uTP worst-case estimate. Size any systemd `MemoryMax`
for the admitted torrent and peer budgets, not that measured baseline.

## Logging: `[logging]`

The daemon writes operational events to stderr and a more detailed history to
`<state_dir>/debug.log`. These outputs have independent filters; lowering
console verbosity does not discard debug-file events. Changes require a restart.
Client commands keep their usual stdout/JSON and stderr behavior. The embeddable
engine never installs a subscriber; its application owns tracing configuration.

| Key | Default | Meaning |
|---|---|---|
| `console_filter` | `"info"` | Tracing filter for stderr: lifecycle, torrent failures/recovery and subsystem warnings by default. |
| `file_filter` | `"warn,spill=debug"` | Tracing filter for the debug file: spill debug events and dependency warnings. |
| `file_enabled` | `true` | Set `false` to create no debug-file output or writer thread. Existing logs are not deleted. |
| `peer_endpoints` | `false` | Include peer IP addresses and ports in dial, admission and holepunch trace events in either output. Requires a matching `trace` filter; does not increase verbosity itself. |

Filters accept levels (`off`, `error`, `warn`, `info`, `debug`, `trace`) and
module directives, for example `warn,spill::engine::storage=trace`. Invalid
effective filters stop startup; the file filter is validated even if file output
is disabled. `RUST_LOG`, when set, overrides only `console_filter`; it never
changes the file filter. Startup failures before logging configuration is installed always go to stderr.

`debug.log` is opened in append mode with permissions `0600`, including across
restarts. spill does not rotate, truncate or limit the file; retention is the
system administrator's responsibility. If an external tool renames the file,
restart spill to reopen it at the original path.

A background thread writes files without blocking the torrent runtime. Its
queue holds at most 1024 events, each capped at 16 KiB; saturation drops new file events rather than
blocking transfers. Dropped-event counts are reported to stderr at most once a
minute and on clean shutdown. Shutdown drains queued events; logs are not
fsynced and a crash can lose recent events. An unusable debug-log destination
stops startup. Later write failures are reported once to stderr per
failure episode, retried on subsequent events, and do not stop torrents;
recovery is also reported. Writer-health messages bypass console filtering.
`health --json` also exposes cumulative dropped events and the current file-write
failure flag under `logging`; successful writes clear the flag, not the count.

Verbosity is event-driven, not a progress feed:

- `info`: daemon/torrent lifecycle, recovery and changed port mappings. Successful
  lease renewals do not repeat the mapping announcement.
- `debug`: storage recovery/failure operations, resume rejection, corrupt-piece
  retries, stalls, metadata-peer rejections, protocol/request-timeout peer closes,
  changed tracker failures/recovery and web-seed failures. No periodic progress,
  successful checkpoints or peer dumps.
- `trace`: peer/dial/handshake churn, inbound rejection reasons, transport fallback,
  successful piece/block activity, checkpoint saves and DHT maintenance. Enable
  only the module you are investigating.

Continuous listener, UDP, entropy, reconciliation and port-mapping failures
report the first failure rather than every retry; recovery is logged. Identical
persisted torrent errors are not repeated until cleared or changed. Retry
schedules and current errors remain available through the status API.
Use `spill --json get ID` and `spill --json health` for current progress and
network diagnostics rather than a logging heartbeat.

Torrent events carry a torrent ID. Peer endpoints remain off by default; enable
`peer_endpoints` to correlate connections with remote addresses during debugging.
This associates addresses with torrent IDs in retained logs. It does not change
peer addresses in live JSON diagnostics. Inbound rejections before torrent
routing omit endpoints even when this setting is enabled; their reasons are
logged under `spill::engine::session` at `trace`. Payloads and tracker URLs/credentials
are not intentionally logged. Logs can still contain local paths and error text:
review them before sharing.
See [Troubleshooting](troubleshooting.md#where-are-the-logs) for systemd and macOS.

## Torrent defaults: `[defaults]`

These are the per-torrent settings. They are copied into each new torrent and
can be changed per torrent later with `spill set ID key=value` or the web
interface.

| Key | Default | Meaning |
|---|---|---|
| `download_limit` | `0` | Download rate limit for this torrent in payload bytes per second. `0` is unlimited. |
| `upload_limit` | `0` | Upload rate limit, same units. |
| `seed_ratio` | `0.0` | Pause after uploading this multiple of the torrent's size. `0` disables the rule. Must be finite and not negative. |
| `seed_time` | `0` | Pause after this many seconds of active seeding. Paused time and downtime do not count. `0` disables the rule. |
| `file_priorities` | `[]` | `[]` downloads every file, except that magnet `so` supplies the initial selection when the list is empty. Otherwise one of `"skip"`, `"normal"` or `"high"` per file, in the order `spill files` shows. Only meaningful per torrent. |
| `trackers` | `true` | Announce to the torrent's trackers. |
| `dht` | `true` | Use the DHT over the address families supported by `listen`, and local service discovery when `allow_local_network = true`. Always off for private torrents. |
| `pex` | `true` | Exchange peers with connected peers. Always off for private torrents. |
| `web_seeds` | `true` | Download from the torrent's HTTP web seeds. |

When either seed goal is reached, the torrent is paused. Resume it to keep
seeding; the goal is checked again and will pause it again unless you change it.

## Examples

A desktop user with the web interface:

```toml
download_dir = "/home/me/Downloads"
web_listen = "127.0.0.1:8080"
active_downloads = 3
active_seeds = 10

[defaults]
upload_limit = 2097152   # 2 MiB/s
seed_ratio = 2.0
```

A home server on a LAN with IPv6:

```toml
state_dir = "/var/lib/spill"
download_dir = "/srv/torrents"
control_socket = "/run/spill/control.sock"
listen = "[::]:6881"
port_mapping = false    # automatic mapping requires an IPv4 listener
allow_local_network = true
peers = 100

[defaults]
dht = true               # separate IPv4 and IPv6 routing tables
```

The annotated files `deploy/spill.toml` and `deploy/spill-user.toml` are
complete starting points for Linux; `deploy/spill-macos.toml` uses the macOS
defaults.
