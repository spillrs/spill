# Command line

```text
spill serve [--config PATH]
spill [--json] create [OPTIONS] TARGET
spill [--json] inspect SOURCE
spill [--socket PATH] [--json] COMMAND
spill --version
```

`spill serve` runs the daemon in the foreground, writing operational logs to
stderr and appending detailed logs to `<state_dir>/debug.log`. See
[Logging](configuration.md#logging-logging) for filters.
`spill create` creates torrent files locally and `spill inspect` reads metainfo or
magnet links without a daemon. Other commands talk to a running daemon over its
Unix socket. Without `--socket`, client commands use:

- Linux: `$XDG_RUNTIME_DIR/spill.sock`, falling back to
  `~/.local/state/spill/spill.sock`.
- macOS: `~/Library/Application Support/spill/spill.sock`.

Pass `--socket` if your config sets a different `control_socket`.

`--json` prints the daemon's raw JSON response data instead of text, one
document per line. Use it for scripts. `--socket` and `--json` can appear
before or after the client command. For `create`, `--json` prints the output path,
`info_hash` (stable ID), `format` (`v1`, `v2` or `hybrid`), `v1` and `v2`
(lowercase hex or null), `total_bytes`, and `piece_count`; progress stays on stderr.

## IDs

A metainfo torrent's stable ID is its 40-digit SHA-1 hash for v1 and hybrid
torrents, or its full 64-digit SHA-256 hash for v2-only torrents. A magnet keeps
its supplied primary ID after resolution. Commands accept the stable ID or any
known full-hash alias, case-insensitively, and any prefix matching exactly one
torrent. Exact matches take precedence; an ambiguous prefix is an input error.
Both hashes of a hybrid address the same torrent, destination and accounting.
Adding metainfo or a magnet with an already-known hash is a duplicate, even if
the other hash or destination differs. Exception: a magnet with BEP 53 `so`
enables its referenced skipped files in the existing torrent and returns that
ID, preserving other priorities, settings, destination and pause state. No-op
selection updates do not restart the torrent. All supplied hash topics must
already identify the same entry. `list`, `get` and `--json` output
preserve full IDs; use prefixes when entering commands.

## Commands

| Command | What it does |
|---|---|
| `inspect SOURCE` | Inspect a local `.torrent`, a quoted magnet link, or `-` for metainfo on stdin, without a daemon or network access. See below. |
| `create TARGET [OPTIONS]` | Create a v1, v2 or hybrid `.torrent` from a file or directory, without a daemon. See below. |
| `add SOURCE [--output DIR] [--paused] [--set KEY=JSON]...` | Add a local `.torrent` file, `magnet:` link, or HTTP(S) torrent URL. `--output` must be an absolute directory; it is created if missing. Defaults to `download_dir`. `--paused` prevents startup; repeated `--set` overrides daemon defaults before startup. Prints the torrent ID. Existing torrents are errors except for magnet `so` selection updates; see [File selection](torrents.md#file-selection). |
| `list [--watch]` | All torrents: full ID, state, verified/total bytes, name. `--watch` refreshes every second until Ctrl-C. |
| `status` | Same data as `list`, plus daemon health. Most useful with `--json`. |
| `get ID` | Everything about one torrent: settings, state, progress, lifetime downloaded/uploaded bytes, timestamps, seeding time, errors, tracker messages, and live transfer details (rates, peers, trackers). |
| `set ID KEY=JSON ...` | Change per-torrent settings. Values are JSON: numbers, `true`/`false`, or arrays. See below. |
| `files ID` | Files in torrent order: index, length, priority, path. Requires metadata. |
| `pause ID` | Stop the torrent and keep it paused across restarts. |
| `resume ID` | Allow a paused torrent to run again (it may wait for a free slot). |
| `recheck ID` | Verify every piece against the data on disk, then leave the torrent paused. Use after moving or editing files, or after an unclean power loss. |
| `reannounce ID` | Announce to the tracker again as soon as the tracker's `min interval` allows. Fails if trackers are disabled or the torrent is not running. A magnet still resolving metadata announces again once resolution finishes. |
| `remove ID [--delete-payload]` | Remove the torrent. Files are kept unless `--delete-payload` is given; see [Torrents](torrents.md#removing-torrents). |
| `health` | Plain-text daemon health, listener readiness, inbound peer observations, port mapping and admission state. Use `--json` for structured output. See [Health](#health). |
| `shutdown` | Stop the daemon cleanly. |

In `get` JSON, `transfer.peers[].transport` is `"tcp"`, `"utp"` or `"http"`
(for web seeds). `transfer.peers[].encryption` is `"rc4"` or `"plaintext"`
for peer streams, or `null` for web seeds and unavailable/unnegotiated observations;
it does not describe web-seed TLS. A uTP peer also has
`utp_retransmits`, the number of resent
outgoing uTP datagrams. `transfer.utp_retransmits` totals these over the current
running owner, including peers that have closed; restarting the owner resets it.
Counts are sampled while peer I/O is attached; later FIN-linger retransmissions
are not included. `transfer.pending_contacts` counts queued and currently dialing
or negotiating discovery contacts, not established peers. These diagnostics are not wire traces.

## Health

`spill health` prints readable fields, including the running daemon's version;
`spill health --json` returns the complete health object. The control/HTTP API
stays JSON. Existing fields include `version` (the running daemon's version),
`pid`, `running`, `listen`, `advertised_port`,
`port_mapping` and `admission_closed`.
The text output includes local discovery readiness and its last error. LSD
failures do not stop the daemon. Interfaces are checked every minute; sockets
are rebuilt only on changes or errors, including retrying unavailable interfaces
or port 6771. The `network` object adds:

| Field | Meaning |
|---|---|
| `tcp_listen`, `udp_listen` | Local address while the listener/receiver task is running; `null` offline or stopped. Not proof of external reachability. |
| `utp_enabled`, `dht_enabled` | Configured session permissions. DHT can be enabled without its lazy node having started. |
| `pending_dials`, `pending_dials_limit` | Used and maximum shared dial permits. Finished attempts retain permits until their owner consumes the result. |
| `queued_contacts` | Shared contact entries awaiting attempts, including delayed retries. Not a count of immediately eligible peers. |
| `pending_handshakes`, `pending_handshakes_limit`, `pending_handshakes_per_ip_limit` | Current shared inbound handshake usage, total cap and per-source-IP cap. |
| `utp_connections`, `utp_connections_limit` | Current uTP multiplexer entries and cap, including connecting and closing streams. Both zero offline. |
| `inbound_tcp`, `inbound_utp` | Separate passive observations for each peer transport, with the fields below. |
| `lsd` | Local discovery: `enabled` permission, `active` multicast socket readiness, `announces` batches sent on at least one interface, `received` valid announcements from other sessions, and `last_error` (or `null`). Counters reset on daemon restart. Any successful interface send clears `last_error`, even if another interface failed in the same batch; those partial failures remain in the logs. |

| Inbound field | Meaning |
|---|---|
| `active` | Successful inbound handshakes whose transports are still alive, including LAN peers. |
| `active_public` | Public-address subset of `active`. |
| `total` | Successful inbound handshakes since daemon startup, including LAN peers. |
| `public_total` | Public-address subset of `total`. |
| `last_public_ago_secs` | Seconds since the last successful public inbound handshake; `null` means none observed since startup. |

Counts exclude bare TCP accepts, incomplete/invalid handshakes, outbound peers
and web seeds. Totals survive disconnections and torrent removal, but reset with
the daemon. Public classification excludes loopback, private, link-local and
shared-address IPv4 ranges; mapped IPv4 addresses are classified as IPv4.
These observations do not contact any external service or send probes. A recent
public handshake is evidence of connectivity at that time; a quiet swarm is not
evidence of a closed port. See [Troubleshooting](troubleshooting.md#nobody-can-connect-to-me).

Health also includes `logging`: `file_enabled`, cumulative `events_dropped`
since daemon startup, and `write_failed` for the most recent debug-file write.
A successful write clears `write_failed`; it does not erase the drop count.
These observations do not make health independent of slow control or cleanup
work. Resource counts are sampled separately, not one atomic reservation view.

## Inspecting torrents and magnets

```sh
spill inspect download.torrent
spill inspect download.torrent --json
spill inspect 'magnet:?xt=urn:btih:0123456789012345678901234567890123456789'
spill inspect - < download.torrent
```

Output is formatted JSON by default; `--json` produces one compact JSON document
for scripts. Both include `kind`, `info_hash`, `format`, `v1`, `v2`, and `name`.
Metainfo inspection validates the content structure and reports the private flag,
piece length/count, file paths, lengths, offsets and padding flags, plus available
tracker tiers, web seeds, comment, creator, creation date and source. `total_bytes`
includes virtual padding; `data_bytes` excludes it. File paths follow the metainfo
layout (relative to the root for multi-file torrents), not sanitized disk paths.
`layers_pending` indicates missing v2 layers in a hybrid.

Magnet inspection reports decoded `parameters`, including discovery hints and
file selections; it does not resolve metadata. The existing magnet parser skips
invalid tracker hints. Inspection never contacts trackers or peers, reads payload,
adds a torrent, or connects to the daemon. Metainfo uses the protocol parser's
32 MiB input budget; successful inspection does not guarantee daemon admission
under the engine's stricter [limits](limits.md). Non-UTF-8 text is displayed with
replacement characters; hashes always use the original info bytes. Control
characters are JSON-escaped. Output can contain private tracker passkeys: redact
it before sharing.

## Creating torrents

```sh
spill create ./payload -a https://tracker.example/announce -o payload.torrent
spill create ./payload -d -l 20 -t 4 -p -s my-source
spill --json create ./payload -w https://seed.example/payload/
```

The options follow mktorrent's conventions:

| Option | Meaning |
|---|---|
| `-a`, `--announce URL[,URL]` | Tracker tier. Comma-separated URLs are alternatives; repeating the option adds backup tiers. HTTP(S) and UDP are supported. Optional for trackerless torrents. |
| `-c`, `--comment TEXT` | Comment outside the info hash. |
| `-d`, `--no-date` | Omit the creation date, making unchanged input and options reproducible across worker counts. |
| `-F`, `--format v1\|v2\|hybrid` | Torrent format; default v1. |
| `-l`, `--piece-length N` | Piece size is **2^N bytes**, not N bytes. Range 15–28 (32 KiB–256 MiB); default 18 (256 KiB). |
| `-n`, `--name NAME` | Torrent root name; defaults to the target basename. |
| `-o`, `--output PATH` | Output path; default `NAME.torrent` in the current directory. Existing files, directories and symlinks are never replaced. Parent directory must exist. |
| `-p`, `--private` | Set `info.private=1`. |
| `-s`, `--source TEXT` | Set `info.source`; affects the info hash, like the private flag. |
| `-t`, `--threads N` | Nonzero hashing worker count; default is the available CPU count. Each worker uses a 64 KiB read buffer. |
| `-v`, `--verbose` | Show piece progress even when stderr is not a terminal. Progress is otherwise automatic on terminals. |
| `-w`, `--web-seed URL[,URL]` | HTTP(S) web seeds. May be repeated. For directory torrents, use a directory URL ending in `/`. |

V1 sorts whole relative path bytes to match mktorrent. V2 and hybrid use BEP 52
file-tree order (byte-sorted path components) and piece-aligned files; hybrids
add BEP 47 padding entries. Hybrid creation refuses a source directory with a
top-level `.pad` entry. Hidden and zero-length files are
included; empty directories are omitted. Symlinks to files and directories are
followed, including links outside the target tree; ancestor cycles, dangling
links and special files are errors. Keep the source unchanged until creation
finishes. Creation checks for observed file changes but does not take a
filesystem snapshot.

Unlike mktorrent, spill rejects a directory containing no files, matching its
supported metainfo format. [Limits](limits.md) apply; if a payload needs too many
pieces, select a larger piece length. Paths are limited to 32 levels including
the torrent root.

Creation only reads the source; it does not add or start seeding the torrent.
Afterward, use `spill add FILE.torrent --output /absolute/parent` to verify and
seed files already at the expected destination. A directory torrent expects
`PARENT/NAME/`; a single-file torrent, including v2 and hybrid, expects
`PARENT/NAME`. A name override or
spill's [filesystem name sanitization](torrents.md#adding-torrents) can make those
paths differ from the original source paths. Source symlinks are followed during
creation, but are not adopted as payload paths when seeding.

Without `--json`, `get ID` prints the complete torrent details as formatted JSON;
`set ID ...` prints the resulting settings as `key=value` lines.

## Identity and discovery in JSON

`get`, `list` and `status` expose `id`, `format` (`"v1"`, `"v2"` or `"hybrid"`),
`v1` (40-digit SHA-1 or `null`), and `v2` (64-digit SHA-256 or `null`).
`metadata_ready` means validated metainfo sufficient for payload verification
is available; it is independent of payload recovery or completion.
`info_validated` means the info dictionary and all supplied magnet topics have
passed validation. `hash_bytes` and `hash_total_bytes` report authenticated v2
layer acquisition, excluding proof siblings and repeated roots, not payload
progress. These counters are zero when no acquisition is needed at startup.
V2-only magnets acquire layers before payload preparation; partial acquisition
is discarded on stop. `layers_pending` identifies hybrids using SHA-1 while v2
layers are absent. They rebuild layers from verified payload at full completion;
the next start uses both swarms for public torrents. Private hybrids announce
only v1. See [v2 and hybrid torrents](v2.md).

`completed_announces` is an array of `{ "swarm": "40 hex digits", "delivered":
true }` records. `false` means pending; an absent record means no completion
event was recorded for that swarm. Delivery is persisted independently for each
public hybrid swarm; private hybrids record only v1 delivery.

`last_error` reports torrent-level failures, not tracker failures. `tracker_error`
exposes a tracker failure while no selected tracker is succeeding for that swarm;
recovery or successful fallback clears it. Non-JSON `list` prints it beneath the
row, and the web UI includes it in the error filter without changing torrent state.
Per-tracker errors remain in `transfer.trackers[].error`, alongside state,
counters and reply age. `transfer.trackers[].active` identifies the selected
tracker for each swarm. A failed backup can retain its error while the selected
tracker succeeds; that does not make the torrent failed or set `tracker_error`.
`tracker_message` exposes a selected tracker's warning, also available in
`transfer.trackers[].warning`.

`transfer.trackers[]` and `transfer.dht[]` identify their swarm with the same
`swarm` key: SHA-1 for v1, the 20-byte SHA-256 prefix for v2. DHT observations
are an array, empty when disabled. Public hybrids with authenticated layers have
independent tracker/DHT state for both keys but shared payload progress and
budgets. Pending-layer hybrids use only the v1 swarm. Private hybrids announce
only v1, and private torrents disable DHT in every format. Each public hybrid
tracker swarm receives the same torrent-wide upload/download counters, not
per-swarm traffic; summing their accounting can double-count bytes.
DHT entries also report `node_ids` (hexadecimal) and `external_addresses`, each
an array in IPv4/IPv6 order. Disabled families have a `null` node ID; an address
is `null` until a public bind address or DHT address votes establish it. Before
that, the node ID is not yet address-bound. These identities are session-wide,
not different for each swarm. See [DHT security](networking.md#dht-security-and-public-addresses).

## Runtime diagnostics in JSON

`transfer` is also available during magnet acquisition and payload recovery.
During metadata acquisition it reports tracker snapshots, active peer count,
`pending_contacts`, and `metadata_error`: the most recent core metadata-peer
rejection or request timeout, retained through payload promotion. This is not a
torrent failure and is not cleared when another peer succeeds. Per-peer payload
records and DHT lookup snapshots are not published during metadata acquisition.
Selected-byte counts remain `null` until metadata is ready.

| Transfer field | Meaning |
|---|---|
| `diagnostics_age_ms` | Age of the last diagnostic sample, or `null` before the first sample. Samples normally refresh every two seconds; an old sample is not evidence of current activity. |
| `storage` | Latest storage-worker observation, or `null` before storage exists. Fields: `operation`, `operation_age_ms`, `hashes_in_flight`, `reads_in_flight`. |
| `holepunch` | Execution-local cumulative coordination counters described below. |

Storage `operation` identifies the worker's current activity or wait:
`recovery`, `recovery_wait` (shared recheck capacity), `idle`, `write`, `hash`,
`read`, `leaves`, `finish_file`, `sync` (completion barrier), `fingerprint`,
`checkpoint`, `metadata_checkpoint`, `layers`, `publish` (result delivery),
`drain`, `stopped` or `failed`. `operation_age_ms` is how long that label has
remained unchanged, not an individual disk-I/O latency. Hash/read counts include
submitted payload jobs waiting for blocking execution or collection; they
exclude recovery jobs. Jobs can overlap the named worker operation. Check
`diagnostics_age_ms` before interpreting storage activity: operation age keeps
increasing even if the observation is stale.

`transfer.peers[].detail.remote_upload_only` reports the peer's BEP 21 hint:
`true` means it does not intend to download more pieces. Missing or malformed
hints leave the previous value unchanged; the initial value is `false`. This
hint does not mean the peer has every piece or that it cannot upload to spill.

Holepunch counters reset for each execution:

- `rendezvous_started`: introducer requests queued by this torrent.
- `remote_errors`: BEP 55 error frames received while holepunching is enabled.
- `rendezvous_expired`: pending introducer requests that reached their deadline.
- `punches_queued`: punch leases created or ordinary queued contacts upgraded.
  Duplicates do not increase this count.
- `punches_expired`: leases that expired before dial admission, including
  upgraded contacts restored to their original schedule.

These counters are sampled diagnostics, not NAT reachability tests or transfer
success counts. All execution diagnostics reset when the torrent restarts.

## Accounting in JSON

`get`, `list` and `status` expose the same per-torrent accounting fields:

| Field | Meaning |
|---|---|
| `downloaded_bytes` | Lifetime payload received from peers and web seeds, including corrupt data, duplicates and re-downloads processed by the engine. Excludes existing files and protocol overhead. |
| `uploaded_bytes` | Lifetime payload written to peer transports, excluding protocol overhead. Does not prove the remote peer retained it. |
| `hash_failed_bytes`, `hash_failed_pieces` | Lifetime bytes and piece-verification attempts rejected after a downloaded piece fails its hash check. Counts the actual piece length, including a short final piece; repeated failures count again. Excludes startup/recheck corruption. |
| `verified_bytes` | Currently verified bytes across the full torrent, not cumulative downloads. |
| `wanted_bytes`, `wanted_verified_bytes` | Selected data bytes and their verified portion, excluding padding and skipped-file portions of boundary pieces. `null` before metadata is available. |
| `completed` | Saved or live completion of the selected target after the completion barrier. Independent of whether the torrent is running. Changing file selection clears it until the new target is checked by a run. |
| `seed_seconds` | Seconds running ready and complete, including idle seeding; excludes recovery, pauses and downtime. |
| `added_at` | When this catalog entry was created. |
| `completed_at` | Most recent recorded transition to complete, including adopted files. Retained if the torrent later becomes incomplete. |
| `last_download_at`, `last_upload_at` | Most recent observation of payload received/sent, or `null` if none. |

Timestamps are UTC Unix epoch seconds; optional timestamps are `null` until
known. Activity timestamps are sampled, not packet timestamps. Live rates update
about every two seconds; lifetime accounting is sampled about every second.
`transfer.downloaded_bytes` and `transfer.uploaded_bytes` count only the current
run and reset when it restarts. `transfer.hash_failed_bytes` and
`transfer.hash_failed_pieces` likewise count only the current run. Hash-failure
bytes are already included in downloaded traffic, not additional traffic.
`transfer` is `null` when no run is present.
Stopped progress comes from the saved resume record; file fingerprints are
validated when the torrent next runs.

Lifetime totals, timestamps and seeding time survive orderly restarts and
rechecks. Removing and re-adding a torrent starts its accounting afresh. See
[Torrents](torrents.md#accounting) for checkpoint durability.

## Changing settings with `set`

Keys are the [`[defaults]` keys](configuration.md#torrent-defaults-defaults):

```sh
spill set ID download_limit=1048576 upload_limit=524288
spill set ID seed_ratio=2 seed_time=86400
spill set ID trackers=true dht=false pex=false web_seeds=true
spill set ID 'file_priorities=["high","skip","normal"]'
spill set ID 'file_priorities=[]'      # back to all files
```

- Quote values that contain brackets or spaces so your shell passes them intact.
- A running torrent is stopped and restarted with the new settings. A paused
  torrent stays paused.
- `file_priorities` needs one entry per file in `files` order, including
  padding files, which must be `"skip"`. Explicit file priorities need metadata, so wait
  for a magnet to resolve first.
- `dht` and `pex` cannot be enabled on private torrents.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | The operation failed |
| 2 | Invalid input (bad arguments, values, ambiguous ID, invalid torrent) |
| 3 | No torrent matches the ID |
| 4 | Cannot reach the daemon (not running, wrong socket, permission denied) |
| 130 | `list --watch` interrupted with Ctrl-C |

## Control protocol

Client commands send one JSON object per line to the socket and read one JSON
response line. Other programs can do the same, for example with `socat`:

```sh
# Linux (when XDG_RUNTIME_DIR is set)
socket="$XDG_RUNTIME_DIR/spill.sock"
# macOS
# socket="$HOME/Library/Application Support/spill/spill.sock"
echo '{"op":"get","id":"3f2a"}' | socat - "UNIX-CONNECT:$socket"
```

Requests use `op` plus the same fields as the commands (`id`, `values`,
`delete_payload`, `magnet`, `url`, `output_dir`, `paused`, `settings`). For `add`, `metainfo` is the
`.torrent` file as a standard base64 string (at most 16 MiB decoded); give
exactly one of `metainfo`, `magnet`, or `url`. Optional `settings` is an object
of per-torrent overrides merged with daemon defaults; `paused` defaults to false.
Settings are validated before the torrent is saved or started. Private metainfo
always disables DHT and PEX, including initial overrides. An HTTP(S) torrent URL
is still fetched when adding paused; payload transfer and discovery wait for
`resume`.
Responses are `{"code":0,"data":…,"error":null}`;
`code` uses the exit codes above. Requests are limited to about 22 MiB.
