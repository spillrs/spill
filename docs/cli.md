# Command line

```text
spill serve [--config PATH]
spill [--json] create [OPTIONS] TARGET
spill [--socket PATH] [--json] COMMAND
spill --version
```

`spill serve` runs the daemon in the foreground, writing operational logs to
stderr and appending detailed logs to `<state_dir>/debug.log`. See
[Logging](configuration.md#logging-logging) for filters.
`spill create` creates torrent files locally without a daemon. Other commands talk to a running daemon over its
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
the other hash or destination differs. `list`, `get` and `--json` output
preserve full IDs; use prefixes when entering commands.

## Commands

| Command | What it does |
|---|---|
| `create TARGET [OPTIONS]` | Create a v1, v2 or hybrid `.torrent` from a file or directory, without a daemon. See below. |
| `add SOURCE [--output DIR] [--paused] [--set KEY=JSON]...` | Add a local `.torrent` file, `magnet:` link, or HTTP(S) torrent URL. `--output` must be an absolute directory; it is created if missing. Defaults to `download_dir`. `--paused` prevents startup; repeated `--set` overrides daemon defaults before startup. Prints the new ID. Adding a torrent that already exists is an error. |
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
| `health` | Daemon process ID, number of running torrents, listen address, port mapping and `admission_closed`. See [Troubleshooting](troubleshooting.md#torrents-stay-queued-after-an-internal-failure). |
| `shutdown` | Stop the daemon cleanly. |

In `get` JSON, `transfer.peers[].transport` is `"tcp"`, `"utp"` or `"http"`
(for web seeds). A uTP peer also has `utp_retransmits`, the number of resent
outgoing uTP datagrams. `transfer.utp_retransmits` totals these over the current
running owner, including peers that have closed; restarting the owner resets it.
Counts are sampled while peer I/O is attached; later FIN-linger retransmissions
are not included. `transfer.pending_contacts` counts queued and currently dialing
discovery contacts, not established peers. These diagnostics are not wire traces.

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
the next start uses both swarms. See [v2 and hybrid torrents](v2.md).

`completed_announces` is an array of `{ "swarm": "40 hex digits", "delivered":
true }` records. `false` means pending; an absent record means no completion
event was recorded for that swarm. Delivery is persisted independently for each
hybrid swarm.

`last_error` reports torrent-level failures, not tracker failures. Tracker errors
remain in `transfer.trackers[].error`, alongside each tracker's state, counters
and reply age. A failed tracker can retain its last error while another tracker
is succeeding; that does not make the torrent failed. `tracker_message` exposes
a tracker warning, also available in `transfer.trackers[].warning`.

`transfer.trackers[]` and `transfer.dht[]` identify their swarm with the same
`swarm` key: SHA-1 for v1, the 20-byte SHA-256 prefix for v2. DHT observations
are an array, empty when disabled. Hybrids with authenticated layers have
independent tracker/DHT state for both keys but shared payload progress and
budgets. Pending-layer hybrids use only the v1 swarm. Each tracker swarm
receives the same torrent-wide upload/download counters, not per-swarm traffic;
summing tracker accounting across both swarms can double-count bytes.

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
  padding files, which must be `"skip"`. File selection needs metadata, so wait
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
