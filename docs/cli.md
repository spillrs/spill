# Command line

```text
spill serve [--config PATH]
spill [--socket PATH] [--json] COMMAND
spill --version
```

`spill serve` runs the daemon in the foreground. All other commands talk to a
running daemon over its Unix socket. Without `--socket`, client commands use:

- Linux: `$XDG_RUNTIME_DIR/spill.sock`, falling back to
  `~/.local/state/spill/spill.sock`.
- macOS: `~/Library/Application Support/spill/spill.sock`.

Pass `--socket` if your config sets a different `control_socket`.

`--json` prints the daemon's raw JSON response data instead of text, one
document per line. Use it for scripts. `--socket` and `--json` can appear
before or after the client command.

## IDs

A torrent's ID is its info hash as 40 hex characters. Commands accept any
prefix that matches exactly one torrent; an ambiguous prefix is an input error.

## Commands

| Command | What it does |
|---|---|
| `add SOURCE [--output DIR]` | Add a local `.torrent` file, `magnet:` link, or HTTP(S) torrent URL. `--output` must be an absolute directory; it is created if missing. Defaults to `download_dir`. Prints the new ID. Adding a torrent that already exists is an error. |
| `list [--watch]` | All torrents: ID prefix, state, verified/total bytes, name. `--watch` refreshes every second until Ctrl-C. |
| `status` | Same data as `list`, plus daemon health. Most useful with `--json`. |
| `get ID` | Everything about one torrent: settings, state, progress, lifetime downloaded/uploaded bytes, timestamps, seeding time, errors, tracker messages, and live transfer details (rates, peers, trackers). |
| `set ID KEY=JSON ...` | Change per-torrent settings. Values are JSON: numbers, `true`/`false`, or arrays. See below. |
| `files ID` | Files in torrent order: index, length, priority, path. Requires metadata. |
| `pause ID` | Stop the torrent and keep it paused across restarts. |
| `resume ID` | Allow a paused torrent to run again (it may wait for a free slot). |
| `recheck ID` | Verify every piece against the data on disk, then leave the torrent paused. Use after moving or editing files, or after an unclean power loss. |
| `reannounce ID` | Announce to the tracker again as soon as the tracker's `min interval` allows. Fails if trackers are disabled for the torrent. |
| `remove ID [--delete-payload]` | Remove the torrent. Files are kept unless `--delete-payload` is given; see [Torrents](torrents.md#removing-torrents). |
| `health` | Daemon process ID, number of running torrents and listen address. |
| `shutdown` | Stop the daemon cleanly. |

In `get` JSON, `transfer.peers[].transport` is `"tcp"`, `"utp"` or `"http"`
(for web seeds). A uTP peer also has `utp_retransmits`, the number of resent
outgoing uTP datagrams. `transfer.utp_retransmits` totals these over the current
running owner, including peers that have closed; restarting the owner resets it.
Counts are sampled while peer I/O is attached; later FIN-linger retransmissions
are not included. `transfer.pending_contacts` counts queued and currently dialing
discovery contacts, not established peers. These diagnostics are not wire traces.

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
- `dht` and `pex` cannot be enabled on private torrents. `dht` cannot be enabled
  when the daemon listens on an IPv6 address.

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
`delete_payload`, `magnet`, `url`, `output_dir`). For `add`, `metainfo` is the
`.torrent` file as a standard base64 string (at most 16 MiB decoded); give
exactly one of `metainfo`, `magnet`, or `url`.
Responses are `{"code":0,"data":…,"error":null}`;
`code` uses the exit codes above. Requests are limited to about 22 MiB.
