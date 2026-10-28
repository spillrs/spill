# Command line

```text
spill serve [--config PATH]
spill ctl [--socket PATH] [--json] COMMAND
spill --version
```

`spill serve` runs the daemon in the foreground. `spill ctl` talks to a running
daemon over its Unix socket. Without `--socket`, `ctl` uses:

- Linux: `$XDG_RUNTIME_DIR/spill.sock`, falling back to
  `~/.local/state/spill/spill.sock`.
- macOS: `~/Library/Application Support/spill/spill.sock`.

Pass `--socket` if your config sets a different `control_socket`.

`--json` prints the daemon's raw JSON response data instead of text, one
document per line. Use it for scripts.

## IDs

A torrent's ID is its info hash as 40 hex characters. Commands accept any
prefix that matches exactly one torrent; an ambiguous prefix is an input error.

## Commands

| Command | What it does |
|---|---|
| `add SOURCE [--output DIR]` | Add a `.torrent` file (path) or a `magnet:` link. `--output` must be an absolute directory; it is created if missing. Defaults to `download_dir`. Prints the new ID. Adding a torrent that already exists is an error. |
| `list [--watch]` | All torrents: ID prefix, state, verified/total bytes, name. `--watch` refreshes every second until Ctrl-C. |
| `status` | Same data as `list`, plus daemon health. Most useful with `--json`. |
| `get ID` | Everything about one torrent: settings, state, progress, uploaded bytes, seeding time, errors, tracker messages, and live transfer details (rates, peers, trackers). |
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

## Changing settings with `set`

Keys are the [`[defaults]` keys](configuration.md#torrent-defaults-defaults):

```sh
spill ctl set ID download_limit=1048576 upload_limit=524288
spill ctl set ID seed_ratio=2 seed_time=86400
spill ctl set ID trackers=true dht=false pex=false web_seeds=true
spill ctl set ID 'file_priorities=["high","skip","normal"]'
spill ctl set ID 'file_priorities=[]'      # back to all files
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

`spill ctl` sends one JSON object per line to the socket and reads one JSON
response line. Other programs can do the same, for example with `socat`:

```sh
# Linux (when XDG_RUNTIME_DIR is set)
socket="$XDG_RUNTIME_DIR/spill.sock"
# macOS
# socket="$HOME/Library/Application Support/spill/spill.sock"
echo '{"op":"get","id":"3f2a"}' | socat - "UNIX-CONNECT:$socket"
```

Requests use `op` plus the same fields as the commands (`id`, `values`,
`delete_payload`, `magnet`, `output_dir`). For `add`, `metainfo` is the
`.torrent` file as a standard base64 string (at most 16 MiB decoded); give
either `metainfo` or `magnet`. Responses are `{"code":0,"data":…,"error":null}`;
`code` uses the exit codes above. Requests are limited to about 22 MiB.
