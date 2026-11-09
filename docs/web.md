# Web interface and HTTP API

## Enabling it

The web interface is off by default. Set a loopback address in the config and
restart the daemon:

```toml
web_listen = "127.0.0.1:8080"
```

Only loopback addresses (`127.0.0.1`, `::1`) are accepted. To use it from
another machine, tunnel it, for example `ssh -L 8080:127.0.0.1:8080 server`,
then open <http://127.0.0.1:8080> locally.

## The page

- A compact transfer table with search, sorting, multi-selection, state filters,
  lifetime downloaded/uploaded totals, rates, peers, ETA and TRK/DHT/PEX/WEB
  discovery policy. Torrent errors appear in the status cell with an alert icon
  and danger colour; hover over status for tracker warnings. Tracker failures
  also appear in the status cell, Overview and error filter until recovery or
  successful fallback, without changing torrent state. Sources retains individual
  tracker errors, including backups hidden by a working selected tracker.
  On narrow screens, scroll the table horizontally to see all columns; torrent
  names keep their space rather than collapsing.
- Adding a `.torrent` file, magnet link, or HTTP(S) torrent URL, with an optional
  output directory. Paste URLs into the same field as magnet links. The add
  dialog initializes Trackers, DHT, PEX and Web seeds from daemon defaults;
  these policies and “Start immediately”
  are applied in the add request, before any discovery or payload transfer.
  Private metainfo always disables DHT and PEX.
- Pause, resume, recheck, reannounce and remove (optionally deleting payload).
- A resizable bottom inspector with Overview, Files, Peers and Sources tabs.
  Overview shows the stable ID, format, full SHA-1/SHA-256 hashes, metadata
  readiness, per-swarm completion delivery, lifetime accounting, selected
  progress and UTC dates. Yes/no fields use labelled green checks and red
  crosses; unknown values stay explicit. Long hashes wrap without truncation. Ratio uses uploaded bytes / full size.
  Use Left/Right arrows or Home/End to switch tabs when a tab has keyboard
  focus. Press `/` outside a dialog or input to focus the transfer search.
- A dense peer table: one line per peer, no direction/connection IDs or prose
  annotations. Hover over peer state for additional diagnostic counters/flags.
- Source tables include tracker URLs, swarm keys, announce timing, peer counts
  and errors, plus per-swarm DHT and available web-seed observations.
- Editable file priorities, rate limits and seeding goals. Rate and seeding
  dialogs pair sliders with exact numeric inputs. Enter values beyond the
  slider's initial range to extend it; the sliders do not cap saved settings.
  Rate limits use
  KiB/s (1024 bytes/s); seeding time uses hours. Fractional values are rounded
  to whole bytes/s and seconds when saved; the API still uses bytes/s and
  seconds. 0 means unlimited or disables the seeding rule. Values use monospace.
  File-priority controls are disabled while disconnected or saving changes;
  unsaved selections revert to the last snapshot.
- A footer daemon indicator shows the version and a green connection dot.
  Click it for connection status, last update, PID, running torrents, admission
  state, TCP/UDP listeners, uTP/DHT settings, inbound peer counts and whether a
  port mapping is active. An active mapping also shows NAT-PMP or UPnP. Mapping
  warnings belong in Logs, not the status panel. Green means connected, not
  that every network service is healthy. On disconnection the dot turns red, an error is shown, and the status
  panel labels its data as the last snapshot.
- Choose **Logs** in the daemon-status dialog to view recent `debug.log` output.
  Follow refreshes every second and scrolls to the latest entry; uncheck it to
  pause. Refresh and Copy work on the displayed snapshot. Only the latest
  64 KiB are read, with the first cut line omitted. Closing the view stops log
  polling. Logs are displayed as plain text, not HTML. File logging must be
  enabled; journald and early startup messages are not included. Console and
  file filters differ; see [Logging](configuration.md#logging-logging).

Downloaded totals include corrupt payload and re-downloads, not adopted files.
Current-run counters reset when a torrent restarts; lifetime counters survive
restarts. Hover over a table total for its exact byte count. See
[Accounting in JSON](cli.md#accounting-in-json) for field definitions.

## Security model

- The server only answers requests whose `Host` is the loopback address and
  port it listens on, which blocks DNS-rebinding attacks.
- Every API request must carry the `X-Spill-Token` header. The page embeds a
  random token generated when the daemon starts.
- Cross-origin requests are rejected.

This protects against other websites in your browser. It is **not**
authentication against other users on the same machine: anyone who can reach
the loopback port can load the page and its token. Don't enable the web
interface on shared machines you don't trust.

## HTTP API

All endpoints require the `X-Spill-Token` header. `/api` and `/api/upload`
use the same operations and response format as the
[control protocol](cli.md#control-protocol). The
page source embeds the current token; it changes whenever the daemon restarts.

### `POST /api`

A JSON body with an `op` field, limited to 64 KiB:

```sh
curl -s http://127.0.0.1:8080/api \
  -H "X-Spill-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"op":"set","id":"3f2a","values":{"upload_limit":1048576}}'
```

Operations: `add` (magnets or torrent URLs), `get`, `set`, `list`, `status`,
`files`, `pause`, `resume`, `recheck`, `reannounce`, `remove`, `health`, `shutdown`.

For a URL add, send `{"op":"add","url":"https://example.org/file.torrent"}`
with optional `output_dir`, `paused`, and `settings` fields. For example,
`{"op":"add","url":"https://example.org/file.torrent","paused":true,"settings":{"web_seeds":false}}`
fetches metainfo but does not start discovery or payload transfer. Settings merge
with daemon defaults and are validated before saving. See the
[control protocol](cli.md#control-protocol).

The `get` response includes `transfer.peers[].transport`: `"tcp"`, `"utp"`
or `"http"` (web seed), just like the Unix control API. The same diagnostics
include per-peer and owner-total `utp_retransmits`, and `pending_contacts`;
see [Command line](cli.md#commands) for their meaning. The same stable-ID and
full-hash alias lookup applies over HTTP, including 64-digit v2 hashes. See
[Identity and discovery in JSON](cli.md#identity-and-discovery-in-json) for
format, metadata readiness, and per-swarm completion/tracker/DHT fields.

### `POST /api/logs`

Returns a bounded tail of the configured `<state_dir>/debug.log`. There are no
request fields; clients cannot choose a file path. Uses the control response envelope, with
`data` containing `enabled`, `text` and `truncated`:

```json
{"code":0,"data":{"enabled":true,"text":"…","truncated":false},"error":null}
```

At most 64 KiB are read per request. If older bytes are omitted, `truncated`
is true and the first cut line is discarded. Invalid UTF-8 is replaced for
text display. When file logging is disabled, `enabled` is false and `text`
is empty, even if an old log file exists. Read failures return a nonzero code
and an error message, not an empty success. Responses use `Cache-Control:
no-store`. Like other API access, this is not authentication against local
users; logs may contain sensitive operational details.

### `POST /api/upload`

Upload a `.torrent` file as the raw request body (at most 16 MiB). The output
directory is an optional, URL-encoded query parameter. `paused=true` and
`settings` (a URL-encoded JSON object of initial overrides) are also accepted:

```sh
curl -s "http://127.0.0.1:8080/api/upload?output_dir=%2Fsrv%2Ftorrents" \
  -H "X-Spill-Token: $TOKEN" -H 'Content-Type: application/x-bittorrent' \
  --data-binary @file.torrent
```
