# Web interface and HTTP API

## Enabling it

The web interface is off by default. Set a loopback address in the config and
restart the daemon:

```toml
web_listen = "127.0.0.1:8080"
```

Only loopback addresses (`127.0.0.1`, `::1`) are accepted. To use it from
another machine, tunnel it, for example `ssh -N -L 127.0.0.1:8081:127.0.0.1:8080 server`,
then open <http://127.0.0.1:8081> locally. The local port need not match the remote port.

## The page

- **Torrent table:** Search across name, ID, SHA-1/SHA-256 hash, and output path;
  sort by any metric (including peers and ETA); multi-select with checkboxes,
  `Shift+Click`, or `Ctrl/Cmd+Click`; filter by All torrents, Downloading
  (including `starting` and `metadata`), Checking (`checking`/`recovering`),
  Seeding, Paused, Queued, and Needs attention (errors, retrying, tracker/metadata
  warnings). Press `/` to focus search and `Escape` to clear search or close details.
- **Adding torrents:** Paste a magnet link or HTTP(S) torrent URL, or choose /
  drag-and-drop a `.torrent` file (anywhere on the page or onto the upload box).
  Discovery toggles and “Start immediately” apply atomically before startup;
  private metainfo always disables DHT and PEX.
- **Actions:** Pause, resume, recheck (leaves torrent paused), reannounce, and
  remove. The remove confirmation defaults to keeping files; checking delete
  only removes files created by spill (adopted seeds are never deleted).
- **Inspector tabs:**
  - **Overview:** Stable ID, full hashes, and save path with copy buttons,
    metadata/layer state, lifetime accounting, active caps/seeding goals, and
    per-swarm completion delivery.
  - **Files:** Paginated file manifest with batch priority editing (`All normal`,
    `Skip all`, `Save changes`, `Discard`) so multiple edits restart the torrent
    once instead of per file. Virtual padding entries are marked and locked to `skip`.
  - **Peers:** Active peers with direction (`inbound`/`outbound`), transport
    (`tcp`/`utp`/`http`), encryption (`rc4`/`plaintext`), rates, totals, and
    hover diagnostics.
  - **Sources:** Editable discovery toggles (`trackers`, `dht`, `pex`, `web_seeds`)
    plus tracker, DHT, and web-seed tables.
- **Session limits & Torrent limits:** Edit daemon-wide caps (**Session limits**,
  reset on daemon restart) or per-torrent caps (**Torrent limits**, saved across
  restarts) in MiB/s without restarting transfers. **Seeding rules** edits upload
  ratio (against data size) and active seeding hours.
- **Daemon status & Logs:** Click the footer indicator for listeners, inbound
  public peer counts and recency, LSD, port mapping status/errors, session caps,
  and debug-log health. Click **Logs** for a 64 KiB tail of `debug.log` with
  follow (auto-pauses if you scroll up), refresh, and copy. If the daemon restarts
  and rotates its token, the UI pauses polling and prompts (or auto-reloads when
  no dialog is open).

Downloaded totals include corrupt payload and re-downloads, not adopted files.
Current-run counters reset when a torrent restarts; lifetime counters survive
restarts. Hover over a table total for its exact byte count. See
[Accounting in JSON](cli.md#accounting-in-json) for field definitions.

## Security model

- The server only accepts loopback addresses (`127.0.0.0/8`, `[::1]`) or
  `localhost` as the `Host` hostname, including when SSH forwards a different
  local port. If `Origin` is present it must match the browser-facing HTTP host
  and port. Disallowed `Host`/`Origin` requests receive HTTP `403`.
- Every API request must carry the `X-Spill-Token` header. The page embeds a
  random token generated when the daemon starts; missing or stale tokens receive
  HTTP `401` with a JSON error body.
- Cross-origin requests are rejected.

This protects against other websites in your browser. It is **not**
authentication against other users on the same machine: anyone who can reach
the loopback port can load the page and its token. Don't enable the web
interface on shared machines you don't trust.

## HTTP API

All `/api*` endpoints require `POST` (`GET` returns `405 Method Not Allowed`),
the `X-Spill-Token` header, and return `Cache-Control: no-store`. For local
scripts on the same machine, prefer `spill --json` or the
[Unix control socket](cli.md#control-protocol), which need no token extraction.

When calling the HTTP API from a script, read the current token from
`<meta name="spill-token" content="…">` on `GET /` (it rotates on every daemon
restart):

```sh
TOKEN="$(curl -s http://127.0.0.1:8080/ | sed -n 's/.*name="spill-token" content="\([^"]*\)".*/\1/p')"
```

### `POST /api`

A JSON body with an `op` field, limited to 2 MiB:

```sh
curl -s http://127.0.0.1:8080/api \
  -H "X-Spill-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"op":"set","id":"3f2a","values":{"upload_limit":1048576}}'
```

Operations: `add` (magnets or torrent URLs), `get`, `set`, `list`, `status`,
`files`, `pause`, `resume`, `recheck`, `reannounce`, `remove`, `health`,
`set_bandwidth`, `shutdown`.

For `set_bandwidth`, send at least one of `download_limit` or `upload_limit`,
for example `{"op":"set_bandwidth","upload_limit":1048576}`. Rates are
unsigned integers in payload bytes per second (`0` means unlimited). Omitted
caps stay unchanged. The response data contains both effective caps. The
change lasts until the daemon restarts and does not restart torrents. The same
operation is available on the Unix control socket.

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
text display. When file logging is disabled (or `debug.log` does not exist yet), `text` is
empty. Read failures return a nonzero code
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
