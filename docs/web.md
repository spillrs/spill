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
  lifetime downloaded/uploaded totals, rates, peers, ETA and discovery policy.
  Hover over status for the latest error or tracker message.
- Adding a `.torrent` file or a magnet link, with an optional output directory.
- Pause, resume, recheck, reannounce and remove (optionally deleting payload).
- A resizable bottom inspector with Overview, Files, Peers and Sources tabs.
  Overview groups lifetime accounting, selected progress, completion, UTC dates,
  settings and current-run diagnostics. Ratio uses uploaded bytes / full size.
- A dense peer table: one line per peer, no direction/connection IDs or prose
  annotations. Hover over peer state for additional diagnostic counters/flags.
- Source tables include tracker URLs, announce timing, peer counts and errors,
  plus available DHT and web-seed observations.
- Editable file priorities, rate limits and seeding goals. Values use monospace.

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

Both endpoints require the `X-Spill-Token` header, and use the same operations
and response format as the [control protocol](cli.md#control-protocol). The
page shows the current token; it changes whenever the daemon restarts.

### `POST /api`

A JSON body with an `op` field, limited to 64 KiB:

```sh
curl -s http://127.0.0.1:8080/api \
  -H "X-Spill-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"op":"set","id":"3f2a","values":{"upload_limit":1048576}}'
```

Operations: `add` (magnets), `get`, `set`, `list`, `status`, `files`, `pause`,
`resume`, `recheck`, `reannounce`, `remove`, `health`, `shutdown`.

The `get` response includes `transfer.peers[].transport`: `"tcp"`, `"utp"`
or `"http"` (web seed), just like the Unix control API. The same diagnostics
include per-peer and owner-total `utp_retransmits`, and `pending_contacts`;
see [Command line](cli.md#commands) for their meaning.

### `POST /api/upload`

Upload a `.torrent` file as the raw request body (at most 16 MiB). The output
directory is an optional, URL-encoded query parameter:

```sh
curl -s "http://127.0.0.1:8080/api/upload?output_dir=%2Fsrv%2Ftorrents" \
  -H "X-Spill-Token: $TOKEN" -H 'Content-Type: application/x-bittorrent' \
  --data-binary @file.torrent
```
