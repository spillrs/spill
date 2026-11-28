# Quickstart

[Install spill](install.md), then start the daemon:

```sh
spill serve
```

Leave it running in the foreground and use a second terminal for the commands
below. Downloads default to `~/Downloads`. If using a config, start with
`spill serve --config /path/to/spill.toml` (validate a config first with
`spill serve --config /path/to/spill.toml --check`). Set `SPILL_SOCKET` or pass
`--socket PATH` to client commands if you configured a different `control_socket`.
Automation should manage `serve` as a persistent process; check `spill health`
before starting another daemon.

## Add torrents

```sh
spill add ~/Downloads/debian.iso.torrent
spill add 'magnet:?xt=urn:btih:…&tr=udp://tracker.example:1337/announce'
spill add movie.torrent --output ./incoming
```

`add` prints the torrent's hex ID (one line, so `id=$(spill add file.torrent)`
works directly). Anywhere an ID is expected you can use any unambiguous prefix
(for example `3f2a` below).

## Watch progress

```sh
spill list            # compact table of torrents, progress, rates, and errors
spill list --watch    # refresh every second; Ctrl-C to stop
spill health          # daemon listeners, inbound peers, port mapping, and debug log
spill get 3f2a        # summary for one torrent
spill --json get 3f2a # full torrent JSON, including peers and trackers
```

A torrent moves through `starting`, `metadata` (magnets only), `recovering`,
`downloading` and `seeding`. See [Torrents](torrents.md) for every state.

## Control torrents

```sh
spill pause 3f2a
spill resume 3f2a
spill set 3f2a upload_limit=1048576 seed_ratio=2
spill files 3f2a
spill set 3f2a 'file_priorities={"1":"skip","0":"high"}'
spill recheck 3f2a
spill remove 3f2a                    # keep downloaded files
spill remove 3f2a --delete-payload   # also delete files spill created
```

Lifecycle commands (`pause`, `resume`, `recheck`, `reannounce`, `remove`) also
accept multiple IDs at once. For automation, prefer `spill --json list` and
`spill --json get ID` over `list --watch`, which runs until interrupted. See
[Command line](cli.md) for exit codes and the control protocol. Stop the daemon
with `spill shutdown`.

## Use the web interface

Create a minimal TOML config (omitted keys keep their defaults), for example:

```sh
mkdir -p ~/.config/spill
printf 'web_listen = "127.0.0.1:8080"\n' > ~/.config/spill/spill.toml
```

Stop the daemon and restart with
`spill serve --config ~/.config/spill/spill.toml`, then open
<http://127.0.0.1:8080>. The page shows progress, rates, ratios and
errors, adds torrents and magnets, and edits settings and file priorities.
See [Web interface and HTTP API](web.md).

## Next steps

- Run spill in the background with systemd or launchd: [Install](install.md#macos).
- Explore offline tools (`create`, `inspect`, `verify`) and JSON output: [Command line](cli.md).
