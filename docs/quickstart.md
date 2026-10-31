# Quickstart

[Install spill](install.md), then start the daemon:

```sh
spill serve
```

Leave it running in the foreground and use a second terminal for the commands
below. Downloads default to `~/Downloads`. If using a config, start with
`spill serve --config /absolute/path/to/spill.toml`. Add `--socket PATH` to
client commands if you configured a different `control_socket`.
Automation should manage `serve` as a persistent process; check `spill health`
before starting another daemon.

## Add torrents

```sh
spill add ~/Downloads/debian.iso.torrent
spill add 'magnet:?xt=urn:btih:…&tr=udp://tracker.example:1337/announce'
spill add movie.torrent --output /srv/media/incoming
```

`add` prints the torrent's ID: its info hash in hex. Anywhere an ID is expected
you can use any unambiguous prefix of it.

## Watch progress

```sh
spill list            # one line per torrent
spill list --watch    # refresh every second; Ctrl-C to stop
spill get 3f2a        # everything about one torrent, including peers and trackers
spill --json get 3f2a # the same as JSON
```

A torrent moves through `starting`, `metadata` (magnets only), `recovering`,
`downloading` and `seeding`. See [Torrents](torrents.md) for every state.

## Control torrents

```sh
spill pause 3f2a
spill resume 3f2a
spill set 3f2a upload_limit=1048576 seed_ratio=2
spill files 3f2a
spill set 3f2a 'file_priorities=["high","skip","normal"]'
spill recheck 3f2a
spill remove 3f2a                    # keep downloaded files
spill remove 3f2a --delete-payload   # also delete files spill created
```

For automation, prefer `spill --json list` and `spill --json get ID` over
`list --watch`, which runs until interrupted. See [Command line](cli.md) for
exit codes and the control protocol. Stop the daemon with `spill shutdown`.

## Use the web interface

Create a TOML config containing:

```toml
web_listen = "127.0.0.1:8080"
```

Stop the daemon and restart with
`spill serve --config /absolute/path/to/spill.toml`, then open
<http://127.0.0.1:8080>. The page shows progress, rates, ratios and
errors, adds torrents and magnets, and edits settings and file priorities.
See [Web interface and HTTP API](web.md).
