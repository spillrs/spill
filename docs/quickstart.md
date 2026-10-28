# Quickstart

This assumes `spill serve` is running with the default socket (see
[Install](install.md)). Add `--socket PATH` to every `spill ctl` command if you
configured a different `control_socket`.

## Add torrents

```sh
spill ctl add ~/Downloads/debian.iso.torrent
spill ctl add 'magnet:?xt=urn:btih:…&tr=udp://tracker.example:1337/announce'
spill ctl add movie.torrent --output /srv/media/incoming
```

`add` prints the torrent's ID: its info hash in hex. Anywhere an ID is expected
you can use any unambiguous prefix of it.

## Watch progress

```sh
spill ctl list            # one line per torrent
spill ctl list --watch    # refresh every second; Ctrl-C to stop
spill ctl get 3f2a        # everything about one torrent, including peers and trackers
spill ctl --json get 3f2a # the same as JSON
```

A torrent moves through `starting`, `metadata` (magnets only), `recovering`,
`downloading` and `seeding`. See [Torrents](torrents.md) for every state.

## Control torrents

```sh
spill ctl pause 3f2a
spill ctl resume 3f2a
spill ctl set 3f2a upload_limit=1048576 seed_ratio=2
spill ctl files 3f2a
spill ctl set 3f2a 'file_priorities=["high","skip","normal"]'
spill ctl recheck 3f2a
spill ctl remove 3f2a                    # keep downloaded files
spill ctl remove 3f2a --delete-payload   # also delete files Spill created
```

## Use the web interface

Add to your config and restart:

```toml
web_listen = "127.0.0.1:8080"
```

Then open <http://127.0.0.1:8080>. The page shows progress, rates, ratios and
errors, adds torrents and magnets, and edits settings and file priorities.
See [Web interface and HTTP API](web.md).
