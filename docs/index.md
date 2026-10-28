# Spill

a modern torrent client implemented in Rust.

Spill runs in the background on Linux and macOS for a single user, downloads
and seeds torrents, and is controlled from the command line (`spill ctl`) or
an optional local web page.

Spill aims to be correct, interoperable with other clients, stable under long
running use and small. It is in beta.

## What it does

- Downloads and seeds BitTorrent v1 torrents from `.torrent` files and magnet links.
- Connects to peers over TCP and uTP (BEP 29).
- Finds peers through HTTP, HTTPS and UDP trackers, the DHT, peer exchange (PEX)
  and HTTP web seeds.
- Supports private torrents (BEP 27): DHT and PEX are switched off and tracker
  announces follow private-tracker expectations.
- Selects files and priorities per torrent, limits download and upload rates,
  and stops seeding at a ratio or time goal.
- Survives restarts and crashes: verified progress is checkpointed, and only
  files that changed on disk are rehashed.
- Keeps everything in one state directory with an SQLite catalog. One port
  (TCP and UDP, same number) is all that needs to be reachable. Automatically
  requests router mappings using NAT-PMP or UPnP IGD.

## What it does not do (yet)

- BitTorrent v2 or hybrid torrents.
- Protocol encryption (MSE/PE) or local peer discovery.
- Multiple users, remote web access or authentication. The web interface only
  listens on loopback.
- Platforms other than Linux and macOS.

## Where to go next

- [Install](install.md) Spill and run it as a service.
- Follow the [Quickstart](quickstart.md) to add your first torrent.
- Look up any setting in [Configuration](configuration.md) and any command in
  [Command line](cli.md).
- Read [Torrents](torrents.md) and [Networking](networking.md) to understand
  what Spill does with your files and your network.
