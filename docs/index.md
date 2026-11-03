# spill

A modern torrent client built in Rust.

Run spill as a single-user daemon on Linux and macOS, controlled through the
CLI or an optional local web interface. Or embed the same engine in your Rust
application without running a separate daemon.

## Run the client

[Install spill](install.md), then follow the [quickstart](quickstart.md) to
start the daemon and add your first torrent. Use the
[CLI and JSON output](cli.md) for interactive control and automation, or enable
the [local web interface and HTTP API](web.md).

## Embed the engine

The [Rust library guide](library.md) covers dependencies, your Tokio runtime,
one-shot downloads, reusable sessions, progress, recovery, and shutdown.
The [Rust API reference](https://docs.rs/spill) describes the public interfaces.
Applications and agents can use the library directly; no daemon or SQLite
catalog is required.

## Capabilities

- Download and seed v1, v2-only and hybrid `.torrent` files and magnet links.
  V2/hybrid metainfo TCP discovery and daemon lifecycles have been checked
  with libtorrent 2.1.2.0; see [Scope](#scope) for the validation limits.
- Connect to peers over TCP and uTP (BEP 29).
- Discover peers through HTTP, HTTPS and UDP trackers, the DHT, and peer exchange
  (PEX); download from HTTP web seeds.
- Use private torrents (BEP 27), with DHT and PEX disabled.
- Select files and priorities, limit transfer rates, and configure daemon seeding
  goals.
- Recover verified progress after restarts using checkpoints and file fingerprints.
- Share one peer port over TCP and UDP, with optional NAT-PMP or UPnP mapping.

Read [Torrents](torrents.md), [Networking](networking.md), and
[Private trackers](private-trackers.md) for exact behavior.
[Limits and performance](limits.md) documents resource bounds and measured workloads.

## Scope

spill supports Linux and macOS. The daemon and [embeddable engine](library.md)
accept v1, v2-only and hybrid metainfo and magnets. Controlled libtorrent 2.1.2.0
magnet checks pass in both directions for the reference v2/hybrid fixtures and
a generated 1025-piece layer, including singleton hash serving and source-free
restart. Pending-layer hybrid downloads from v1-only peers and rebuilt-layer
seeding still require controlled interop validation; the recorded magnet checks
cover peer-supplied layers.
Controlled plaintext TCP checks against libtorrent 2.1.2.0 cover direct peers,
local HTTP/UDP trackers, isolated DHT, hybrid PEX and the v2/hybrid daemon
lifecycle, including kill/restart and proof serving. Transmission 4.1.3 covers
the v1 magnet/HTTP-tracker regression. The
[recorded checks](https://github.com/spillrs/spill/blob/main/tests/interop/baselines/discovery-daemon.json)
are not evidence of public-swarm capacity, v2 uTP interoperability or universal
client compatibility. HTTP(S) web seeds support all three formats. Controlled
HTTP checks against libtorrent 2.1.2.0 cover v2/hybrid file URLs, short tails,
padding and combined peer/web-seed downloads; see the
[web-seed checks](https://github.com/spillrs/spill/blob/main/tests/interop/baselines/webseeds.json).
Protocol encryption (MSE/PE) and local peer discovery are not supported.
The daemon is single-user; its optional web interface listens only on loopback
and has no multi-user authentication. Remote access requires an SSH tunnel.

## Documentation versions

The website's `/docs/` guide follows the latest published release. Documentation
in a source checkout describes that checkout. Check `spill --version` and use
the corresponding version in the Rust API reference when integrating an older
release.
