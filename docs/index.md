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
- Discover peers through HTTP, HTTPS and UDP trackers, the DHT, peer exchange
  (PEX) and local service discovery (LSD); download from HTTP web seeds.
- Use private torrents (BEP 27), with DHT and PEX disabled.
- Select files and priorities, limit transfer rates, and configure daemon seeding
  goals.
- Recover verified progress after restarts using checkpoints and file fingerprints.
- Share one peer port over TCP and UDP, with optional NAT-PMP or UPnP mapping.

Read [Torrents](torrents.md), [Networking](networking.md), and
[Private trackers](private-trackers.md) for exact behavior.
[Limits and performance](limits.md) documents resource bounds and measured workloads.

## Scope

spill supports Linux and macOS. Both the daemon and [embeddable engine](library.md)
accept v1, v2-only, and hybrid metainfo, magnet links, and HTTP(S) web seeds, with
MSE/PE peer encryption ([Networking](networking.md#peer-encryption)) and optional
BEP 14 local service discovery ([Networking](networking.md#local-service-discovery)).

Interoperability checks cover controlled tests against Transmission 4.1+ and
libtorrent 2.1.2.0 (v1/v2/hybrid TCP transfers, magnets, trackers, DHT, hybrid PEX,
web seeds, and kill/restart recovery). Recorded baselines live in
[`tests/interop/baselines/`](https://github.com/spillrs/spill/tree/main/tests/interop/baselines)
and are not evidence of public-swarm capacity, v2 uTP interoperability, or
universal client compatibility; see [v2 and hybrid torrents](v2.md) and
[Limits and performance](limits.md) for exact coverage and bounds.

The daemon is single-user; its optional web interface listens only on loopback
and has no multi-user authentication. Remote access requires an SSH tunnel.

## Documentation versions

The website's `/docs/` guide follows the latest published release. Documentation
in a source checkout describes that checkout. Check `spill --version` and use
the corresponding version in the Rust API reference when integrating an older
release.
