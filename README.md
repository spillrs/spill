# Spill

a modern torrent client implemented in Rust.

Spill runs as a single-user BitTorrent v1 daemon on Linux and macOS.

Documentation lives in [`docs/`](docs/index.md): installation, configuration,
the command line, the web interface, networking, private trackers, limits and
architecture.

## Usage

```sh
spill serve [--config /absolute/path/spill.toml]
spill ctl add example.torrent
spill ctl add 'magnet:?xt=urn:btih:…'
spill ctl list --watch
spill ctl get ID
spill ctl set ID upload_limit=1048576 seed_ratio=2
spill ctl pause ID | resume ID | recheck ID | reannounce ID
spill ctl remove ID [--delete-payload]
spill ctl shutdown
```

See [`docs/cli.md`](docs/cli.md) and [`docs/configuration.md`](docs/configuration.md).
Example config files, systemd units and a launchd agent are in `crates/daemon/deploy/`.

## Build

```sh
cargo build --release --locked -p daemon   # target/release/spill
```

## Test

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

`cargo test` runs protocol codec tests and daemon end-to-end tests with
in-process peers, trackers and web seeds; it needs no network. Interoperability
runs against Transmission and scale baselines are opt-in; see
[`tests/interop/README.md`](tests/interop/README.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE).
