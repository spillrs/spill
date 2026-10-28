# Spill

a modern torrent client implemented in Rust.

Spill runs as a single-user BitTorrent v1 daemon on Linux and macOS.

Documentation lives in [`docs/`](docs/index.md): installation, configuration,
the command line, the web interface, networking, private trackers, limits and
architecture.

## Usage

```sh
spill serve [--config /absolute/path/spill.toml]
spill add example.torrent
spill add 'magnet:?xt=urn:btih:…'
spill list --watch
spill get ID
spill set ID upload_limit=1048576 seed_ratio=2
spill pause ID
spill resume ID
spill recheck ID
spill reannounce ID
spill remove ID [--delete-payload]
spill shutdown
```

See [`docs/cli.md`](docs/cli.md) and [`docs/configuration.md`](docs/configuration.md).
Example config files, systemd units and a launchd agent are in `deploy/`.

## Install and build

Install from crates.io on Linux or macOS:

```sh
cargo install spill --locked
```

Or build from source:

```sh
cargo build --release --locked   # target/release/spill
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
