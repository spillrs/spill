# spill

A modern torrent client built in Rust.
Run it as a single-user daemon on Linux and macOS, or embed the engine in your
application.

The [user guide](https://spill.rs/docs/) covers the latest published release.
Documentation for this checkout lives in [`docs/`](docs/index.md).

## Install and run

Install a prebuilt binary with the [installation script](https://spill.rs/install.sh):

```sh
curl -fsSL https://spill.rs/install.sh | sh
```

Or install with Cargo:

```sh
cargo install spill --locked
```

Prebuilt binaries support Linux x86_64/ARM64 with glibc 2.39+ and Apple Silicon
macOS 11+. See the [installation guide](https://spill.rs/docs/install/) for
compatibility, manual downloads, upgrades, and service setup. spill is not yet
available through OS package managers.

Start the daemon and leave it running:

```sh
spill serve
```

In another terminal:

```sh
spill add /path/to/file.torrent
spill add 'magnet:?xt=urn:btih:…'
spill list --watch
```

Downloads default to `~/Downloads`. The optional local web interface is off by
default. See the [quickstart](https://spill.rs/docs/quickstart/).

For automation, use JSON output rather than an interactive watch:

```sh
spill --json list
spill --json get ID
spill set ID upload_limit=1048576 seed_ratio=2
spill pause ID
spill resume ID
spill remove ID                 # keep downloaded files
spill shutdown
```

Replace `ID` with the ID printed by `add`, or an unambiguous prefix.
See the [CLI reference](https://spill.rs/docs/cli/) and
[configuration reference](https://spill.rs/docs/configuration/).

## Embed in a Rust application

Use the engine without daemon dependencies:

```sh
cargo add spill --no-default-features
```

The engine runs in your application's Tokio runtime; no daemon process or
catalog is required. Start with the [library guide](docs/library.md),
[Rust API reference](https://docs.rs/spill), and runnable
[`download`](examples/download.rs) and [`progress`](examples/progress.rs)
examples. The latter demonstrates concurrent, resumable downloads.

## Build

```sh
cargo build --release --locked   # target/release/spill
```

Example configs, systemd units, and a launchd agent are in [`deploy/`](deploy).

## Test

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

`cargo test` runs protocol codec tests, daemon end-to-end tests, public-engine
end-to-end tests, and Rust documentation examples without public networking.
Interoperability tests against Transmission and scale scenarios are opt-in;
see [`tests/interop/README.md`](tests/interop/README.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE).
