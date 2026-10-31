# Install

Install spill with the hosted script, a manual release download, or Cargo.
spill is not yet distributed through OS package managers.

## Install with the script (recommended)

```sh
curl -fsSL https://spill.rs/install.sh | sh
spill --version
```

The script installs the latest published release to `~/.local/bin`, checks its
SHA-256 checksum, and replaces the binary atomically. It does not use sudo,
change shell profiles, or start a service. If `spill` is not found, follow the
printed PATH instructions or run `"$HOME/.local/bin/spill"` directly.

Requires `curl`, `tar`, and `sha256sum` or `shasum`. Supported platforms and
binary compatibility are listed below. Inspect or pin the script installation:

```sh
curl -fsSL https://spill.rs/install.sh -o install.sh
# Read install.sh before running it.
sh install.sh --version 0.6.0 --install-dir "$HOME/.local/bin"
```

Stop a running daemon before replacing its binary. Checksums detect corruption,
not a compromised release publisher. For upgrades, also read [Upgrading](#upgrading).

## Install a release binary

Download an archive and `SHA256SUMS` from the
[GitHub Releases](https://github.com/spillrs/spill/releases) page. Archives
contain only `spill`; no Rust toolchain is needed.

| Platform | Archive target | Compatibility |
|---|---|---|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | glibc 2.39 or newer (Ubuntu 24.04 baseline) |
| Linux ARM64 | `aarch64-unknown-linux-gnu` | glibc 2.39 or newer (Ubuntu 24.04 baseline) |
| macOS Apple Silicon | `aarch64-apple-darwin` | Built targeting macOS 11 or newer |

Linux binaries are dynamically linked against glibc, not musl; Alpine and
older distributions should build from source. No Intel macOS binary is
provided; use Cargo or build from source instead. SQLite and the web assets
are embedded in the binary.

Choose a released version and the target from the table:

```sh
version=VERSION # replace with a release version, without the v prefix
target=x86_64-unknown-linux-gnu
archive="spill-${version}-${target}.tar.gz"
base="https://github.com/spillrs/spill/releases/download/v${version}"
curl -fLO "$base/$archive"
curl -fLO "$base/SHA256SUMS"
grep -F "  $archive" SHA256SUMS | sha256sum -c -
tar -xzf "$archive"
sudo install -d /usr/local/bin
sudo install -m 0755 spill /usr/local/bin/spill
```

On macOS, use `shasum -a 256 -c -` instead of `sha256sum -c -`.
The macOS binary is initially unsigned and unnotarized. If Gatekeeper blocks
it, verify the checksum and provenance first, then remove its quarantine
attribute with `xattr -d com.apple.quarantine spill` before installing. Do not
disable Gatekeeper globally. Checksums detect corruption, not a compromised
release account.

Service/config examples are not in the binary archives; use `deploy/` from
the matching [source tag](https://github.com/spillrs/spill/tags) for the
service instructions below.

## Install from crates.io

spill supports Linux and macOS and needs a current stable Rust toolchain
(see `rust-toolchain.toml`). SQLite is bundled; no system libraries are
required beyond a C compiler.

```sh
cargo install spill --locked
```

This installs `spill` into Cargo's binary directory, usually `~/.cargo/bin`.
The service examples below expect `/usr/local/bin/spill`; either install the
binary there or change the executable path in the service definition.

## Build from source

```sh
git clone https://github.com/spillrs/spill
cd spill
cargo build --release --locked
sudo install -d /usr/local/bin
sudo install -m 0755 target/release/spill /usr/local/bin/spill
```

The single `spill` binary runs the daemon with `spill serve`; other commands,
such as `spill list`, control the running daemon.

## Run it once

```sh
spill serve
```

Without `--config`, spill uses built-in defaults:

| What | Default |
|---|---|
| State (catalog, lock) | Linux: `~/.local/state/spill`; macOS: `~/Library/Application Support/spill` |
| Downloads | `~/Downloads` |
| Control socket | Linux: `$XDG_RUNTIME_DIR/spill.sock`, otherwise `~/.local/state/spill/spill.sock`; macOS: `~/Library/Application Support/spill/spill.sock` |
| Peer port | `0.0.0.0:6881`, TCP and UDP |
| Web interface | off |

Stop it with Ctrl-C, `SIGTERM` or `spill shutdown`. Shutdown waits for
every torrent to stop cleanly and saves its progress.

To use a config file, pass an absolute path: `spill serve --config /path/to/spill.toml`.
See [Configuration](configuration.md) for every key.

## macOS

For Cargo/source installs, install Apple's command-line tools
(`xcode-select --install`) and stable Rust. Neither is needed for a release
binary. The binary can run directly with `spill serve`; no systemd or extra
libraries are needed.
State and the control socket live in `~/Library/Application Support/spill`,
and downloads in `~/Downloads`.

To run at login, install the launchd user agent. Stop any manually running
spill first. Obtain `deploy/` from the matching source tag, then run from that
source directory:

```sh
mkdir -p "$HOME/Library/Application Support/spill" "$HOME/Library/LaunchAgents"
cp deploy/spill-macos.toml "$HOME/Library/Application Support/spill/spill.toml"
agent="$HOME/Library/LaunchAgents/rs.spill.daemon.plist"
cp deploy/spill.plist "$agent"
plutil -replace ProgramArguments.0 -string "$(command -v spill)" "$agent"
plutil -replace ProgramArguments.3 -string "$HOME/Library/Application Support/spill/spill.toml" "$agent"
plutil -replace StandardOutPath -string "$HOME/Library/Application Support/spill/spill.log" "$agent"
plutil -replace StandardErrorPath -string "$HOME/Library/Application Support/spill/spill.log" "$agent"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/rs.spill.daemon.plist"
tail -f "$HOME/Library/Application Support/spill/spill.log"
```

The commands set the executable to your installed `spill`, including script
and Cargo installations. launchd does not expand `~`, so the commands substitute
absolute paths. Edit the TOML before loading if needed; its example enables
the web interface on `127.0.0.1:8080`. The agent restarts after failure, not
clean shutdown. The log is not rotated automatically; truncate or rotate it
occasionally if it grows. To unload it:

```sh
launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/rs.spill.daemon.plist"
```

Allow incoming connections when the macOS application firewall prompts for
spill. See [Troubleshooting](troubleshooting.md) for firewall and file-limit
settings. The Linux-only `tests/interop` harnesses are not supported on macOS.

## Run as a Linux user service (recommended)

Obtain `deploy/` from the matching source tag and run from that source directory.
The unit template uses `/usr/local/bin/spill`; set it to your installed path:

```sh
mkdir -p ~/.config/spill ~/.config/systemd/user
cp deploy/spill-user.toml ~/.config/spill/spill.toml
cp deploy/spill-user.service ~/.config/systemd/user/spill.service
systemctl --user edit --full spill.service
# Set ExecStart to the absolute path printed by: command -v spill
# Keep the serve --config arguments.
systemctl --user daemon-reload
systemctl --user enable --now spill
journalctl --user -u spill -f
```

Edit `~/.config/spill/spill.toml` first: paths in the file must be absolute.
To keep the daemon running after you log out, enable lingering with
`loginctl enable-linger $USER`.

## Run as a Linux system service

`spill.service` and `spill.toml` in the same directory run spill as a dedicated
`spill` account with state in `/var/lib/spill` and the control socket at
`/run/spill/control.sock`:

```sh
sudo useradd --system --home /var/lib/spill --shell /usr/sbin/nologin spill
sudo install -d -m 0755 /etc/spill
sudo install -m 0644 deploy/spill.toml /etc/spill/spill.toml
sudo install -m 0644 deploy/spill.service /etc/systemd/system/spill.service
sudo systemctl daemon-reload
sudo systemctl enable --now spill
sudo -u spill spill --socket /run/spill/control.sock list
```

The system unit expects `/usr/local/bin/spill`. Install the binary there first;
a user-local script or Cargo binary is not accessible under its `ProtectHome`
sandbox. The download directory must be listed in `ReadWritePaths`.

## Open the port

Peers, the DHT and UDP trackers all use the listen port, default `6881`, over
both TCP and UDP. Outbound traffic needs no rules. For inbound peers, allow
that port through the host firewall. spill tries automatic NAT-PMP and UPnP
router mapping by default; check `spill health`. If mapping is disabled
(as in the system-service example) or your router refuses, forward the listen
port for TCP and UDP yourself. See [Networking](networking.md#automatic-port-mapping).

## Upgrading

Before 1.0, spill does not migrate old catalogs. If an upgrade reports
`unsupported catalog version`, start with a fresh `state_dir`. Keep your
download directory: re-adding a torrent whose complete files are already there
verifies and seeds them without downloading again.

v0.6 requires catalog version 5; older catalogs require a
fresh state directory. Re-adding files does not recover their previous traffic
totals or timestamps.
