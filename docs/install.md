# Install

## Install from crates.io

Spill supports Linux and macOS and needs a current stable Rust toolchain
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

Without `--config`, Spill uses built-in defaults:

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

Install Apple's command-line tools (`xcode-select --install`) and stable Rust,
then use either installation method above. Apple Silicon is supported. The
binary can run directly with `spill serve`; no systemd or extra libraries are needed.
State and the control socket live in `~/Library/Application Support/spill`,
and downloads in `~/Downloads`.

To run at login, install the launchd user agent. Stop any manually running
Spill first. From the repository root:

```sh
mkdir -p "$HOME/Library/Application Support/spill" "$HOME/Library/LaunchAgents"
cp deploy/spill-macos.toml "$HOME/Library/Application Support/spill/spill.toml"
agent="$HOME/Library/LaunchAgents/rs.spill.daemon.plist"
cp deploy/spill.plist "$agent"
plutil -replace ProgramArguments.3 -string "$HOME/Library/Application Support/spill/spill.toml" "$agent"
plutil -replace StandardOutPath -string "$HOME/Library/Application Support/spill/spill.log" "$agent"
plutil -replace StandardErrorPath -string "$HOME/Library/Application Support/spill/spill.log" "$agent"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/rs.spill.daemon.plist"
tail -f "$HOME/Library/Application Support/spill/spill.log"
```

The template assumes `/usr/local/bin/spill`; change `ProgramArguments[0]` if
installed elsewhere. launchd does not expand `~`, so the commands substitute
absolute paths. Edit the TOML before loading if needed; its example enables
the web interface on `127.0.0.1:8080`. The agent restarts after failure, not
clean shutdown. The log is not rotated automatically; truncate or rotate it
occasionally if it grows. To unload it:

```sh
launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/rs.spill.daemon.plist"
```

Allow incoming connections when the macOS application firewall prompts for
Spill. See [Troubleshooting](troubleshooting.md) for firewall and file-limit
settings. The Linux-only `tests/interop` harnesses are not supported on macOS.

## Run as a Linux user service (recommended)

Example units live in `deploy/` in the source repository.

```sh
mkdir -p ~/.config/spill ~/.config/systemd/user
cp deploy/spill-user.toml ~/.config/spill/spill.toml
cp deploy/spill-user.service ~/.config/systemd/user/spill.service
systemctl --user daemon-reload
systemctl --user enable --now spill
journalctl --user -u spill -f
```

Edit `~/.config/spill/spill.toml` first: paths in the file must be absolute.
To keep the daemon running after you log out, enable lingering with
`loginctl enable-linger $USER`.

## Run as a Linux system service

`spill.service` and `spill.toml` in the same directory run Spill as a dedicated
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

The unit uses systemd sandboxing (`ProtectSystem=strict`, `ProtectHome`), so
the download directory must be listed in `ReadWritePaths`.

## Open the port

Peers, the DHT and UDP trackers all use the listen port, default `6881`, over
both TCP and UDP. Outbound traffic needs no rules. For inbound peers, allow
that port through the host firewall. Spill tries automatic NAT-PMP and UPnP
router mapping by default; check `spill health`. If mapping is disabled
(as in the system-service example) or your router refuses, forward the listen
port for TCP and UDP yourself. See [Networking](networking.md#automatic-port-mapping).

## Upgrading

Before 1.0, Spill does not migrate old catalogs. If an upgrade reports
`unsupported catalog version`, start with a fresh `state_dir`. Keep your
download directory: re-adding a torrent whose complete files are already there
verifies and seeds them without downloading again.
