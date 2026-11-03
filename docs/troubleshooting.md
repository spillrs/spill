# Troubleshooting

## Where are the logs?

`spill serve` writes timestamped operational events to stderr, with color only
on a terminal. Detailed logs append to `<state_dir>/debug.log`; spill does not
rotate or limit this file. Retention is managed externally. Client command
stdout and JSON are not logs.

On Linux under systemd, stderr goes to the journal:

```sh
journalctl --user -u spill -f             # user service
sudo journalctl -u spill -f               # system service
sudo journalctl -u spill -p warning       # warnings and errors
sudo tail -f /var/lib/spill/debug.log     # system service debug history
```

spill supplies severity prefixes when stderr is connected to the journal, so
`journalctl -p` can filter by priority. The journal owns its retention. For a user
service, the default debug path is `~/.local/state/spill/debug.log`.

For temporary console debugging:

```sh
RUST_LOG=spill=debug spill serve --config /absolute/path/to/spill.toml
```

Under systemd, set `Environment=RUST_LOG=spill=debug` in a service override and
restart. Remove the override and restart when finished. This changes only the
console/journal output, not the independently configured debug file. Normal
debug logs omit peer churn, successful piece/block activity and checkpoints;
select a module at `trace` when those details are needed, for example
`RUST_LOG=warn,spill::engine::peer_io=trace`. There are no periodic progress
logs; use `spill --json get ID` for current state. See
[Logging configuration](configuration.md#logging-logging) for filters.

Torrent failures are also kept in the catalog: `spill get ID` shows
`last_error` and, when retrying, `retry_at`. Tracker failures appear separately
in `transfer.trackers[].error`; tracker warnings appear in `tracker_message`
and `transfer.trackers[].warning`. A backup tracker's last error can remain
visible while another tracker succeeds. Torrent log events include their ID
so you can correlate interleaved transfers.

On macOS, the launchd agent captures stderr in
`~/Library/Application Support/spill/spill.log`; detailed logs are in
`~/Library/Application Support/spill/debug.log`. Neither is macOS unified
logging. Check the agent with
`launchctl print gui/$(id -u)/rs.spill.daemon`. The separate launchd stderr file
needs administrator-managed retention; see [Installation](install.md#macos).

`debug_log_write_failed` means the debug file could not be written.
Check free space and permissions; transfers continue, and `debug_log_recovered`
marks successful writing again. `debug_log_events_dropped` reports file events
lost to a full writer queue. Capture the journal/console too when reporting
these failures. Very verbose filters can generate substantial output.

## macOS file-descriptor limit

macOS shells often start with a soft limit of only 256 files. `spill serve`
raises its soft limit to the smaller of its hard limit and `kern.maxfilesperproc`, read
using `/usr/sbin/sysctl`; failure stops startup with an error rather than
silently keeping the low limit. Client commands do not query or change this limit.
Check the system cap with
`sysctl kern.maxfilesperproc`. The launchd example also requests 10240 files.
For unusually large workloads, increase the system cap and the agent's hard
limit through your system administration policy before restarting spill.

## `daemon unavailable` (exit code 4)

A client command can't reach the control socket.

- Is the daemon running? For the Linux user service, run
  `systemctl --user status spill`; for the macOS launchd agent, run
  `launchctl print gui/$(id -u)/rs.spill.daemon`.
- Is the client using the same socket as the daemon? Pass `--socket PATH` if the
  config sets `control_socket`.
- For the Linux system service, run client commands as the `spill` user (the socket is mode
  `0600`): `sudo -u spill spill --socket /run/spill/control.sock list`.

## The daemon won't start

| Error | Fix |
|---|---|
| `Address already in use` | Another program (often another client) uses the `listen` port for TCP or UDP. Pick another port or stop it. |
| `control socket already in use` | Another spill is running with the same socket. |
| `Resource temporarily unavailable` | Another spill holds the lock on the same `state_dir` (`daemon.lock`). |
| `unsupported catalog version` | Use a compatible spill binary: the catalog is newer than this binary supports or is a nonempty unversioned catalog. Do not reset its version manually. See [Upgrading](install.md#upgrading). |
| `absolute paths and a nonzero active cap are required` | A path in the config is relative, or uses `~`, or the active slots are invalid, or `web_listen` isn't loopback. |
| `unknown field …` | A typo or a removed key in the config (for example `allow_local_peers`, now `allow_local_network`). |

## Adding a torrent fails

- `file already exists; choose another output directory`: some of the
  torrent's files (or a directory with a file's name) already exist and spill
  won't overwrite them. If all files are present and complete, spill adopts
  them instead; otherwise move them away or choose another `--output`.
- `torrent already exists`: it's already in the catalog; `spill list`.
- `output_dir must be absolute`: give a full path to `--output`.
- `private torrents require a .torrent file`: the magnet points at a private
  torrent; download the `.torrent` from the tracker instead.

## A torrent doesn't download

- `queued`: all download slots are busy. Raise `active_downloads` or pause another.
- `metadata` for a long time: no peer with the metadata was found. Check the
  magnet has trackers or that the DHT is enabled. A v2-only magnet with
  `info_validated` but not `metadata_ready` still needs authenticated piece
  layers from peers. Hybrids can download with layers pending and rebuild them
  locally at full completion.
- `downloading` with no peers: check `last_error` and the trackers in `get`. If
  peers are on your LAN, set `allow_local_network = true`. Peers that require
  protocol encryption can't be reached (see [Limits](limits.md#known-gaps)).
  For uTP-only peers, check that `utp` is enabled (the default).
- `retrying`: read `last_error`. Disk full and permission errors are the usual
  causes. `pause` then `resume` retries immediately. Local filesystem failures
  retry with backoff; invalid authenticated metadata, private magnets and learned
  duplicate hashes pause the torrent instead.
- On a private tracker, `transfer.trackers[].error` shows the tracker's reason,
  for example an unregistered torrent or a client that isn't whitelisted.

## Torrents stay queued after an internal failure

If `spill health` shows `"admission_closed": true` while the daemon is running,
a torrent stopped without proving that its files were released. spill then
starts no further torrents, so it never opens the same files twice. The
torrent's `last_error` names the failure. Restart the daemon, and report the
error if it repeats.

## Nobody can connect to me

Allow the `listen` port for TCP and UDP in your host firewall. `spill health`
prints listener readiness, current inbound peers, successful inbound handshake
totals and the time since the last public TCP/uTP handshake. No observations
means only that none were seen since startup, not that the port is closed.
These are passive observations: no external service or probe is used. Local
peers do not count as evidence of public reachability. A failed automatic
mapping can coexist with successful inbound peers when forwarding is manual.

For scripts, use `spill health --json`. Its `network` fields are described in
[Health](cli.md#health). `port_mapping` shows
`enabled`, `protocol` (`nat-pmp` or `upnp`), `external_ip`,
`external_tcp_port`, `external_udp_port`, `last_error` and `next_renewal`
(a Unix timestamp in seconds, null when no renewal is scheduled). The top-level
`advertised_port` shows the port currently sent to peers and trackers.

If the router refuses or never answers, check that NAT-PMP or UPnP is enabled
on it. `port_mapping_gateway = "192.168.1.1"` can correct NAT-PMP gateway
discovery on multi-interface hosts. On networks that block multicast, use the
router's UPnP description URL, for example
`port_mapping_gateway = "http://192.168.1.1:5000/rootDesc.xml"`. This skips
NAT-PMP and SSDP discovery; use the actual description URL for your router,
not its web administration page. Only HTTP and unicast hosts are accepted.
Each mapping attempt has an eight-second deadline, including two seconds for
SSDP discovery and up to three seconds per description fetch. On busy networks,
a slow device can delay discovery of the router until a later backoff retry;
a direct description URL avoids that discovery budget.
UPnP error 718 means a conflicting mapping: remove the conflicting entry or
choose another listen port. Error 725 is handled by retrying with a permanent
lease automatically. Otherwise set `port_mapping = false` and forward the
listen port manually for TCP and UDP. Downloads and seeding over
outbound connections still work when mapping fails. Double NAT and carrier-grade
NAT may require changes upstream; a mapping at the local router alone cannot
make those reachable. Automatic mapping currently requires an IPv4 listener.

On macOS, allow spill's incoming-connections prompt. In System Settings →
Network → Firewall → Options, allow the installed `spill` binary. Replacing
an unsigned development binary may prompt again. Don't disable the firewall;
allow only spill. The firewall permission does not configure your router.

## The DHT doesn't work

- The DHT uses the families supported by `listen`. IPv6-only listeners need
  reachable IPv6 routers or saved IPv6 contacts. Dual-stack listeners can also
  learn IPv6 contacts from IPv4 nodes, then verify them over IPv6. `listen = "[::]:…"` enables both families.
- `dht_bootstrap = []` with no saved nodes leaves the DHT with nobody to talk to.

## Torrents recheck after an upgrade

An incompatible resume checkpoint causes the bound torrent's payload to be
fully rechecked when it next starts. Existing good data is not downloaded again. Keep the state
directory and payload in place; deleting the catalog also discards the recorded
authority to reopen partial files. Catalog migrations run automatically at
startup; unsupported catalog versions are a separate issue. See
[Upgrading](install.md#upgrading).

## Files look wrong after a crash or power loss

Run `spill recheck ID`, then `spill resume ID`. Recheck hashes every piece and
leaves the torrent paused. Resuming downloads any missing or corrupt pieces.
