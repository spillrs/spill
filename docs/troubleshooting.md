# Troubleshooting

## Where are the logs?

The daemon writes errors to standard error. On Linux under systemd that is
the journal:

```sh
journalctl --user -u spill -f     # user service
sudo journalctl -u spill -f       # system service
```

Per-torrent errors are also kept in the catalog: `spill get ID` shows
`last_error`, `tracker_message` and, when retrying, `retry_at`.

On macOS, the launchd agent writes to
`~/Library/Application Support/spill/spill.log`. Check the agent with
`launchctl print gui/$(id -u)/rs.spill.daemon`.

## macOS file-descriptor limit

macOS shells often start with a soft limit of only 256 files. `spill serve`
raises its soft limit to the smaller of its hard limit and `kern.maxfilesperproc`, read
using `/usr/sbin/sysctl`; failure stops startup with an error rather than
silently keeping the low limit. Client commands do not query or change this limit.
Check the system cap with
`sysctl kern.maxfilesperproc`. The launchd example also requests 10240 files.
For unusually large workloads, increase the system cap and the agent's hard
limit through your system administration policy before restarting Spill.

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
| `control socket already in use` | Another Spill is running with the same socket. |
| `Resource temporarily unavailable` | Another Spill holds the lock on the same `state_dir` (`daemon.lock`). |
| `unsupported catalog version` | The catalog is from an older Spill. Use a fresh `state_dir`; re-add torrents, and complete files are adopted without downloading. |
| `absolute paths and a nonzero active cap are required` | A path in the config is relative, or uses `~`, or the active slots are invalid, or `web_listen` isn't loopback, or `dht = true` with an IPv6 `listen`. |
| `unknown field …` | A typo or a removed key in the config (for example `allow_local_peers`, now `allow_local_network`). |

## Adding a torrent fails

- `file already exists; choose another output directory`: some of the
  torrent's files (or a directory with a file's name) already exist and Spill
  won't overwrite them. If all files are present and complete, Spill adopts
  them instead; otherwise move them away or choose another `--output`.
- `torrent already exists`: it's already in the catalog; `spill list`.
- `output_dir must be absolute`: give a full path to `--output`.
- `private torrents require a .torrent file`: the magnet points at a private
  torrent; download the `.torrent` from the tracker instead.

## A torrent doesn't download

- `queued`: all download slots are busy. Raise `active_downloads` or pause another.
- `metadata` for a long time: no peer with the metadata was found. Check the
  magnet has trackers or that the DHT is enabled.
- `downloading` with no peers: check `last_error` and the trackers in `get`. If
  peers are on your LAN, set `allow_local_network = true`. Peers that require
  protocol encryption can't be reached (see [Limits](limits.md#known-gaps)).
  For uTP-only peers, check that `utp` is enabled (the default).
- `retrying`: read `last_error`. Disk full and permission errors are the usual
  causes. `pause` then `resume` retries immediately.
- On a private tracker, `last_error` shows the tracker's reason, for example an
  unregistered torrent or a client that isn't whitelisted.

## Torrents stay queued after an internal failure

If `spill health` shows `"admission_closed": true` while the daemon is running,
a torrent stopped without proving that its files were released. Spill then
starts no further torrents, so it never opens the same files twice. The
torrent's `last_error` names the failure. Restart the daemon, and report the
error if it repeats.

## Nobody can connect to me

Allow the `listen` port for TCP and UDP in your host firewall. Run
`spill health` (or expand **Health** in the web interface): `port_mapping`
shows `enabled`, `protocol` (`nat-pmp` or `upnp`), `external_ip`,
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

On macOS, allow Spill's incoming-connections prompt. In System Settings →
Network → Firewall → Options, allow the installed `spill` binary. Replacing
an unsigned development binary may prompt again. Don't disable the firewall;
allow only Spill. The firewall permission does not configure your router.

## The DHT doesn't work

- The DHT needs an IPv4 `listen` address. With `listen = "[::]:…"` the daemon
  logs `DHT disabled` and runs without it.
- `dht_bootstrap = []` with no saved nodes leaves the DHT with nobody to talk to.

## Files look wrong after a crash or power loss

Run `spill recheck ID`, then `resume`. Recheck hashes every piece and
downloads anything missing or corrupt.
