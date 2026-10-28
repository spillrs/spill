# Private trackers

Torrents with the `private` flag (BEP 27) are fully supported. Add them from a
`.torrent` file; magnets that resolve to private metadata are rejected.

## What changes for private torrents

- **DHT and PEX are always off.** Spill doesn't advertise them in handshakes,
  doesn't send DHT port messages, and refuses `set dht=true` or `pex=true`. The
  web interface disables those controls.
- **Peers come only from the torrent's trackers** (and its web seeds, if any).
- Everything else, including rate limits, file selection and seeding goals,
  works the same as for public torrents.

## Announces

Private trackers track your ratio from announces, so Spill is careful about them:

- `started` is sent when a torrent starts; `stopped` when it is paused, removed
  or the daemon shuts down. Resuming starts a fresh session with counters at zero.
- `uploaded` counts payload actually written to peer sockets; `downloaded`
  counts payload received, including data that later fails its hash check.
  Protocol overhead is not counted.
- `completed` is sent once, when a download that had missing data finishes.
  Delivery is saved in the catalog (`completed_announce` in `get`: `null` = not
  completed here, `false` = waiting to be delivered, `true` = acknowledged), so
  it survives restarts and is not repeated. If a tracker accepts the event but
  its reply is lost, it may be sent again.
- Torrents whose files were already complete when added (cross-seeding, moved
  data) announce as seeds and never send `completed`.
- A `tracker id` from the tracker is sent back on later announces, and the
  tracker's `min interval` is respected, including for `reannounce`.
- Tracker errors are reported without the tracker URL, so passkeys don't
  appear in error messages.

## Tracker messages

- A **failure reason** ("Unregistered torrent", "Passkey not found", …) is
  shown as the torrent's `last_error`.
- A **warning message** from a successful announce is shown as `tracker_message`.

Both appear in `spill get` and on the web page.

## Client whitelists

Most private trackers only accept whitelisted clients. Spill identifies itself
honestly, as peer ID prefix `-sP0500-` and `Spill/VERSION`, and never
impersonates another client. Until a tracker whitelists Spill, its announces may
be rejected with a failure reason saying so. Check your tracker's rules before
using Spill there.
