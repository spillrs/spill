# Private trackers

spill supports torrents with the `private` flag (BEP 27). Add them from a
`.torrent` file; magnets that resolve to private metadata are rejected.

## What changes for private torrents

- **DHT, PEX, local discovery and holepunch discovery are off.** spill doesn't
  advertise DHT or PEX in handshakes, doesn't send DHT port messages, and refuses
  `set dht=true` or `pex=true`. The
  daemon disables them when private metainfo is added, regardless of the web
  add dialog's DHT and PEX selections.
- **Outgoing peers come only from the active tracker** (and metainfo web seeds,
  if enabled). The library rejects caller-supplied direct peers for private torrents.
- **One tracker URL is active at a time.** Switching is allowed only after its
  failure. Before using another URL, spill disconnects existing peers, cancels
  pending dials and clears old contacts. A recovered preferred URL does not
  replace a working fallback. Different URLs are treated as different trackers,
  even if they share a hostname.
- **Private metadata is not served** through `ut_metadata`.
- Rate limits, file selection and seeding goals work the same as for public
  torrents.

## Announces

Private trackers track your ratio from announces, so spill follows these rules:

- `started` is sent when a torrent starts, except that partial seeds announce
  `paused` (BEP 21). This means selected files are complete, not that the torrent
  has stopped. `left` still counts missing bytes across the whole torrent.
- `stopped` is best-effort within five seconds when paused, removed, stopped by a
  seeding goal or shut down. Peer transfers stop before final counters are sampled.
- Resuming or switching private tracker URLs starts fresh tracker accounting:
  `uploaded`, `downloaded` and `corrupt` start at zero rather than carrying forward
  traffic from the previous session. Returning to an earlier URL also starts fresh.
  `left` still describes the whole torrent; local lifetime totals are unchanged.
- `uploaded` counts payload actually written to peer sockets. Tracker `downloaded`
  subtracts hash-failed bytes from received payload, floored at zero; HTTP reports
  failed bytes separately as `corrupt`. Redundant payload is not subtracted.
  Protocol overhead is not counted. Local lifetime download totals include corrupt
  traffic and can therefore differ from tracker counters.
- `completed` is sent once, when a download that had missing data finishes.
  Delivery is saved per swarm in the catalog (`completed_announces` in `get`:
  an absent record = not completed here, `delivered: false` = waiting,
  `delivered: true` = acknowledged), so it survives restarts and is not repeated. If a tracker accepts the event but
  its reply is lost, it may be sent again.
- Private hybrids announce only their v1 identity, avoiding duplicate credit for
  shared traffic. Their separate v2 tracker swarm is not used for discovery.
  Pure-v2 private torrents announce their v2 identity.
- Torrents whose files were already complete when added (cross-seeding, moved
  data) announce as seeds and never send `completed`.
- A `tracker id` from the tracker is sent back on later announces, and the
  tracker's `min interval` is respected, including for `reannounce`.
- UDP announces include the URL's path and query through BEP 41 URLData,
  including passkeys. Authentication requires tracker-side URLData support.
- Transport errors omit the request URL. Remote failure and warning text is
  displayed as supplied by the tracker and may itself contain sensitive data.
- Bencoded failure messages in bounded non-200 HTTP bodies are also displayed;
  other error bodies fall back to the HTTP status. HTTP 410 disables the URL.

## Tracker messages

- A **failure reason** ("Unregistered torrent", "Passkey not found", …) is
  shown as `tracker_error` while no selected tracker is succeeding for that swarm.
  It clears after recovery or successful fallback. Torrent failures remain in
  `last_error`; tracker rejection does not fail the torrent.
- A selected tracker's **warning message** is shown as `tracker_message`.

Tracker errors appear in `spill list`, `spill get`, and the web table and inspector.
Per-tracker errors remain available in `transfer.trackers[].error`, including
failed backups hidden by a successful selected tracker.

## Client whitelists

Most private trackers only accept whitelisted clients. spill identifies itself
with a version-derived `-sP…-` peer ID prefix and the wire identifier
`spill/VERSION`; it never impersonates another client. See
[Peer connections](networking.md#peer-connections) for the prefix format.
Until a tracker whitelists spill, its announces may be rejected. Check your
tracker's rules before using spill there.
