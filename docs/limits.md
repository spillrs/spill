# Limits and performance

## Hard limits

Torrents beyond these limits are refused when added.

| Limit | Value |
|---|---|
| `.torrent` / metadata size | 16 MiB |
| Pieces per torrent | 1,048,576 |
| Piece size | 512 MiB |
| Data files per torrent (excluding padding) | 100,000 |

V2 expanded piece commitments, cached proof-tree nodes above the piece layer,
and expanded paths share a separate 16 MiB admission budget. Proof trees are
cached per file root on first use. For one large file this allows approximately
262,000 pieces (256K), fewer when expanded paths consume part of the budget.
Magnet size admission includes its trackers and required piece layers and runs
before layer acquisition.

Only the first 32 web seeds of a torrent are used.

Offline [torrent creation](cli.md#creating-torrents) uses the same file, piece
count, and metainfo-size ceilings. It accepts power-of-two piece sizes from
32 KiB to 256 MiB (default 256 KiB), follows symlinks, and limits paths to
32 levels including the torrent root. It rejects entirely empty directories;
zero-length files are supported. A fixed piece size that would exceed the
piece count or metainfo-size limit must be increased explicitly.

Daemon-wide limits:

| Limit | Value |
|---|---|
| Running torrents (`active_downloads` + `active_seeds`) | 64 |
| Connections per torrent (`peers`) | 4096 (default 50) |
| Peers uploaded to at once, per torrent | 8 |
| Config file size | 64 KiB |

There is no limit on how many torrents the catalog holds; torrents beyond the
active slots wait as `queued`.

Library callers have the same per-torrent resource bounds: `Profile.peers` must
be 1–4096, and `cache_bytes` and `write_queue_bytes` must each be 16 KiB–1 GiB.
Admission and magnet metadata resolution reject invalid profiles with
`InvalidInput` before starting work. `Profile::validate()` checks these bounds
without starting an execution.

Canonical peer replacement (BEP 40) may retain one waiting socket per torrent
while the evicted peer task joins. The waiting socket starts no peer task until
capacity is released; `peers` and session peer reservations remain unchanged.

## Holepunch coordination

BEP 55 uses the ordinary session dial-token, pending-dial, peer and uTP limits;
it has no reserved dial capacity or guaranteed admission latency.

| Bound | Value |
|---|---|
| Pending rendezvous per torrent | 8, at most one per target and introducer |
| Rendezvous lifetime, including output queueing | 10 seconds |
| Queued holepunch messages per connection | 8 |
| Queued punch dial lifetime | 3 seconds |
| Incoming rendezvous processed per connection | 4 per 10-second window |
| Incoming connect messages processed per connection | 4 per 10-second window |
| Retained PEX introductions per connection | 200 |

Each incoming-message window starts on its first message. Rejected and
duplicate requests consume the allowance too; excess requests are ignored
without a reply. The next message after the window expires starts a new one.
Relay pairs are queued only if both connections' output queues have room.
Expired punch-only dials are dropped. Upgraded ordinary contacts return to
their original source and schedule. Duplicate introductions do not extend
a queued punch's lifetime.
PEX introductions follow the existing one-minute receive gate and disappear
when the connection closes or disables PEX.

## Resource use

- **Memory**: each running torrent may buffer up to `cache_bytes` +
  `write_queue_bytes` (32 MiB by default) at peak. Typical use is far lower.
- **Open files**: spill keeps every file of a running torrent open while it
  runs. A torrent with 100,000 files uses about 100,000 file descriptors. The
  daemon raises its soft descriptor limit to the hard limit at startup on
  Linux. On macOS it uses the smaller of the hard limit and
  `kern.maxfilesperproc`; see [Troubleshooting](troubleshooting.md#macos-file-descriptor-limit).
- **Threads**: one async thread runs the daemon; disk I/O and hashing run on a
  blocking thread pool.

## Measured baselines

Measured on a Linux desktop (Intel Core Ultra 7 265, ext4) on a local network,
against Transmission 4.1. These are historical workload measurements, not
rankings or guarantees for the current release. Raw summaries, source revisions,
and workload details are in
[the baseline records](https://github.com/spillrs/spill/tree/main/tests/interop/baselines).

| Scenario | Result |
|---|---|
| 1 GiB from one Transmission seed | spill 11.2 s, Transmission 19.5 s |
| 8 leechers × 256 MiB from one Transmission seed | spill ~10.6 s (p50 of 10 runs), Transmission 13.7 s |
| 33 torrents, 768 MiB, 4 download slots | 35 MiB peak memory, 118 open files, 1.5 ms p95 control latency |
| Restart with 33 complete torrents | 0.1 s, nothing downloaded again |
| One torrent with 100,000 files | 184 MiB peak memory, 100,018 open files; restart 9.7 s |

The peer-wire comparisons include startup and choking delays. Filesystem caches
were not dropped, some recorded source trees were dirty, and completion is not
defined identically: spill includes its durability barrier; Transmission is
observed through RPC progress. The 33-torrent workload uses local web seeds,
not a peer-wire swarm. See the
[measurement methodology](https://github.com/spillrs/spill/blob/main/tests/interop/README.md)
and per-run notes before drawing comparisons.

## Known gaps

- Automatic mapping supports NAT-PMP and UPnP IGD, not PCP or IPv6 pinholes.
- V2 and hybrid interoperability has been checked only against libtorrent
  2.1.2.0 on a controlled network; see [Scope](index.md#scope) and
  [v2 and hybrid torrents](v2.md#compatibility). Web-seed checks do not cover
  public-swarm capacity or HTTPS certificate interoperability.
- Files of a running torrent stay open; torrents with very many files are
  costly (see above).
