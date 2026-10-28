# Interoperability smoke tests

Requires Python 3.11+, Transmission **4.1+** (`transmission-daemon` on PATH),
`findmnt`, and a built Spill daemon. The harness uses `--preferred-transports`,
which is not supported by Transmission 4.0. `--transport tcp` is the default;
`--transport utp` enables and prefers uTP and requires observing uTP peers in
Spill's diagnostics in both directions (TCP fallback alone cannot pass).
Use a sufficiently large fixture to span diagnostic sampling, and the machine's
actual LAN address (not the literal `LAN_IP`). `--rate-limit 1MiB` applies a
Spill upload/download payload cap for small fixtures that would otherwise finish
before diagnostic sampling. It defaults to unlimited; leave it unset for throughput
comparisons. Network access is explicitly opt-in.

```sh
# Transmission -> Spill, then Spill -> Transmission; verifies SHA-256 both ways.
python tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release

# uTP in both directions; also combine with --kill-at 0.5 or --magnet.
python tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --transport utp --size 1GiB --piece-length 1MiB

# Private swarm in both directions, including completed-payload recovery.
python tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --private --http-tracker --restart-check

# Kill Spill partway through the download, restart, and verify recovery.
# Use a larger fixture so the kill threshold can be sampled before completion.
python tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --size 1GiB --piece-length 1MiB --kill-at 0.5

# Optional timing against Transmission -> Transmission with the same source/fixture/disk.
python tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --size 1GiB --piece-length 1MiB --download-only --reference

python tests/interop/transmission_smoke.py --help
```

`--download-only` skips the reverse transfer. `--magnet` and `--http-tracker`
exercise the same transfers using tracker-only metadata discovery or a local
HTTP tracker. `--private --http-tracker` sets the info dictionary's private flag;
combine it with `--kill-at` or `--restart-check`, but not `--magnet`.
No public tracker, DHT, PEX, LPD, NAT mapping or encryption is
used by the Transmission harness. TCP-only runs disable uTP in both clients.

`transport-samples.jsonl` records each direction's elapsed time, observed transports,
payload progress and Spill's outgoing uTP retransmit count since the first sample
of that direction, about once per second. The latest samples are also retained in `result.json` under
`directions`, including on timeout. Counters survive owner restarts within the
harness's sampled totals. Transmission does not expose its uTP retransmit count
through RPC, so `spill_utp_retransmits` is explicitly Spill's count, not the
remote sender's. These are sampled diagnostics, not complete packet traces.

`--reference` retains elapsed time and sampled payload rates in `result.json`
for future benchmark automation; it does not enforce performance thresholds.
Use release runs on disk for comparisons. `--restart-check` checks
completed-payload restart and mtime-triggered revalidation with source discovery
disabled, verifying the payload checksum after each restart.

Logs/results are retained under `target/interop/`; generated payloads are removed
after successful runs unless `--keep` is supplied. Tmpfs requires explicit
`--allow-tmpfs` and is not suitable for disk-performance measurements.

## Tier 3: named disk baselines

`scale.py` requires Python 3.11+, `findmnt`, and Spill, but not Transmission.
It uses only local HTTP range sources; it measures storage, scheduling, catalog,
and control behaviour, not peer-wire throughput or choking. Runs are opt-in and
must stay off CI. No Python packages or JavaScript build step are needed.

```sh
# 33 torrents: one 256 MiB payload plus 32 × 16 MiB, four download slots,
# 4 MiB/s per torrent, then a 60-second completed soak and seeds-only restart.
python tests/interop/scale.py multi-torrent --allow-network --release

# Large-piece hashing while all torrents have a silent UDP tracker.
python tests/interop/scale.py multi-torrent --allow-network --release \
  --piece-length 16777216 --dead-udp-tracker

# Cross the old 60,000-file checkpoint boundary (default: 60,001 files).
python tests/interop/scale.py many-files --allow-network --release

# Exercise the full admitted file count.
python tests/interop/scale.py many-files --allow-network --release --files 100000
```

`many-files` accepts `--files 1..100000` (default 60001) and creates empty files
and one 256 KiB payload. This isolates inode,
descriptor, provisioning, and checkpoint costs rather than issuing one HTTP
request per tiny file. It verifies a `40 × file_count` fingerprint, a persisted
complete checkpoint, and recovery with web seeds disabled and zero download slots.
Both scenarios verify SHA-256, joined shutdown, catalog removal, and payload
cleanup; `--keep` skips removal for inspection. Each needs a fresh `--artifacts`
directory, or creates one under `target/interop/` automatically. Failures retain
payloads for inspection. Use `--timeout` for slower disks and `--soak-seconds` to
extend the multi-torrent soak. `--piece-length` changes the multi-torrent piece
size (256 KiB by default; multiples of 256 KiB dividing 16 MiB).
`--dead-udp-tracker` enables tracker discovery with a local UDP sink that never
replies, and verifies independent transaction IDs on the shared session socket while downloads,
control requests, restart and cleanup continue. No public tracker is used.
The default run is an initial baseline, not a
long-duration soak or a public-swarm stress test.

`result.json` records the scenario version, revision, dirty-worktree flag, runner
and binary hashes, workload, filesystem, timings, HTTP request count, sampled RSS,
file descriptors, control latency, and per-process peak RSS. Samples are retained
for comparisons. Run scenarios separately on an otherwise idle machine for
performance comparisons; passing has no speed or RSS threshold.

`baselines/` retains compact summaries of measured runs, including the original
multi-torrent setup. Compare matching workload/scenario versions and note the
measurement method and machine. Full logs, catalogs, fixtures, and sample series
remain in the run's artifact directory. The dedicated local web-seed smoke
harness has been removed; ordinary web-seed behaviour is covered by daemon
end-to-end tests.

## Tier 3: peer-wire swarm

`swarm.py` uses one Transmission 4.1+ seed and N independently networked Spill
leechers. A local HTTP tracker returns all participants, permitting leecher-to-
leecher transfers as well as seed uploads. Web seeds, DHT, PEX, LPD, NAT mapping,
and encryption are disabled. HTTP carries only tracker messages; payload uses
BitTorrent TCP by default. Pass `--transport utp` to prefer uTP in all clients;
Spill runs then require an observed uTP peer for every leecher. The default workload is eight leechers, each downloading a
256 MiB fixture with 1 MiB pieces and no bandwidth caps.

```sh
python tests/interop/swarm.py --allow-network --host LAN_IP --release
python tests/interop/swarm.py --allow-network --host LAN_IP --release --transport utp

# Same seed, fixture shape, disk and leecher count, using Transmission leechers.
# Run separately, not concurrently with the Spill workload.
python tests/interop/swarm.py --allow-network --host LAN_IP --leecher transmission

# Isolated one-to-one 1 GiB comparison, including sampled payload-only rates.
python tests/interop/transmission_smoke.py --allow-network --host LAN_IP --release \
  --size 1GiB --piece-length 1MiB --download-only --reference
```

`--leechers` accepts 1..32; `--size`, `--piece-length`, `--timeout`, `--artifacts`,
`--keep` and `--allow-tmpfs` control the run. Results include per-leecher completion
and first-payload samples, leecher upload totals, peak peer connections, seed
choking/upload counts, aggregate throughput, process RSS, and every payload's
SHA-256 check. Upload byte figures are the latest live counter samples and can
lag final socket writes. More than one leecher requires observed multi-peer
connections. `peer-samples.jsonl` retains each leecher's live peer diagnostics:
Spill snapshots include Fast negotiation, choke flags, assigned and authorized
requests, upload queue lengths, grants, output backpressure, writer free slots,
and pending/finished upload reads. Spill refreshes
these snapshots about every two seconds, so repeated polling can return the same
observation. The trace helps investigate stalls without relying on tracing logs;
it does not record disconnect reasons or individual wire requests.
These measurements exercise peer-wire traffic but do not instrument request
pipeline depth; they are not evidence of a particular scheduling algorithm.
Spill completion includes its durability barrier; Transmission completion uses
RPC `percentDone`, so the completion guarantees are not identical. Startup,
connection delays, seed choking and filesystem cache state affect elapsed rates.
Neither runner drops filesystem caches. Keep full timings and sampled payload
rates distinct, and run comparisons sequentially on an otherwise idle machine.
