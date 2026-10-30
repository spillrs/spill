# AGENTS.md

Rules for any agent working in this repository. They override habits from
other codebases. When a rule and a request conflict, ask before breaking it.

## What Spill is

A single-user BitTorrent v1 daemon for Linux and macOS, written in Rust.
Correctness and interoperability with other clients come first, then stability
under real use, then performance. The code must stay minimal: every line has to earn its place.

## Layout

| Path | Responsibility | Must not |
|---|---|---|
| `src/protocol` | Pure wire codecs: bencode, metainfo, magnet, peer messages, trackers, KRPC, resume format | Do I/O, use async, spawn tasks |
| `src/engine/core` | Clock-driven torrent state machine: `Input` in, `Action` out (picker, choker, requests, extensions) | Do I/O, read the clock, use OS entropy |
| `src/engine/torrent` | Shell and runtime: perform core `Action`s, feed `Input`s, publish status | Make protocol or scheduling decisions |
| `src/engine/storage` | One writer task per torrent: writes, hashing, upload reads, resume checkpoints | Fsync outside the completion barrier |
| `src/engine/provisioning` | Safe on-disk paths, file creation, `.part` naming, payload removal | Know about peers or pieces in flight |
| `src/engine/session` | Shared listener, dial queue, DHT, tracker UDP, global budgets | Own per-torrent policy |
| `src/engine/{peer_io,discovery,dht,webseed}` | Transports and discovery sources feeding the shell | Write payload or make scheduling decisions |
| `src/daemon` | Config, SQLite catalog, supervisor, Unix control socket, CLI, web UI | Reach into engine internals; the engine API is the boundary |
| `deploy` | Example configs and service definitions | Depend on local machine paths |
| `tests/engine` | Public-library end-to-end scenarios with in-process peers, trackers or web seeds | Depend on the daemon binary or test-only engine APIs |
| `tests/interop` | Python harnesses against real clients (Transmission) | Require changes in `src/` to work |
| `docs` | End-user documentation, rendered on the homepage (`SUMMARY.md` is the page order) | Describe internals that churn; link to code instead |

## Testing policy

Behaviour is tested end to end. Unit tests are not wanted.

- **No unit tests in `engine` or `daemon`.** Do not add `#[test]`,
  `#[cfg(test)]` modules, `tests.rs` files or test fixtures under their source directories.
- **Protocol is the only exception:** codec tests (golden vectors, round
  trips, malformed input) live in `tests/protocol/`.
- **No test-only code in production.** No `#[cfg(test)]` constructors,
  hooks, feature flags or public APIs that exist only for tests. If a test
  needs to observe something, it must be useful in the daemon's JSON output
  to real users too.
- **A bug fix comes with an end-to-end reproduction** in the matching module
  under `tests/daemon/` or `tests/engine/` when one can be built with in-process
  peers, trackers, web seeds or filesystem payloads. Otherwise, add or extend an
  interop scenario. Library-only behaviour belongs under `tests/engine/`.
- Keep `tests/daemon.rs` and `tests/engine.rs` as module lists; group scenarios
  by behaviour under their matching directories and put shared harness code in
  `tests/common/`. Engine scenarios must not import daemon-dependent helpers or
  require the daemon binary. Retain real-daemon assertions when moving library
  scenarios; do not duplicate existing protection.
- Do not add tests to raise coverage. A test must protect a user-visible
  behaviour that could plausibly regress.

### Test tiers

| Tier | What | Command | When |
|---|---|---|---|
| 0 | Protocol codecs | `cargo test --test protocol` | Every change |
| 1 | Daemon and public-library end to end: real `spill` binary where needed, in-process peers, trackers, web seeds, no public network | `cargo test --test daemon --test engine` | Every change |
| 2 | Interop with Transmission 4.1+: both directions, kill and restart, magnet, HTTP tracker | `python tests/interop/transmission_smoke.py --help` | Before merging engine or daemon changes; on a dev machine with a LAN address |
| 3 | Limits and soak scenarios (large and many torrents, swarms, long runs) | Scenarios in `tests/interop/` | Before releases and after architectural changes; dev machine only |

Tiers 2 and 3 never run in CI or in cloud agent sessions. Say which tiers you
ran and which you did not.

## Code rules

- **No defensive code.** Do not guard against states that cannot happen:
  no `checked_*` arithmetic on counters, sizes or `Instant` deadlines, no
  overflow flags, no re-validating values the caller just built.
- **Validate input once, at the boundary:** config loading, control requests
  and remote data (metainfo, peer messages, tracker and DHT replies).
  Internal code trusts its callers.
- **Broken internal invariants panic** with `expect`, `assert!` or
  `unreachable!`. Never `debug_assert!` followed by carrying on, and never
  swallow an error to keep going.
- **Remote misbehaviour is not an engine error.** A bad peer, tracker or web
  seed gets disconnected or ignored; it never fails the torrent.
- **Durability follows other clients.** Payload is synced only at the
  completion barrier; resume checkpoints rely on file fingerprints plus
  rechecking changed pieces at startup. Do not add fsyncs, directory syncs or
  per-I/O checks.
- **No new abstractions without two real users.** No traits, generics, builders
  or wrapper types with a single implementation. Prefer concrete types and
  plain functions.
- **No aliases or re-exports** that just rename things. Keep visibility at
  `pub(crate)` unless the daemon needs it.
- **Small error surface.** Use `io::Error` internally and plain `thiserror`
  enums at the daemon boundary. Do not build error taxonomies.
- **Comments state why, not what,** and stay short. No changelog or
  history comments, and no comments that restate the code.
- **Prefer deleting code.** When a change makes something unused, remove
  it in the same change.

## Before finishing a change

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets --locked -- -D warnings`
3. `cargo test --workspace --locked` (tiers 0 and 1)
4. If you changed user-visible behaviour, defaults, limits, config keys,
   control commands or the HTTP API, update the matching page in `docs/` in the
   same change. Check `docs/configuration.md` against `src/daemon/config.rs`
   and `docs/cli.md` against `src/daemon/cli.rs` and `control.rs`.
   `README.md` holds only usage, build, test and license.
5. If you changed engine networking, storage or scheduling, list the tier 2
   and 3 scenarios that need a run on the dev machine.

## Do not

- Add dependencies without saying why the standard library or an existing
  dependency is not enough.
- Support old catalog schemas or on-disk formats before 1.0; bump the
  version and document that a fresh state directory is required.
- Put model names, agent names or session links in code, comments or docs.