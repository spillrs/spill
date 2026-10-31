# Architecture

The daemon and embedded applications use the same engine. This page describes
stable boundaries; the [source](https://github.com/spillrs/spill/tree/main/src)
is the reference for implementation details. For integration, start with the
[Rust library guide](library.md) and [API reference](https://docs.rs/spill).

## One package, two uses

The `spill` Cargo package contains a library and the `spill` binary.

| Layer | Responsibility |
|---|---|
| `spill::protocol` | Pure wire codecs and validated BitTorrent v1 metainfo. No I/O. |
| `spill::engine` | Sessions, torrent lifecycles, peers, discovery, storage, and observations. |
| Daemon | CLI, configuration, SQLite catalog, supervisor, Unix control socket, and local web interface. |

The daemon uses the public engine API, just like an embedding application.
Disable default features to use the engine without daemon dependencies.

## Ownership and scheduling

A session owns shared networking and resource budgets. Each admitted torrent
owns its lifecycle and transfer state. Its deterministic core takes inputs and
returns actions; the runtime performs those actions and feeds results back.
Protocol and scheduling decisions stay in the core, not in transport or storage.

The daemon uses a single-threaded Tokio runtime. Embedded applications supply
their own current-thread or multithread runtime. Blocking filesystem operations
and hashing run on Tokio's blocking pool.

Bounded queues connect peer I/O, transfer state, and the per-torrent writer.
Backpressure prevents a slow disk or peer from creating an unbounded payload
queue. Admission reserves per-torrent budgets; these are not a total RSS limit.
See [Limits and performance](limits.md).

## Verification and durability

Only hash-verified pieces are uploaded. Pieces can be verified from buffered
data or read back from disk. Resume checkpoints pair verified progress with file
fingerprints; changed files are rechecked before transfer resumes.

Payload is synced at the completion barrier, not after each write.
[Restarts and durability](torrents.md#restarts-crashes-and-durability) describes
what checkpoints can and cannot guarantee after a crash or power loss.

Stopping joins owned peer, storage, discovery, and blocking work before releasing
the destination. Embedded callers explicitly await cleanup; see
[Stop and shutdown](library.md#stop-and-shutdown).

## Daemon persistence

The SQLite catalog stores torrent intent, settings, accounting, recovery data,
and errors. The supervisor reconciles that intent with engine executions and
available download/seed slots. CLI, Unix-socket, and HTTP requests use the same
control dispatcher.

## Validation

Protocol codec tests live in `tests/protocol/`; public-engine and daemon
end-to-end scenarios live in `tests/engine/` and `tests/daemon/`. They run without
public networking. Transmission interoperability and scale scenarios are opt-in;
see the [test harness guide](https://github.com/spillrs/spill/blob/main/tests/interop/README.md).
