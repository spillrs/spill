# Architecture

This page is for curious users and contributors. It describes how Spill is put
together, not every detail; the code is the reference. The
[Rust API reference](https://docs.rs/spill) documents the library.

## Package and modules

Spill is one Cargo package, published as `spill`, with a library and a binary:

```text
src/protocol     wire formats only: bencode, metainfo, magnets, peer messages,
                 tracker and DHT messages, the resume format. No I/O.
src/engine       the BitTorrent engine: sessions, torrents, peers, discovery,
                 storage.
src/daemon       the `spill` binary: config, SQLite catalog, supervisor,
                 control socket, CLI and web interface.
deploy           example configs, systemd units and a launchd agent.
```

The library exposes `spill::protocol` and `spill::engine`; the binary starts at
`src/daemon/main.rs`. `protocol` knows nothing about sockets or files, `engine`
knows nothing about the catalog or the CLI, and `daemon` only uses the engine's
public API. Engine internals are restricted to the engine module.

Codec tests live in `tests/protocol/`; end-to-end scenarios live in
`tests/daemon/` and `tests/engine/`, with shared harness code in `tests/common/`.
Run tiers 0 and 1 without public networking with `cargo test --locked`.

### Embedding

The default `daemon` feature builds the binary and its database, CLI and web
integration. Library consumers use `default-features = false` to exclude
`rusqlite`, `axum`, `clap`, `toml` and `serde_json`. HTTP/TLS and filesystem
support remain engine dependencies. On macOS, engine gateway discovery still
needs Tokio's process support when port mapping is explicitly enabled.

The library requires a caller-owned Tokio runtime on Linux/macOS, with I/O and
time drivers enabled. Current-thread and multithread runtimes are supported;
consumers enable Tokio's `rt-multi-thread` for the latter. Spill creates no runtime,
signal handler or global tracing subscriber.

`spill::engine::download(source, destination).await` owns a short-lived session
and joins cleanup before returning success or failure. It uses an ephemeral
IPv4 listener, DHT with the [default routers](networking.md#dht), no port mapping
and no LAN permission. Dropping its future requests stop but cannot await
cleanup. For cancellation with cleanup proof, retain an explicit session/handle,
request stop and then join shutdown, even if an earlier wait timed out.

Two examples build without daemon features:

```sh
mkdir -p downloads recovery
cargo run --no-default-features --example download -- FILE.torrent downloads
cargo run --no-default-features --example download -- --magnet 'magnet:?...' downloads
cargo run --no-default-features --example progress -- downloads recovery A.torrent B.torrent
```

`download()` keeps no recovery record. Spill never overwrites files it has no
recorded authority for, so rerunning it after an interruption fails with
`AlreadyExists` until the partial files are removed.

Applications with durable storage supply the optional binding and other
acknowledgements through `Persistence`; applications without it need no
callbacks. The progress example saves the destination binding, resume
checkpoints and completed-announce delivery state from these callbacks, and on
the next run passes them back as `Destination::Recover`, so interrupted
downloads resume. The resume record alone does not authorize reopening partial
payload; the binding does, and only for the directory it was recorded in, so
store the two together. Store the resume bytes as given, and
`DestinationBinding` through its serde implementation. Its serialized form may
change before 1.0.

## One process, one async thread

The daemon runs on a single-threaded Tokio runtime. Every network operation is
asynchronous on that thread; anything that blocks (file reads and writes,
SHA-1 hashing) runs on Tokio's blocking thread pool.

```text
            spill / web page
                     │ JSON
          ┌──────────▼──────────┐
          │ daemon: supervisor  │── SQLite catalog (torrents, settings,
          │ (reconcile ~1 s)    │   accounting, resume data, DHT nodes)
          └──────────┬──────────┘
                     │ admit / stop
          ┌──────────▼──────────┐
          │ engine: Session     │── TCP listener, UDP (DHT + trackers + uTP),
          │                     │   dial queue, bandwidth budget, DHT node
          └───┬────────────┬────┘
              │            │      one task per running torrent
      ┌───────▼───┐  ┌─────▼─────┐
      │ torrent A │  │ torrent B │ …
      └───────────┘  └───────────┘
```

## The daemon

- The **catalog** (`catalog.sqlite` in WAL mode) is the source of truth: each
  torrent's metainfo or magnet, output directory, intent (run, paused, recheck,
  remove), settings, uploaded bytes, seeding time, resume data, the files Spill
  created, and errors with retry times.
- The **supervisor** reconciles the catalog with what is running about once a
  second and immediately after any command. It starts torrents into free slots,
  stops paused ones, applies seed goals, schedules retries and records
  accounting.
- **Control** requests from the Unix socket and the web page go through the
  same in-process dispatcher.

## The engine

### Session

The session owns everything torrents share: the listening TCP socket (incoming
handshakes are routed to the right torrent by info hash), one UDP socket on the
same port (DHT traffic, tracker replies and uTP peer packets are demultiplexed
there), the uTP connections and timers, the outgoing connection queue, the global bandwidth budget,
the HTTP client for trackers and the DHT node. It also enforces the address
policy (`allow_local_network`) and the daemon-wide resource limits.
`Session::bind` takes separate `NetworkOptions` and `Limits`. Network defaults
use an ephemeral IPv4 port, uTP, no port mapping and no LAN permission; reusable
sessions permit DHT only with `NetworkOptions::dht = Some(DhtOptions { ... })`.
`dht_state()` does not start an unused node. See [Networking](networking.md#dht)
for initialization and failure behaviour.
Session handles are cloneable: dropping one clone does not stop transfers.
Dropping the last requests stop; await `shutdown()` to prove cleanup finished.
Cancelled shutdown waits remain joinable through another handle. After joined
shutdown, dropping all session clones releases the listening port even if finished
torrent handles are retained.
`admit(TorrentOptions::new(source, destination))` returns a `TorrentHandle`
synchronously after validation and reservation. Sources are metainfo or parsed
magnets; the engine resolves metadata and prepares the destination. No catalog
or persistence callbacks are required. `inspect()` resolves a magnet without
creating payload files, using the same admission limits. Offline sessions accept
only metainfo rechecks.

Destinations require an existing directory: `Open` safely creates or adopts a
complete seed, `Create` refuses existing payload, `Adopt` verifies a read-only
seed, and `Recover` requires previously recorded authority. `recovery()` retains
that authority and the latest acknowledged checkpoints for a subsequent execution.
Stop is terminal; resuming means admitting a new execution after joined cleanup.
Finished executions retire automatically; old handles cannot affect replacements.

Transfer handles retain outcomes after retirement: `wait_complete()` waits for wanted
content, while `outcome()` reports termination, errors and whether cleanup finished.
Storage and checkpoint failures preserve their underlying error kind and cause.
`changed()` yields latest-value observations and returns false after the final
report is consumed, without requiring a separate join waiter.
A clean incomplete stop is not a successful download. A failed execution can still
prove cleanup after its peers, writer and blocking jobs have joined; inspect
`cleanup_complete` before reusing its destination. Resume and tracker-delivery
callbacks, including resolved-metadata and destination-binding acknowledgements,
run on blocking threads. Metadata acknowledgement precedes destination preparation;
binding acknowledgement precedes file creation. Resume acknowledgements precede
content completion; delivery acknowledgements precede advancing tracker delivery.
Stop and shutdown join in-progress callbacks; they have no hard deadline.

For an embedded download, `RunPolicy::UntilComplete` stops after wanted-content
completion. It allows up to five seconds for a pending tracker completed announce;
no pending delivery means no grace period. Partial selection does not announce
full-torrent completion. Grace expiry is not a content failure, but persistence
or cleanup failures still fail the execution. This grace does not bound blocking
I/O or joined cleanup. The daemon uses `Persistent` to keep serving until stopped.

### A torrent

Both daemon and library transfers use the same engine-owned lifecycle, including
metadata resolution, preparation, recovery and joined cleanup. Within transfer,
the three parts are:

- **Core**: a deterministic state machine. It receives inputs (a peer message,
  a finished disk write, a timer tick) and returns actions (send a message,
  write a block, hash a piece, connect to an address). It does no I/O and never
  reads the clock itself. All BitTorrent decisions live here: piece picking
  (rarest first, file priorities, endgame), requests and timeouts, choking,
  extensions (BEP 6, 9, 10, 11), bans.
- **Shell**: performs the core's actions on real peer sockets and storage and
  feeds results back as inputs.
- **Discovery**: trackers, DHT lookups, PEX contacts and web seeds produce
  addresses and connections for the shell.

Each peer connection runs as its own small task that reads and writes frames,
with bounded queues so a slow peer can't use unbounded memory.

### Storage

Each torrent has one writer task between the core and the disk:

```text
peer → shell → core ─write block─▶ writer ─pwrite─▶ files (*.part)
                    ◀─written────          ─hash──▶ blocking pool
                    ◀─verified / failed─
peer ◀─ shell ◀─ core ◀─block──── writer ◀─pread── files (upload cache)
```

- Blocks are written with positional writes straight to the destination files.
- A piece is hashed from memory when it's still buffered, otherwise read back
  from disk. Only verified pieces are ever uploaded.
- Uploads are served through a small read-ahead cache.
- Files are renamed from `NAME.part` to `NAME` when complete.
- Resume data (verified pieces plus file fingerprints) is handed to the daemon
  every two minutes, at completion and at stop. Files are synced to disk only at
  completion.

## Design rules

- **Correctness first**: only data that matches its piece hash is kept as
  verified or uploaded; remote input is validated at the boundary and a bad peer
  is disconnected, never allowed to fail the torrent.
- **Bounded everything**: queues, caches, connection counts and message sizes
  all have fixed limits.
- **Joined shutdown**: stopping a torrent or the daemon waits for every peer,
  disk and discovery task to finish before releasing files.
- **Small and direct**: concrete types over abstraction layers, end-to-end tests
  over unit tests. See `AGENTS.md` for the full contributor rules.
