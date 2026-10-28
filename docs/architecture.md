# Architecture

This page is for curious users and contributors. It describes how Spill is put
together, not every detail; the code is the reference.

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

Codec tests live in `tests/protocol/`, and daemon end-to-end tests in
`tests/daemon.rs`. Run them with `cargo test --test protocol` and
`cargo test --test daemon`, or run both with `cargo test --locked`.

## One process, one async thread

The daemon runs on a single-threaded Tokio runtime. Every network operation is
asynchronous on that thread; anything that blocks (file reads and writes,
SHA-1 hashing) runs on Tokio's blocking thread pool.

```text
            spill ctl / web page
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

### A torrent

Each running torrent is one task with three parts:

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
