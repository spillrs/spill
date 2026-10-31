# Rust library

Embed spill's BitTorrent engine in your application to download, seed, inspect
magnets, and manage concurrent torrents without a
separate daemon, SQLite catalog, CLI, or web server.

## Dependencies and runtime

```sh
cargo add spill --no-default-features
cargo add tokio --features fs,macros,rt,net,time
```

The default `daemon` feature includes the binary and its CLI, database, and web
dependencies. Disable it for engine-only applications. HTTP/TLS and filesystem
support remain engine dependencies. Linux and macOS are supported.

Supply a Tokio runtime with I/O and time drivers enabled. Current-thread and
multithread runtimes are supported; enable Tokio's `rt-multi-thread` feature for
the latter. spill creates no runtime, signal handler, or global tracing subscriber.

## Download a torrent

Create an output directory first, then use `spill::engine::download`:

```rust
use spill::engine::{
    download,
    profile::Profile,
    torrent::{Destination, TorrentSource},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let bytes = tokio::fs::read("example.torrent").await?;
    let source = TorrentSource::Metainfo(Profile::default().parse_owned(&bytes)?);
    tokio::fs::create_dir_all("downloads").await?;
    download(source, Destination::Open("downloads".into())).await
}
```

For magnets, parse the URI with `spill::protocol::magnet::Magnet::parse` and
pass `TorrentSource::Magnet`. Private torrents require metainfo, not magnets.
See the [API reference](https://docs.rs/spill) for exact signatures.

`download()` stops after the selected content completes and joins cleanup before
returning. It uses an ephemeral IPv4 listener, uTP, public DHT routers, no port
mapping, and no permission to contact LAN addresses. It keeps no recovery record.
If interrupted, rerunning refuses to overwrite partial files; use persisted
recovery for resumable transfers.

Dropping the download future requests stop but cannot await cleanup. For timeouts
or cancellation that must prove files have been released, use an explicit session.

## Sessions and progress

A `Session` shares listeners, discovery, and resource budgets across torrents.
Use `Session::bind(NetworkOptions, Limits)`, then
`session.admit(TorrentOptions::new(source, destination))` to obtain a
`TorrentHandle`. Admission validates and reserves resources synchronously;
metadata resolution and destination preparation happen asynchronously.

- `NetworkOptions` defaults to an ephemeral IPv4 listener, uTP enabled, no port
  mapping, no LAN permission, and no DHT. Set `dht = Some(DhtOptions { ... })`
  to permit DHT discovery. Unlike `download()`, reusable sessions do not enable
  DHT implicitly.
- `Limits` bounds aggregate resources; each torrent has a `Profile` and
  `TorrentSettings`. See [Limits](limits.md) and the API reference.
- `RunPolicy::Persistent` keeps serving after completion; `UntilComplete` stops
  after selected-content completion. `Recheck` verifies metainfo against disk
  without peer or discovery I/O. Offline sessions accept only metainfo rechecks.
- `Session::inspect` resolves magnet metadata without creating payload files.
- `snapshot()` gives current progress; `changed().await` observes latest-value
  updates, not an event history. It returns false after the final report.
- `wait_complete()` waits for wanted-content completion, not termination.
  `outcome()` reports termination, errors, and cleanup. A clean incomplete stop
  is not a successful download.

`UntilComplete` allows up to five seconds for a pending tracker completed
announce. This is not a deadline for blocking I/O, callbacks, or cleanup.

## Destinations and recovery

All destinations require an existing root directory:

| Destination | Behavior |
|---|---|
| `Open` | Create new payload, or verify and adopt an already complete seed. |
| `Create` | Refuse existing payload. |
| `Adopt` | Verify an existing read-only seed. |
| `Recover` | Reopen a previous execution using its recorded destination binding and checkpoints. |

spill never overwrites files it has no recorded authority to write.
A resume checkpoint alone does not authorize reopening partial payload:
`DestinationBinding` does. Store the binding, its original root directory,
resume bytes, and completed-announce delivery state together. Preserve resume
bytes unchanged and serialize the binding through its serde implementation.
Its format may change before 1.0.

`Persistence` provides optional acknowledgements for resolved metadata, destination
bindings, checkpoints, and completed-announce delivery. They run on blocking
threads. Acknowledgement failures fail the execution. Callbacks must not wait
synchronously for their own execution or session to stop.

Metadata acknowledgement precedes destination preparation; binding acknowledgement
precedes file creation. Resume acknowledgement precedes content completion;
tracker-delivery acknowledgement precedes advancing delivery state. Stop and
shutdown join callbacks already in progress, without a hard deadline.

Use `handle.recovery()` to retain the latest acknowledged recovery state for a
subsequent execution. For recovery across process restarts, persist it yourself.

## Stop and shutdown

Call `handle.request_stop()` then await `handle.outcome()`, or use `handle.stop()`.
Stop is terminal: resuming means admitting a new execution after joined cleanup.
Finished executions retire automatically; old handles cannot control replacements.

Call `session.shutdown().await` before releasing the session. Session clones
share executions; dropping one clone does not stop them. Dropping the last clone
requests stop but cannot await it. A cancelled shutdown wait can be joined again
through another retained handle. After shutdown, drop all session clones to
release its listening port.

`outcome().cleanup_complete` distinguishes a failed transfer with joined cleanup
from a failure that cannot prove resources were released. Inspect that outcome
before reusing a destination after an error. Session admission closes if cleanup
cannot be proven.

## Runnable examples

From a source checkout matching your library version:

```sh
mkdir -p downloads recovery
cargo run --no-default-features --example download -- FILE.torrent downloads
cargo run --no-default-features --example download -- --magnet 'magnet:?...' downloads
cargo run --no-default-features --example progress -- downloads recovery A.torrent B.torrent
```

The `download` example is minimal; `progress` demonstrates concurrent downloads,
progress reporting, persistence callbacks, and recovery after interruption.
Examples live in `examples/` in the
[source repository](https://github.com/spillrs/spill). Prefer these and the
versioned API reference over inferring library methods from daemon commands.
