# Rust library

Embed spill's BitTorrent engine in your application to create torrents, download,
seed, inspect magnets, and manage concurrent torrents without a
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
use spill::engine::{Destination, TorrentSource, download};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let bytes = tokio::fs::read("example.torrent").await?;
    let source = TorrentSource::from_metainfo_bytes(&bytes)?;
    tokio::fs::create_dir_all("downloads").await?;
    download(source, Destination::Open("downloads".into())).await
}
```

For magnets, construct the source with `TorrentSource::from_magnet(uri)?`.

`download()` stops after the selected content completes and joins cleanup before
returning. It uses an ephemeral IPv4 listener, uTP, public DHT routers, no port
mapping, preferred MSE peer encryption, and no permission to contact LAN addresses.
It keeps no recovery record.
If interrupted, rerunning refuses to overwrite partial files; use persisted
recovery for resumable transfers. Dropping the download future requests stop but
cannot await cleanup. For timeouts or cancellation that must prove files have
been released, use an explicit `Session`.

## Sessions and progress

A `Session` shares listeners, discovery, and resource budgets across torrents.
Core types (`Session`, `NetworkOptions`, `DhtOptions`, `Limits`, `TorrentOptions`,
`TorrentSource`, `Destination`, `RunPolicy`, `Profile`, `FilePriority`,
`FileSelection`, `RecoveryState`, `parse_owned`) are re-exported directly from
`spill::engine`.

```rust
use spill::engine::{
    Destination, DhtOptions, Limits, NetworkOptions, RunPolicy, Session, TorrentOptions,
    TorrentSource,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let bytes = tokio::fs::read("example.torrent").await?;
    let source = TorrentSource::from_metainfo_bytes(&bytes)?;
    tokio::fs::create_dir_all("downloads").await?;
    let session = Session::bind(
        NetworkOptions {
            dht: Some(DhtOptions::default()),
            ..Default::default()
        },
        Limits::default(),
    )
    .await?;
    let result = async {
        let mut options = TorrentOptions::new(source, Destination::Open("downloads".into()));
        options.run = RunPolicy::UntilComplete;
        let mut torrent = session.admit(options)?;
        while torrent.changed().await {
            let p = torrent.snapshot().progress;
            println!("{} / {} bytes", p.wanted_verified_bytes, p.wanted_bytes);
            if p.completed {
                break;
            }
        }
        torrent.outcome().await.require_complete()
    }
    .await;
    let shutdown = session.shutdown().await;
    result.and(shutdown)
}
```

Session-wide payload rates start unlimited. Use `Session::set_bandwidth_limits`
to set download and upload caps in bytes per second (`0` means unlimited), and
`Session::bandwidth_limits` to read them in that order. Limits apply to running
and future torrents and compose with per-torrent limits. Download limits pace
requests rather than bytes already in flight. Use
`TorrentHandle::set_bandwidth_limits` to change the per-torrent caps for an
existing execution without restarting it. Set rates in `TorrentOptions` for
future executions; the library does not persist rate changes.

- `NetworkOptions` defaults to an ephemeral IPv4 listener, uTP enabled,
  `PeerEncryption::Preferred`, no port mapping, no LAN permission, no DHT, and
  no LSD. Set `dht = Some(DhtOptions::default())` to enable default public DHT
  routers. Unlike `download()`, reusable sessions do not enable DHT implicitly.
  Local service discovery requires both `lsd = true` and
  `allow_local_network = true`; setting `lsd = true` without LAN permission
  reports an error in `network_status().await.lsd.last_error`.
- `TorrentOptions::no_progress_timeout` is `Option<Duration>` (default `None`).
  Set `Some(duration)` if you want an incomplete execution to exit with
  `TorrentExit::NoProgress` after a stall.
- `TorrentOptions::initial_peers` supplies direct initial peers (rejected for
  private metainfo).
- `Limits` bounds aggregate resources; each torrent has a `Profile` and
  `TorrentSettings`. See [Limits](limits.md) and the API reference.
- `RunPolicy::Persistent` keeps serving after completion; `UntilComplete` stops
  after selected-content completion (allowing up to 5 seconds for pending
  completed tracker announces). `Recheck` runs under `TorrentPhase::Checking`
  and verifies metainfo against disk without peer or discovery I/O. Offline
  sessions accept only metainfo rechecks.
- `Session::network_status().await` reports local listener-task readiness and
  passive inbound TCP/uTP handshake counts, public subsets and last-public ages,
  plus LSD readiness, announce/receive counts and its last error.
- `Session::inspect` resolves magnet metadata without creating payload files;
  await `handle.wait_metadata().await`.
- `snapshot()` gives current progress (`downloaded_bytes`, `uploaded_bytes`,
  `wanted_verified_bytes`, `wanted_bytes`, `phase`); `changed().await` observes
  latest-value updates, returning `false` after the final report.
  `handle.file_priorities()` returns the effective priorities once metadata is
  available (including BEP 53 `so=` selections).
- `wait_complete()` waits for wanted-content completion, not termination.
  `join()` joins termination and returns any execution error (even on a clean
  incomplete stop). `outcome().await.require_complete()` joins termination and
  requires both clean exit and wanted-content completion.

## Formats, magnets, and selection

Private torrents require metainfo, not magnets. `Session::admit` rejects
nonempty `TorrentOptions::initial_peers` for private metainfo; private outgoing
peers must come from its active tracker. See [Private trackers](private-trackers.md).

Use `FileSelection::from_fn(meta.layout(), |index, file| ...)` to construct
custom per-file priorities safely—it automatically keeps virtual BEP 47 padding
entries at `FilePriority::Skip`. BEP 53 `so` ranges on magnets apply after
metadata resolution unless `TorrentOptions.settings.file_priorities` is
non-empty. When restarting from saved metainfo instead of the original magnet,
also persist `handle.file_priorities()`.

`TorrentIdentity`, `InfoHashV1`, and `InfoHashV2` implement `Display`, `FromStr`,
and `Ord`. Parse a 40-char or 64-char hex hash with `str::parse::<TorrentIdentity>()`
to look up an execution via `Session::torrent(identity)`.
`TorrentHandle::info_hash()` returns the admission identity; snapshots contain
all hashes learned during resolution.

For v2/hybrid details:

- `TorrentDiagnostics` identifies tracker and DHT entries by their `swarm` key;
  `dht` contains one entry per swarm.
- `OwnedMetainfo::layers_pending()` identifies hybrids that verify with SHA-1
  while required v2 layers are absent.
- `Persistence::resolved` acknowledges magnet metainfo before payload preparation
  and again when a background hybrid layer rebuild completes. Store both
  publications and use `TorrentHandle::metainfo()` to retain the latest metainfo.
- `Layout::is_multi_file()` indicates that file paths are relative to `name`.
- `port_map::Gateway::Upnp(url::Url)` accepts a validated `url::Url` (or parse
  any gateway string with `Gateway::parse`).

## Verify and remove local payload

`spill::engine::provisioning::verify` checks complete existing payload against
an `OwnedMetainfo`, without a session or Tokio runtime. Supply the parent
download directory; paths use spill's normalized disk names. It is read-only,
stops at the first failure, and does not use network or resume state. Keep files
unchanged during the scan. Asynchronous callers should use `spawn_blocking`.
`spill::engine::provisioning::remove(root, &binding)` deletes writable payload
files recorded in a `DestinationBinding` while leaving read-only adopted seeds
(`binding.is_read_only()`) untouched.

## Create a torrent

`spill::engine::create::create` hashes a file or directory without a session,
network access, or a Tokio runtime:

```rust
use spill::engine::create::{self, Options};

fn main() -> std::io::Result<()> {
    let options = Options {
        announce: vec![vec!["https://tracker.example/announce".into()]],
        creation_date: None,
        ..Default::default()
    };
    let torrent = create::create(std::path::Path::new("payload"), &options,
        &mut |done, total| {
            eprintln!("{done}/{total} pieces");
            Ok(())
        })?;
    std::fs::write("payload.torrent", torrent.metainfo)
}
```

This is a blocking API; use `spawn_blocking` when calling it from an async
application. Call `options.validate()?` to check options before accessing the
filesystem. The progress callback runs on the calling thread. Return an error
from it to cancel; creation joins its workers before returning. The result
includes encoded metainfo, `CreatedTorrent::info_hash` as a `TorrentIdentity`,
root name, total bytes (including alignment padding), and piece count.
The caller owns output publication; the CLI additionally refuses overwrites and
publishes the completed document atomically.

`Options::piece_length` is in **bytes**, unlike the CLI's exponent. Defaults are
256 KiB pieces and the available CPU count. `Options::format` selects
`Format::V1` (the default), `Format::V2` or `Format::Hybrid`. V1 sorts whole
path bytes; v2 and hybrid sort path components and align files to pieces.
Symlinks are followed, and the source is never modified. Keep it unchanged until the call
returns. See [Creating torrents](cli.md#creating-torrents) for supported fields,
source traversal, limits, and the separate add-and-seed workflow; rustdoc owns
the exact API contract.

## Destinations and recovery

All destinations require an existing root directory:

| Destination | Behavior |
|---|---|
| `Open` | Create new payload, or verify and adopt an already complete seed. |
| `Create` | Refuse existing payload. |
| `Adopt` | Verify an existing read-only seed. |
| `Recover` | Reopen a previous execution using its recorded destination binding and checkpoints. |

spill never overwrites files it has no recorded authority to write.
Resume records bind the format, full torrent identity and pending/full layer
mode. A full hybrid cannot trust a pending-mode checkpoint without rechecking.
The rebuild saves a full-mode checkpoint, so a normal completed restart needs
no extra recheck. Fingerprints cover real files, including empty files, not
virtual padding. `Recover` fully rechecks
payload when a checkpoint is incompatible, using its existing destination binding.
Keep that binding and payload; checkpoint incompatibility alone does not require
a fresh state directory.

A resume checkpoint alone does not authorize reopening partial payload:
`DestinationBinding` does. `RecoveryState` derives `Serialize` and `Deserialize`
(along with `dht::SavedState`), so you can persist `(root, RecoveryState)` as
one record and update announce delivery via `RecoveryState::record_announce`.
Preserve resume bytes unchanged and treat the serialized format as an opaque
record tied to the spill version.

`Persistence` provides optional acknowledgements for resolved metadata, destination
bindings, checkpoints, and completed-announce delivery. The `completion_announce`
callback receives `(swarm_key, delivered)`; `RecoveryState::completed_announces`
stores `(swarm_key, delivered)` pairs. Preserve every swarm independently: `false`
means pending and `true` means acknowledged. Acknowledging one hybrid swarm does
not acknowledge the other. They run on blocking
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
through another retained handle. Joined shutdown releases both TCP and UDP
listening ports before returning, even if a clone is kept to read `dht_state()`.

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
