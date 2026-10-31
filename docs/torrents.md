# Torrents

## States

`spill list` and the web page show one state per torrent:

| State | Meaning |
|---|---|
| `queued` | Waiting for a free download or seed slot. |
| `starting` | Admitted; preparing to run. |
| `metadata` | A magnet link is fetching the torrent's metadata from peers. |
| `recovering` | Opening files and checking which pieces are already valid. |
| `checking` | Running an explicit `recheck`. |
| `downloading` | Transferring data. |
| `seeding` | Complete (for the selected files) and uploading. |
| `paused` | Stopped by you or by a seed goal. Stays stopped across restarts. |
| `retrying` | Failed; spill retries automatically. `get` shows the error and when the next attempt happens. |

## Queueing

`active_downloads` and `active_seeds` limit how many torrents run at once, as
two separate pools. A torrent counts as a seed once it is complete. Torrents
that don't fit wait as `queued` and start in the order they were added when a
slot frees up. A torrent that is already running keeps its slot: resuming or
adding another torrent never stops one that is transferring.

## Adding torrents

Add a local `.torrent` file, an HTTP(S) torrent URL, or a magnet link.
The daemon fetches torrent URLs and checks the file before adding it. A magnet
link is added right away and resolves its metadata from peers found via its
`tr=` trackers and the DHT; trackers from the magnet are kept for the download.

Torrents are saved under the output directory: a single-file torrent as one
file, a multi-file torrent in a directory named after the torrent. Names are
made safe for the filesystem: `:` and `\` become `_`, components such as `..`
are rejected, and over-long names are shortened with a short hash so they stay
unique.

### Existing files

- If **all** of a torrent's files already exist at the destination, spill
  verifies them and seeds them without downloading. These adopted files are
  treated as read-only: spill never writes to or deletes them.
- If only **some** of them exist, or a directory sits where a file should go,
  the add fails with `file already exists; choose another output directory`.
  spill never overwrites files it didn't create.

### `.part` files

While downloading, each non-empty file is named `NAME.part`. When every piece
of a file is verified, it is renamed to `NAME`. A file you've set to `skip`
stays `.part` (and sparse) unless pieces shared with a selected file fill it.

## File selection

`spill files ID` lists every file with its priority. Set priorities with
`set ID 'file_priorities=[...]'`, one of `skip`, `normal` or `high` per file:

- `high` files are fetched before `normal` ones.
- `skip` files are not requested. Pieces that span a selected and a skipped
  file are still downloaded completely, so part of a skipped file may be written.
- A torrent is complete, and starts seeding, when every selected file is.
- Padding files (BEP 47) are never written to disk and must be `skip`.

## Seeding goals

A torrent pauses itself when it reaches `seed_ratio` (uploaded bytes divided by
the torrent's size) or `seed_time` (seconds spent seeding, not counting pauses
or downtime). Seeding duration is measured by the engine at lifecycle transitions,
not inferred from supervisor sampling. Uploaded bytes and seeding time are saved
and survive restarts.

## Accounting

Each torrent keeps lifetime downloaded/uploaded payload totals, seeding time,
and added/completed/last-transfer timestamps. Adopted files contribute verified
progress but no downloaded traffic. Corrupt payload and re-downloads count as
traffic, so downloaded bytes can exceed the torrent's size. Persistent
`hash_failed_bytes` and `hash_failed_pieces` count failed downloaded-piece hash
checks (including repeated failures), not corruption found during startup or
recheck. They indicate wasted traffic, but cannot by themselves distinguish bad
sources from local storage problems. Seeding time includes
idle time while ready and complete, not just time spent sending bytes.

Accounting is saved about every minute, on recorded completion-state changes,
and when a run stops. An abrupt crash can lose accounting since the last
checkpoint; power loss can also lose recent SQLite commits. There is no stored
time series. Removing a torrent deletes its accounting.

Completion and selected-file progress remain available while paused or queued.
See [Command line](cli.md#accounting-in-json) for field definitions. Seed ratios
continue to use lifetime uploaded bytes divided by the full torrent size, not
by downloaded bytes or selected-file size.

## Recheck

`spill recheck ID` hashes every piece on disk, records the result, then
leaves the torrent paused. Resume it to continue: missing or corrupt pieces are
downloaded again, and a complete torrent seeds. Recheck after moving files,
editing them, or an unclean power loss during a download.

A magnet with unresolved metadata may use its enabled trackers or DHT to obtain
metadata first. The subsequent scan never downloads missing payload or serves
pieces. Library offline rechecks require supplied metainfo.

## Restarts, crashes and durability

spill saves each torrent's verified pieces together with a fingerprint (size,
modification time and identity) of every file:

- every two minutes while a torrent runs,
- when it completes,
- when it stops.

On the next start, files whose fingerprint still matches are trusted; pieces
in files that changed are hashed again. A crash therefore costs at most the
unsaved progress plus rehashing the files that were being written.

Like other clients, spill does not sync data to disk on every write. It syncs
a torrent's files once, when the download completes, before marking it complete.
After a power loss during a download, data written in the last moments may be
lost without changing the files' fingerprint; a `recheck` finds and repairs it.

## Failures and retries

If a torrent fails (disk full, permission denied, a file removed underneath
it), it shows the error and enters `retrying`. spill retries after 10 seconds,
doubling the delay each time up to 30 minutes. Pausing and resuming clears the
delay. Problems with a single peer, tracker or web seed never fail a torrent;
that source is dropped or retried on its own.

## Removing torrents

- `remove ID` deletes the torrent from spill and keeps all files.
- `remove ID --delete-payload` also deletes the files spill created for it,
  then any directories that became empty. Files spill did not create (adopted
  seeds, anything else in those directories) are left alone, and non-empty
  directories are kept.
