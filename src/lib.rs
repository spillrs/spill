//! # spill
//!
//! A modern torrent client built in Rust.
//! Run the `spill` daemon or embed this engine directly in a Linux/macOS application.
//! No daemon process or catalog is required for embedding.
//!
//! ## Add the engine
//!
//! ```text
//! cargo add spill --no-default-features
//! cargo add tokio --features fs,macros,rt,net,time
//! ```
//!
//! The default `daemon` feature adds the CLI, SQLite catalog, and web interface.
//! Engine-only users disable it. Supply a Tokio runtime with I/O and time drivers;
//! current-thread and multithread runtimes are supported. spill installs no runtime,
//! signal handler, or global tracing subscriber.
//!
//! ## Download a torrent
//!
//! ```no_run
//! use spill::engine::{Destination, TorrentSource, download};
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> std::io::Result<()> {
//!     let bytes = tokio::fs::read("example.torrent").await?;
//!     let source = TorrentSource::from_metainfo_bytes(&bytes)?;
//!     tokio::fs::create_dir_all("downloads").await?;
//!     download(source, Destination::Open("downloads".into())).await
//! }
//! ```
//!
//! [`engine::download`] owns a short-lived session and joins cleanup before returning.
//! It enables public DHT discovery but not LAN access or router mapping. It stores
//! no recovery record: interrupted partial files require recorded destination
//! authority to resume, not another fresh download.
//!
//! ## Manage transfers in an application
//!
//! Use [`engine::Session`] for concurrent torrents, progress, explicit
//! networking policy, persistence, and cancellation with joined cleanup.
//! [`engine::TorrentOptions`] configures each execution;
//! [`engine::Persistence`] acknowledges recovery records.
//! Dropping the one-shot download future or last session handle requests stop but
//! cannot await cleanup. Retain handles and explicitly join shutdown before reusing
//! destinations.
//!
//! This example uses tracker discovery, reports wanted-content progress, and joins
//! shutdown even when admission or the transfer fails. Enable DHT explicitly in
//! [`engine::NetworkOptions`] when needed.
//!
//! ```no_run
//! use spill::engine::{
//!     Destination, Limits, NetworkOptions, RunPolicy, Session, TorrentOptions, TorrentSource,
//! };
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> std::io::Result<()> {
//!     let bytes = tokio::fs::read("example.torrent").await?;
//!     let source = TorrentSource::from_metainfo_bytes(&bytes)?;
//!     tokio::fs::create_dir_all("downloads").await?;
//!     let session = Session::bind(NetworkOptions::default(), Limits::default()).await?;
//!     let result = async {
//!         let mut options = TorrentOptions::new(
//!             source,
//!             Destination::Open("downloads".into()),
//!         );
//!         options.run = RunPolicy::UntilComplete;
//!         let mut torrent = session.admit(options)?;
//!         while torrent.changed().await {
//!             let progress = torrent.snapshot().progress;
//!             println!("{} / {} bytes", progress.wanted_verified_bytes, progress.wanted_bytes);
//!             if progress.completed {
//!                 break;
//!             }
//!         }
//!         torrent.outcome().await.require_complete()
//!     }.await;
//!     let shutdown = session.shutdown().await;
//!     result.and(shutdown)
//! }
//! ```
//!
//! ## Offline utilities
//!
//! [`engine::create::create`] builds torrent files from local payload, and
//! [`engine::provisioning::verify`] checks complete existing
//! payload against metainfo. Both are blocking, read-only operations on the
//! payload and need neither a session nor a Tokio runtime. Their API pages include
//! examples.
//!
//! [`protocol`] exposes pure wire codecs; [`engine`] owns networking and storage.
//! See the [library guide](https://spill.rs/docs/library/) and the `download` and
//! `progress` examples in the matching source release. The
//! [user guide](https://spill.rs/docs/) covers daemon installation and operation.

mod http;

pub mod engine;
pub mod protocol;
