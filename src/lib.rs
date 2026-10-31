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
//! use spill::engine::{
//!     download,
//!     profile::Profile,
//!     torrent::{Destination, TorrentSource},
//! };
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> std::io::Result<()> {
//!     let bytes = tokio::fs::read("example.torrent").await?;
//!     let source = TorrentSource::Metainfo(Profile::default().parse_owned(&bytes)?);
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
//! Use [`engine::session::Session`] for concurrent torrents, progress, explicit
//! networking policy, persistence, and cancellation with joined cleanup.
//! [`engine::torrent::TorrentOptions`] configures each execution;
//! [`engine::torrent::Persistence`] acknowledges recovery records.
//! Dropping the one-shot download future or last session handle requests stop but
//! cannot await cleanup. Retain handles and explicitly join shutdown before reusing
//! destinations.
//!
//! [`protocol`] exposes pure wire codecs; [`engine`] owns networking and storage.
//! See the [library guide](https://spill.rs/docs/library/) and the `download` and
//! `progress` examples in the matching source release. The
//! [user guide](https://spill.rs/docs/) covers daemon installation and operation.

mod http;

pub mod engine;
pub mod protocol;
