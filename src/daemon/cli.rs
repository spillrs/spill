//! Small CLI and text/JSON presentation; no per-feature command trees.
use crate::{
    config,
    control::{Request, Response},
};
use clap::{Parser, Subcommand};
use serde_json::{Map, Value};
use std::{
    io::{self, IsTerminal},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Parser)]
#[command(
    name = "spill",
    version,
    about = "A modern torrent client built in Rust",
    after_help = "Run `spill serve` in one terminal, then use client commands in another.\n`spill create` works offline. Torrent IDs and full-hash aliases accept unambiguous prefixes.\nUse --json for automation. Guide: https://spill.rs/docs/ | Rust engine: https://docs.rs/spill"
)]
pub struct Cli {
    #[arg(long, global = true)]
    /// Unix control socket of the running daemon.
    pub socket: Option<PathBuf>,
    #[arg(long, global = true)]
    /// Print response data as JSON for automation.
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    /// Run the single-user daemon in the foreground.
    Serve {
        /// Absolute TOML config path; omit to use built-in defaults.
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Create a .torrent from a file or directory without a running daemon.
    Create(crate::create::Arguments),
    #[command(flatten)]
    Client(Operation),
}
#[derive(Subcommand)]
pub enum Operation {
    /// Add a local .torrent file, magnet link, or HTTP(S) torrent URL.
    Add {
        source: String,
        /// Absolute output directory; defaults to the configured download_dir.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Add paused, without starting discovery or payload I/O.
        #[arg(long)]
        paused: bool,
        /// Initial per-torrent setting as KEY=JSON; may be repeated.
        #[arg(long = "set", value_name = "KEY=JSON")]
        settings: Vec<String>,
    },
    /// Stop a torrent and keep it paused across restarts.
    Pause { id: String },
    /// Allow a paused torrent to run when a slot is available.
    Resume { id: String },
    /// Remove a torrent, keeping downloaded files unless explicitly deleted.
    Remove {
        id: String,
        #[arg(long)]
        /// Also delete payload files created by spill, never adopted files.
        delete_payload: bool,
    },
    /// Verify payload on disk, then leave the torrent paused.
    Recheck { id: String },
    /// Announce again when the tracker permits it.
    Reannounce { id: String },
    /// Show one torrent's progress, settings, accounting, peers, and sources.
    Get { id: String },
    /// Change per-torrent settings using KEY=JSON values.
    Set {
        id: String,
        #[arg(required = true)]
        values: Vec<String>,
    },
    /// List torrents; --watch refreshes until interrupted.
    List {
        #[arg(long)]
        watch: bool,
    },
    /// Show torrents and daemon health.
    Status,
    /// List files and priorities in torrent order.
    Files { id: String },
    /// Show daemon health, listeners, inbound peers, port mapping, and admission state.
    Health,
    /// Stop the daemon cleanly and save progress.
    Shutdown,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Usage(String),
    #[error("daemon unavailable: {0}")]
    Transport(#[from] io::Error),
    #[error("{1}")]
    Remote(u8, String),
    #[error("interrupted")]
    Interrupted,
}
impl Error {
    pub fn code(&self) -> u8 {
        match self {
            Self::Usage(_) => 2,
            Self::Transport(_) => 4,
            Self::Remote(c, _) => *c,
            Self::Interrupted => 130,
        }
    }
}
pub async fn query(path: &Path, request: &Request) -> Result<Response, Error> {
    let mut stream = tokio::net::UnixStream::connect(path).await?;
    let mut bytes = serde_json::to_vec(request).map_err(io::Error::other)?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    let mut bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    let mut read = BufReader::new(stream).take(64 * 1024 * 1024);
    tokio::time::timeout(Duration::from_secs(60), read.read_until(b'\n', &mut bytes))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    serde_json::from_slice(&bytes).map_err(|e| Error::Transport(io::Error::other(e)))
}
fn request(operation: Operation) -> Result<Request, Error> {
    Ok(match operation {
        Operation::Add {
            source,
            output,
            paused,
            settings,
        } => {
            let (metainfo, magnet, url) = if source.starts_with("magnet:") {
                (None, Some(source), None)
            } else if source
                .get(..7)
                .is_some_and(|s| s.eq_ignore_ascii_case("http://"))
                || source
                    .get(..8)
                    .is_some_and(|s| s.eq_ignore_ascii_case("https://"))
            {
                (None, None, Some(source))
            } else {
                let file = std::fs::File::open(source).map_err(|e| Error::Usage(e.to_string()))?;
                use std::io::Read;
                let mut bytes = Vec::new();
                file.take(spill::engine::profile::MAX_META as u64 + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > spill::engine::profile::MAX_META {
                    return Err(Error::Usage("metainfo exceeds 16 MiB".into()));
                }
                (Some(bytes), None, None)
            };
            Request::Add {
                metainfo,
                magnet,
                url,
                output_dir: output,
                paused,
                settings: parse_values(settings)?,
            }
        }
        Operation::Pause { id } => Request::Pause { id },
        Operation::Resume { id } => Request::Resume { id },
        Operation::Remove { id, delete_payload } => Request::Remove { id, delete_payload },
        Operation::Recheck { id } => Request::Recheck { id },
        Operation::Reannounce { id } => Request::Reannounce { id },
        Operation::Get { id } => Request::Get { id },
        Operation::Set { id, values } => Request::Set {
            id,
            values: parse_values(values)?,
        },
        Operation::List { .. } => Request::List,
        Operation::Status => Request::Status,
        Operation::Files { id } => Request::Files { id },
        Operation::Health => Request::Health,
        Operation::Shutdown => Request::Shutdown,
    })
}
fn parse_values(settings: Vec<String>) -> Result<Map<String, Value>, Error> {
    let mut parsed = Map::new();
    for setting in settings {
        let (key, value) = setting
            .split_once('=')
            .ok_or_else(|| Error::Usage("expected key=JSON_VALUE".into()))?;
        let value = serde_json::from_str(value).map_err(|e| Error::Usage(e.to_string()))?;
        if parsed.insert(key.into(), value).is_some() {
            return Err(Error::Usage(format!("duplicate key {key}")));
        }
    }
    Ok(parsed)
}
fn clean(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { '�' } else { c })
        .collect()
}
fn text(data: &Value) {
    if let Some(rows) = data["torrents"].as_array() {
        let width = rows
            .iter()
            .filter_map(|row| row["id"].as_str())
            .map(str::len)
            .max()
            .unwrap_or(40);
        println!(
            "{:<width$}  STATE         VERIFIED / TOTAL       NAME",
            "ID"
        );
        for row in rows {
            let id = row["id"].as_str().unwrap_or("");
            println!(
                "{:<width$}  {:<12}  {} / {}  {}",
                id,
                row["state"].as_str().unwrap_or("?"),
                row["verified_bytes"],
                row["total_bytes"],
                clean(row["name"].as_str().unwrap_or("magnet"))
            );
            if let Some(error) = row["tracker_error"].as_str() {
                println!("  Tracker: {}", clean(error));
            }
        }
    } else if let Some(files) = data["files"].as_array() {
        for file in files {
            println!(
                "{}  {}  {}  {}",
                file["index"],
                file["length"],
                file["priority"],
                clean(file["path"].as_str().unwrap_or(""))
            );
        }
    } else if let Some(settings) = data
        .as_object()
        .filter(|m| m.contains_key("download_limit"))
    {
        for (key, value) in settings {
            println!("{key}={value}");
        }
    } else {
        println!("{}", serde_json::to_string_pretty(data).unwrap());
    }
}
fn health_text(data: &Value) {
    println!("{:<26} {}", "Version:", data["version"].as_str().unwrap());
    println!("{:<26} {}", "PID:", data["pid"]);
    println!("{:<26} {}", "Running torrents:", data["running"]);
    println!(
        "{:<26} {}",
        "Admission:",
        if data["admission_closed"] == true {
            "closed"
        } else {
            "open"
        }
    );
    println!("{:<26} {}", "Advertised port:", data["advertised_port"]);
    if let Some(network) = data.get("network") {
        for (label, key) in [
            ("TCP listener:", "tcp_listen"),
            ("UDP listener:", "udp_listen"),
        ] {
            println!(
                "{label:<26} {}",
                network[key].as_str().unwrap_or("not running")
            );
        }
        for (label, key) in [("uTP:", "utp_enabled"), ("DHT:", "dht_enabled")] {
            println!(
                "{label:<26} {}",
                if network[key] == true {
                    "enabled"
                } else {
                    "disabled"
                }
            );
        }
        for (transport, key) in [("TCP", "inbound_tcp"), ("uTP", "inbound_utp")] {
            let inbound = &network[key];
            println!(
                "{:<26} {} ({} public)",
                format!("Inbound {transport} peers:"),
                inbound["active"],
                inbound["active_public"]
            );
            println!(
                "{:<26} {} ({} public)",
                format!("Inbound {transport} handshakes:"),
                inbound["total"],
                inbound["public_total"]
            );
            println!(
                "{:<26} {}",
                format!("Last public {transport}:"),
                match inbound["last_public_ago_secs"].as_u64() {
                    Some(seconds) if seconds < 60 => format!("{seconds}s ago"),
                    Some(seconds) => format!("{}m {}s ago", seconds / 60, seconds % 60),
                    None => "none observed since startup".into(),
                }
            );
        }
    }
    let lsd = &data["network"]["lsd"];
    println!(
        "{:<26} {}",
        "Local discovery:",
        if lsd["enabled"] != true {
            "disabled"
        } else if lsd["active"] == true {
            "listening"
        } else {
            "unavailable"
        }
    );
    if let Some(error) = lsd["last_error"].as_str() {
        println!("{:<26} {}", "Local discovery error:", clean(error));
    }
    let mapping = &data["port_mapping"];
    println!(
        "{:<26} {}",
        "Port mapping:",
        if mapping["enabled"] != true {
            "disabled"
        } else {
            mapping["protocol"].as_str().unwrap_or("not established")
        }
    );
    if let Some(error) = mapping["last_error"].as_str() {
        println!("{:<26} {}", "Port mapping error:", clean(error));
    }
}

pub async fn run(command: Operation, socket: Option<PathBuf>, json: bool) -> Result<(), Error> {
    let health = matches!(command, Operation::Health);
    let watch = matches!(command, Operation::List { watch: true });
    let request = request(command)?;
    let socket = socket.unwrap_or_else(config::socket);
    let interrupted = crate::interrupt();
    tokio::pin!(interrupted);
    loop {
        let response = query(&socket, &request).await?;
        if response.code != 0 {
            return Err(Error::Remote(
                response.code,
                response.error.unwrap_or_default(),
            ));
        }
        if watch && !json && io::stdout().is_terminal() {
            print!("\x1b[2J\x1b[H");
        }
        if json {
            println!("{}", response.data);
        } else if health {
            health_text(&response.data);
        } else {
            text(&response.data);
        }
        if !watch {
            return Ok(());
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
            _ = &mut interrupted => return Err(Error::Interrupted),
        }
    }
}
