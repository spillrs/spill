//! Small CLI and text/JSON presentation; no per-feature command trees.
use crate::{
    config,
    control::{Request, Response},
};
use clap::{Parser, Subcommand};
use serde_json::{Map, Value};
use std::{
    io::{self, IsTerminal, Read},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Parser)]
#[command(
    name = "spill",
    version,
    about = "A modern torrent client built in Rust",
    after_help = "Run `spill serve` in one terminal, then use client commands in another.\n`spill create`, `spill inspect` and `spill verify` work offline. Torrent IDs and full-hash aliases accept unambiguous prefixes.\nUse --json for automation. Guide: https://spill.rs/docs/ | Rust engine: https://docs.rs/spill"
)]
pub struct Cli {
    #[arg(long, global = true)]
    /// Unix control socket of the running daemon (defaults to SPILL_SOCKET or platform default).
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
        /// TOML config path; omit to use built-in defaults.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Validate configuration and exit without starting the daemon.
        #[arg(long)]
        check: bool,
    },
    /// Create a .torrent from a file or directory without a running daemon.
    Create(crate::create::Arguments),
    /// Inspect a .torrent file or magnet link without a running daemon.
    Inspect(crate::inspect::Arguments),
    /// Check complete local payload against a .torrent without modifying files or networking.
    Verify(crate::verify::Arguments),
    #[command(flatten)]
    Client(Operation),
}
#[derive(Subcommand)]
pub enum Operation {
    /// Add a local .torrent file (or - for stdin), magnet link, or HTTP(S) torrent URL.
    Add {
        source: String,
        /// Output directory (relative paths resolve from the current directory); defaults to download_dir.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Add paused, without starting discovery or payload I/O.
        #[arg(long)]
        paused: bool,
        /// Initial per-torrent setting as KEY=JSON; may be repeated.
        #[arg(long = "set", value_name = "KEY=JSON")]
        settings: Vec<String>,
    },
    /// Stop one or more torrents and keep them paused across restarts.
    Pause {
        #[arg(required = true)]
        ids: Vec<String>,
    },
    /// Allow one or more paused torrents to run when a slot is available.
    Resume {
        #[arg(required = true)]
        ids: Vec<String>,
    },
    /// Remove one or more torrents, keeping downloaded files unless explicitly deleted.
    Remove {
        #[arg(required = true)]
        ids: Vec<String>,
        #[arg(long)]
        /// Also delete payload files created by spill, never adopted files.
        delete_payload: bool,
    },
    /// Verify payload on disk, then leave the torrent paused.
    Recheck {
        #[arg(required = true)]
        ids: Vec<String>,
    },
    /// Announce again when the tracker permits it.
    Reannounce {
        #[arg(required = true)]
        ids: Vec<String>,
    },
    /// Show one torrent's progress, settings, accounting, peers, and sources.
    Get { id: String },
    /// Change per-torrent settings using KEY=JSON values.
    #[command(
        after_help = "Keys:\n  download_limit=<u64>       Download cap in bytes/s (0 = unlimited)\n  upload_limit=<u64>         Upload cap in bytes/s (0 = unlimited)\n  seed_ratio=<f64>           Stop seeding at upload/size ratio (0 = off)\n  seed_time=<u64>            Stop seeding after active seconds (0 = off)\n  file_priorities=<arr|obj>  [\"skip\",\"normal\",\"high\"] or {\"0\":\"skip\"}\n  trackers=<bool>            Allow tracker announces\n  dht=<bool>                 Allow DHT and local discovery\n  pex=<bool>                 Allow peer exchange\n  web_seeds=<bool>           Allow HTTP(S) web seeds"
    )]
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
    /// Show daemon health and torrents.
    Status,
    /// List files and priorities in torrent order.
    Files { id: String },
    /// Show daemon health, listeners, inbound peers, port mapping, and admission state.
    Health,
    /// Change daemon-wide payload caps until restart.
    SetBandwidth {
        /// Download cap in bytes per second; 0 means unlimited.
        #[arg(long, required_unless_present = "upload")]
        download: Option<u64>,
        /// Upload cap in bytes per second; 0 means unlimited.
        #[arg(long, required_unless_present = "download")]
        upload: Option<u64>,
    },
    /// Stop the daemon cleanly and save progress.
    Shutdown,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Usage(String),
    #[error("daemon unavailable: {0}")]
    Transport(String),
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
    let mut stream = tokio::net::UnixStream::connect(path).await.map_err(|e| {
        Error::Transport(format!(
            "{}: {e} (start with 'spill serve' or pass --socket)",
            path.display()
        ))
    })?;
    let mut bytes = serde_json::to_vec(request).map_err(|e| Error::Transport(e.to_string()))?;
    bytes.push(b'\n');
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| Error::Transport(format!("{}: {e}", path.display())))?;
    let mut bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    let mut read = BufReader::new(stream).take(64 * 1024 * 1024);
    tokio::time::timeout(Duration::from_secs(60), read.read_until(b'\n', &mut bytes))
        .await
        .map_err(|_| Error::Transport(format!("{}: timed out waiting for reply", path.display())))?
        .map_err(|e| Error::Transport(format!("{}: {e}", path.display())))?;
    if bytes.is_empty() {
        return Err(Error::Transport(format!(
            "{}: empty response from daemon",
            path.display()
        )));
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| Error::Transport(format!("{}: invalid response: {e}", path.display())))
}
fn parse_values(settings: Vec<String>) -> Result<Map<String, Value>, Error> {
    let mut parsed = Map::new();
    for setting in settings {
        let (key, value) = setting
            .split_once('=')
            .ok_or_else(|| Error::Usage(format!("expected KEY=JSON, got '{setting}'")))?;
        let value = serde_json::from_str(value)
            .map_err(|e| Error::Usage(format!("invalid JSON value for {key}: {e}")))?;
        if parsed.insert(key.into(), value).is_some() {
            return Err(Error::Usage(format!("duplicate key {key}")));
        }
    }
    Ok(parsed)
}
fn clean(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { '\u{FFFD}' } else { c })
        .collect()
}
pub(crate) fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
fn human_rate(rate: f64) -> String {
    if rate <= 0.0 {
        "-".into()
    } else {
        format!("{}/s", human_bytes(rate.round() as u64))
    }
}
fn human_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else if seconds < 86_400 {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    } else {
        format!("{}d {}h", seconds / 86_400, (seconds % 86_400) / 3600)
    }
}
fn list_text(rows: &[Value]) {
    println!(
        "{:<12}  {:<11}  {:>25}  {:>10}  {:>10}  {:>5}  NAME",
        "ID", "STATE", "PROGRESS", "DOWN", "UP", "PEERS"
    );
    for row in rows {
        let id = row["id"].as_str().unwrap_or("");
        let short = &id[..id.len().min(12)];
        let state = row["state"].as_str().unwrap_or("?");
        let progress = if row["total_bytes"].is_null() {
            "-".to_owned()
        } else {
            let wanted = row["wanted_bytes"]
                .as_u64()
                .unwrap_or_else(|| row["total_bytes"].as_u64().unwrap_or(0));
            let verified = row["wanted_verified_bytes"]
                .as_u64()
                .unwrap_or_else(|| row["verified_bytes"].as_u64().unwrap_or(0));
            let pct = if row["completed"] == true {
                100.0
            } else if wanted == 0 {
                0.0
            } else {
                (100.0 * verified as f64 / wanted as f64).min(99.9)
            };
            format!(
                "{} / {} ({pct:.1}%)",
                human_bytes(verified),
                human_bytes(wanted)
            )
        };
        let down = row["transfer"]["download_bytes_per_second"]
            .as_f64()
            .unwrap_or(0.0);
        let up = row["transfer"]["upload_bytes_per_second"]
            .as_f64()
            .unwrap_or(0.0);
        let peers = row["transfer"]["active_peers"].as_u64().unwrap_or(0);
        let name = clean(row["name"].as_str().unwrap_or("magnet"));
        println!(
            "{short:<12}  {state:<11}  {progress:>25}  {:>10}  {:>10}  {peers:>5}  {name}",
            human_rate(down),
            human_rate(up)
        );
        if let Some(error) = row["last_error"].as_str() {
            println!("  Error: {}", clean(error));
        }
        if let Some(error) = row["tracker_error"].as_str() {
            println!("  Tracker: {}", clean(error));
        }
    }
}
fn files_text(files: &[Value]) {
    println!("{:>5}  {:>10}  {:<8}  PATH", "INDEX", "SIZE", "PRIORITY");
    for file in files {
        let index = file["index"].as_u64().unwrap_or(0);
        let length = human_bytes(file["length"].as_u64().unwrap_or(0));
        let priority = file["priority"].as_str().unwrap_or("?");
        let path = clean(file["path"].as_str().unwrap_or(""));
        let tag = if file["padding"] == true {
            " [padding]"
        } else {
            ""
        };
        println!("{index:>5}  {length:>10}  {priority:<8}  {path}{tag}");
    }
}
fn get_text(row: &Value) {
    let name = clean(row["name"].as_str().unwrap_or("magnet"));
    println!("{:<18} {name}", "Name:");
    println!("{:<18} {}", "ID:", row["id"].as_str().unwrap_or(""));
    println!("{:<18} {}", "Format:", row["format"].as_str().unwrap_or(""));
    if let Some(v1) = row["v1"].as_str() {
        println!("{:<18} {v1}", "SHA-1:");
    }
    if let Some(v2) = row["v2"].as_str() {
        println!("{:<18} {v2}", "SHA-256:");
    }
    println!(
        "{:<18} {} (intent: {})",
        "State:",
        row["state"].as_str().unwrap_or("?"),
        row["intent"].as_str().unwrap_or("?")
    );
    if let Some(dir) = row["output_dir"].as_str() {
        println!("{:<18} {}", "Save to:", clean(dir));
    }
    if let Some(total) = row["total_bytes"].as_u64() {
        let wanted = row["wanted_bytes"].as_u64().unwrap_or(total);
        let verified = row["wanted_verified_bytes"].as_u64().unwrap_or(0);
        let pct = if row["completed"] == true {
            100.0
        } else if wanted == 0 {
            0.0
        } else {
            (100.0 * verified as f64 / wanted as f64).min(99.9)
        };
        println!(
            "{:<18} {} / {} ({pct:.1}%)",
            "Progress:",
            human_bytes(verified),
            human_bytes(wanted)
        );
    } else {
        println!("{:<18} awaiting metadata", "Progress:");
    }
    if let Some(transfer) = row["transfer"].as_object() {
        let down = transfer
            .get("download_bytes_per_second")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let up = transfer
            .get("upload_bytes_per_second")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let peers = transfer
            .get("active_peers")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        println!(
            "{:<18} down {}  up {}  peers {peers}",
            "Transfer:",
            human_rate(down),
            human_rate(up)
        );
    }
    let dl = row["downloaded_bytes"].as_u64().unwrap_or(0);
    let ul = row["uploaded_bytes"].as_u64().unwrap_or(0);
    let seed_secs = row["seed_seconds"].as_f64().unwrap_or(0.0) as u64;
    println!(
        "{:<18} down {}  up {}  seed {}",
        "Lifetime:",
        human_bytes(dl),
        human_bytes(ul),
        human_duration(seed_secs)
    );
    if let Some(settings) = row["settings"].as_object() {
        let dl_cap = settings
            .get("download_limit")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let ul_cap = settings
            .get("upload_limit")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let cap = |n| {
            if n == 0 {
                "unlimited".to_owned()
            } else {
                format!("{}/s", human_bytes(n))
            }
        };
        println!("{:<18} down {}  up {}", "Caps:", cap(dl_cap), cap(ul_cap));
        let sources: Vec<_> = ["trackers", "dht", "pex", "web_seeds"]
            .into_iter()
            .filter(|k| settings.get(*k) == Some(&Value::Bool(true)))
            .collect();
        println!(
            "{:<18} {}",
            "Sources:",
            if sources.is_empty() {
                "none".to_owned()
            } else {
                sources.join(", ")
            }
        );
    }
    if let Some(error) = row["last_error"].as_str() {
        println!("{:<18} {}", "Error:", clean(error));
    }
    if let Some(error) = row["tracker_error"].as_str() {
        println!("{:<18} {}", "Tracker warning:", clean(error));
    }
    if let Some(error) = row["transfer"]["metadata_error"].as_str() {
        println!("{:<18} {}", "Metadata error:", clean(error));
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
    for (label, key) in [
        ("Session download cap:", "download_limit"),
        ("Session upload cap:", "upload_limit"),
    ] {
        if let Some(rate) = data["bandwidth"][key].as_u64() {
            if rate == 0 {
                println!("{label:<26} unlimited");
            } else {
                println!("{label:<26} {}/s", human_bytes(rate));
            }
        }
    }
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
                    Some(seconds) => format!("{} ago", human_duration(seconds)),
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
    let logging = &data["logging"];
    let log_status = if logging["file_enabled"] != true {
        "disabled".to_owned()
    } else if logging["write_failed"] == true {
        format!(
            "write failed ({} dropped)",
            logging["events_dropped"].as_u64().unwrap_or(0)
        )
    } else if logging["events_dropped"].as_u64().unwrap_or(0) > 0 {
        format!(
            "ok ({} dropped)",
            logging["events_dropped"].as_u64().unwrap_or(0)
        )
    } else if let Some(path) = logging["path"].as_str() {
        format!("ok ({})", clean(path))
    } else {
        "ok".to_owned()
    };
    println!("{:<26} {log_status}", "Debug log:");
}

fn build_add_request(
    source: String,
    output: Option<PathBuf>,
    paused: bool,
    settings: Vec<String>,
) -> Result<Request, Error> {
    let output_dir = match output {
        Some(path) if path.is_absolute() => Some(path),
        Some(path) => Some(
            std::env::current_dir()
                .map_err(|e| Error::Usage(format!("cannot resolve output path: {e}")))?
                .join(path),
        ),
        None => None,
    };
    let (metainfo, magnet, url) = if source
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("magnet:"))
    {
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
        let mut bytes = Vec::new();
        let limit = spill::engine::profile::MAX_META as u64 + 1;
        if source == "-" {
            io::stdin()
                .take(limit)
                .read_to_end(&mut bytes)
                .map_err(|e| Error::Usage(format!("stdin: {e}")))?;
        } else {
            let file =
                std::fs::File::open(&source).map_err(|e| Error::Usage(format!("{source}: {e}")))?;
            file.take(limit)
                .read_to_end(&mut bytes)
                .map_err(|e| Error::Usage(format!("{source}: {e}")))?;
        }
        if bytes.len() > spill::engine::profile::MAX_META {
            return Err(Error::Usage("metainfo exceeds 16 MiB".into()));
        }
        (Some(bytes), None, None)
    };
    Ok(Request::Add {
        metainfo,
        magnet,
        url,
        output_dir,
        paused,
        settings: parse_values(settings)?,
    })
}

async fn run_batch(
    socket: &Path,
    ids: Vec<String>,
    json: bool,
    mut build: impl FnMut(String) -> Request,
    mut print_one: impl FnMut(&str, &Value),
) -> Result<(), Error> {
    let single = ids.len() == 1;
    let mut results = Vec::with_capacity(ids.len());
    let mut first_error = None;
    for id in ids {
        let response = query(socket, &build(id.clone())).await?;
        if response.code != 0 {
            let err = Error::Remote(response.code, response.error.unwrap_or_default());
            if single {
                return Err(err);
            }
            eprintln!("{id}: {err}");
            if first_error.is_none() {
                first_error = Some(err);
            }
            continue;
        }
        if json {
            results.push(response.data);
        } else {
            print_one(&id, &response.data);
        }
    }
    if json {
        if single {
            if let Some(data) = results.into_iter().next() {
                println!("{data}");
            }
        } else {
            println!("{}", Value::Array(results));
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

pub async fn run(command: Operation, socket: Option<PathBuf>, json: bool) -> Result<(), Error> {
    let socket = socket
        .or_else(|| {
            std::env::var_os("SPILL_SOCKET")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
        })
        .unwrap_or_else(config::socket);
    match command {
        Operation::Pause { ids } => {
            return run_batch(
                &socket,
                ids,
                json,
                |id| Request::Pause { id },
                |_, data| println!("paused {}", data["id"].as_str().unwrap_or("")),
            )
            .await;
        }
        Operation::Resume { ids } => {
            return run_batch(
                &socket,
                ids,
                json,
                |id| Request::Resume { id },
                |_, data| println!("resumed {}", data["id"].as_str().unwrap_or("")),
            )
            .await;
        }
        Operation::Recheck { ids } => {
            return run_batch(
                &socket,
                ids,
                json,
                |id| Request::Recheck { id },
                |_, data| println!("rechecking {}", data["id"].as_str().unwrap_or("")),
            )
            .await;
        }
        Operation::Reannounce { ids } => {
            return run_batch(
                &socket,
                ids,
                json,
                |id| Request::Reannounce { id },
                |id, _| println!("reannounced {id}"),
            )
            .await;
        }
        Operation::Remove {
            ids,
            delete_payload,
        } => {
            return run_batch(
                &socket,
                ids,
                json,
                |id| Request::Remove { id, delete_payload },
                |_, data| {
                    let id = data["removed"].as_str().unwrap_or("");
                    if data["delete_payload"] == true {
                        if data["deleted_payload"] == true {
                            println!("removed {id} (payload deleted)");
                        } else {
                            println!("removed {id} (adopted files kept)");
                        }
                    } else {
                        println!("removed {id}");
                    }
                },
            )
            .await;
        }
        _ => {}
    }

    let watch = matches!(command, Operation::List { watch: true });
    let kind = match &command {
        Operation::Add { .. } => "add",
        Operation::Get { .. } => "get",
        Operation::Set { .. } => "set",
        Operation::List { .. } => "list",
        Operation::Status => "status",
        Operation::Files { .. } => "files",
        Operation::Health => "health",
        Operation::SetBandwidth { .. } => "set-bandwidth",
        Operation::Shutdown => "shutdown",
        _ => unreachable!("batch ops handled above"),
    };
    let request = match command {
        Operation::Add {
            source,
            output,
            paused,
            settings,
        } => build_add_request(source, output, paused, settings)?,
        Operation::Get { id } => Request::Get { id },
        Operation::Set { id, values } => Request::Set {
            id,
            values: parse_values(values)?,
        },
        Operation::List { .. } => Request::List,
        Operation::Status => Request::Status,
        Operation::Files { id } => Request::Files { id },
        Operation::Health => Request::Health,
        Operation::SetBandwidth { download, upload } => Request::SetBandwidth {
            download_limit: download,
            upload_limit: upload,
        },
        Operation::Shutdown => Request::Shutdown,
        _ => unreachable!("batch ops handled above"),
    };

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
        } else {
            match kind {
                "add" => println!("{}", response.data["id"].as_str().unwrap_or("")),
                "get" => get_text(&response.data),
                "list" => list_text(
                    response.data["torrents"]
                        .as_array()
                        .map_or(&[], Vec::as_slice),
                ),
                "status" => {
                    health_text(&response.data["health"]);
                    println!();
                    list_text(
                        response.data["torrents"]
                            .as_array()
                            .map_or(&[], Vec::as_slice),
                    );
                }
                "files" => files_text(response.data["files"].as_array().map_or(&[], Vec::as_slice)),
                "health" => health_text(&response.data),
                "set" | "set-bandwidth" => {
                    if let Some(settings) = response.data.as_object() {
                        for (key, value) in settings {
                            println!("{key}={value}");
                        }
                    }
                }
                "shutdown" => println!("daemon stopping"),
                _ => println!("{}", serde_json::to_string_pretty(&response.data).unwrap()),
            }
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
