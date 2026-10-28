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
#[command(name = "spill", version, about = "Single-user BitTorrent daemon")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    Serve {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    Ctl(Args),
}
#[derive(clap::Args)]
pub struct Args {
    #[arg(long, global = true)]
    pub socket: Option<PathBuf>,
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Operation,
}
#[derive(Subcommand)]
pub enum Operation {
    Add {
        source: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Remove {
        id: String,
        #[arg(long)]
        delete_payload: bool,
    },
    Recheck {
        id: String,
    },
    /// Announce again when the tracker permits it.
    Reannounce {
        id: String,
    },
    Get {
        id: String,
    },
    Set {
        id: String,
        #[arg(required = true)]
        values: Vec<String>,
    },
    List {
        #[arg(long)]
        watch: bool,
    },
    Status,
    Files {
        id: String,
    },
    Health,
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
        Operation::Add { source, output } => {
            let (metainfo, magnet) = if source.starts_with("magnet:") {
                (None, Some(source))
            } else {
                let file = std::fs::File::open(source).map_err(|e| Error::Usage(e.to_string()))?;
                use std::io::Read;
                let mut bytes = Vec::new();
                file.take(spill::engine::profile::MAX_META as u64 + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > spill::engine::profile::MAX_META {
                    return Err(Error::Usage("metainfo exceeds 16 MiB".into()));
                }
                (Some(bytes), None)
            };
            Request::Add {
                metainfo,
                magnet,
                output_dir: output,
            }
        }
        Operation::Pause { id } => Request::Pause { id },
        Operation::Resume { id } => Request::Resume { id },
        Operation::Remove { id, delete_payload } => Request::Remove { id, delete_payload },
        Operation::Recheck { id } => Request::Recheck { id },
        Operation::Reannounce { id } => Request::Reannounce { id },
        Operation::Get { id } => Request::Get { id },
        Operation::Set { id, values } => {
            let mut parsed = Map::new();
            for value in values {
                let (key, value) = value
                    .split_once('=')
                    .ok_or_else(|| Error::Usage("expected key=JSON_VALUE".into()))?;
                let value = serde_json::from_str(value).map_err(|e| Error::Usage(e.to_string()))?;
                if parsed.insert(key.into(), value).is_some() {
                    return Err(Error::Usage(format!("duplicate key {key}")));
                }
            }
            Request::Set { id, values: parsed }
        }
        Operation::List { .. } => Request::List,
        Operation::Status => Request::Status,
        Operation::Files { id } => Request::Files { id },
        Operation::Health => Request::Health,
        Operation::Shutdown => Request::Shutdown,
    })
}
fn clean(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { '�' } else { c })
        .collect()
}
fn text(data: &Value) {
    if let Some(rows) = data["torrents"].as_array() {
        println!("ID            STATE         VERIFIED / TOTAL       NAME");
        for row in rows {
            let id = row["id"].as_str().unwrap_or("");
            println!(
                "{:<12}  {:<12}  {} / {}  {}",
                &id[..id.len().min(12)],
                row["state"].as_str().unwrap_or("?"),
                row["verified_bytes"],
                row["total_bytes"],
                clean(row["name"].as_str().unwrap_or("magnet"))
            );
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
    } else if let Some(settings) = data.get("settings").and_then(Value::as_object).or_else(|| {
        data.as_object()
            .filter(|m| m.contains_key("download_limit"))
    }) {
        for (key, value) in settings {
            println!("{key}={value}");
        }
    } else {
        println!("{}", serde_json::to_string_pretty(data).unwrap());
    }
}
pub async fn run(args: Args) -> Result<(), Error> {
    let watch = matches!(args.command, Operation::List { watch: true });
    let request = request(args.command)?;
    let socket = args.socket.unwrap_or_else(config::socket);
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
        if watch && !args.json && io::stdout().is_terminal() {
            print!("\x1b[2J\x1b[H");
        }
        if args.json {
            println!("{}", response.data);
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
