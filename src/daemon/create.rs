//! Offline torrent creation; never connects to the daemon.
use clap::{Args, builder::TypedValueParser};
use spill::{
    engine::create::{self, Format, Options},
    protocol::identity::TorrentIdentity,
};
use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
};

#[derive(Args)]
pub struct Arguments {
    /// File or directory to hash.
    target: PathBuf,
    /// Torrent format; v1 matches mktorrent, v2 and hybrid follow BEP 52.
    #[arg(short = 'F', long, default_value = "v1", value_parser = clap::builder::PossibleValuesParser::new(["v1", "v2", "hybrid"]).map(|value| match value.as_str() {
        "v1" => Format::V1,
        "v2" => Format::V2,
        "hybrid" => Format::Hybrid,
        _ => unreachable!("validated format"),
    }))]
    format: Format,
    /// Tracker tier; comma-separated URLs are alternatives, repeated -a adds backups.
    #[arg(short = 'a', long = "announce", value_name = "URL[,URL]", action = clap::ArgAction::Append)]
    announce: Vec<String>,
    /// Comment stored outside the info hash.
    #[arg(short, long)]
    comment: Option<String>,
    /// Omit the creation date for reproducible output.
    #[arg(short = 'd', long = "no-date")]
    no_date: bool,
    /// Piece size is 2^N bytes (15..28); default 18 is 256 KiB.
    #[arg(short = 'l', long = "piece-length", default_value_t = 18, value_parser = clap::value_parser!(u8).range(15..=28), value_name = "N")]
    piece_length: u8,
    /// Torrent root name; defaults to the target basename.
    #[arg(short, long)]
    name: Option<OsString>,
    /// Output path; defaults to NAME.torrent in the current directory. Never overwritten.
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Set the private flag (disables public discovery when used by spill).
    #[arg(short, long)]
    private: bool,
    /// Source string stored inside the info hash.
    #[arg(short, long)]
    source: Option<String>,
    /// Hashing workers; defaults to the available CPU count.
    #[arg(short = 't', long, value_parser = clap::value_parser!(usize))]
    threads: Option<usize>,
    /// Print piece progress even when stderr is not a terminal.
    #[arg(short, long)]
    verbose: bool,
    /// Comma-separated HTTP(S) web seed URLs; may be repeated.
    #[arg(short = 'w', long = "web-seed", value_delimiter = ',')]
    web_seeds: Vec<String>,
}

pub fn run(args: &Arguments, json: bool) -> io::Result<()> {
    // Refuse obvious output collisions before spending time hashing.
    if let Some(output) = &args.output {
        vacant(output)?;
    }
    let defaults = Options::default();
    let options = Options {
        format: args.format,
        piece_length: 1 << args.piece_length,
        threads: args.threads.unwrap_or(defaults.threads),
        name: args.name.clone(),
        announce: args
            .announce
            .iter()
            .map(|tier| tier.split(',').map(str::to_owned).collect())
            .collect(),
        web_seeds: args.web_seeds.clone(),
        comment: args.comment.clone(),
        source: args.source.clone(),
        private: args.private,
        creation_date: if args.no_date {
            None
        } else {
            defaults.creation_date
        },
    };
    let progress = args.verbose || io::stderr().is_terminal();
    let torrent = create::create(&args.target, &options, &mut |done, total| {
        if progress {
            eprint!("\rHashed {done} of {total} pieces.");
        }
        Ok(())
    });
    if progress {
        eprintln!();
    }
    let torrent = torrent?;
    let output = args.output.clone().unwrap_or_else(|| {
        let mut name = torrent.name.clone();
        name.push(".torrent");
        PathBuf::from(name)
    });
    publish(&output, &torrent.metainfo)?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "output": output.to_string_lossy(),
                "info_hash": torrent.info_hash.to_string(),
                "format": match torrent.info_hash {
                    TorrentIdentity::V1(_) => "v1",
                    TorrentIdentity::V2(_) => "v2",
                    TorrentIdentity::Hybrid(..) => "hybrid",
                },
                "v1": torrent.info_hash.v1().map(|hash| hash.to_string()),
                "v2": torrent.info_hash.v2().map(|hash| hash.to_string()),
                "total_bytes": torrent.total_length,
                "piece_count": torrent.piece_count,
            })
        );
    } else {
        println!("Created {:?} ({})", output.as_os_str(), torrent.info_hash);
    }
    Ok(())
}

fn vacant(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("output already exists: {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn publish(output: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut random = [0; 8];
    getrandom::fill(&mut random).map_err(io::Error::other)?;
    let temporary = output.with_file_name(format!(
        ".spill-create-{:016x}.tmp",
        u64::from_ne_bytes(random)
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    // A hard link publishes the completed document atomically without replacing
    // an output created by someone else while hashing.
    let result = file
        .write_all(bytes)
        .and_then(|()| fs::hard_link(&temporary, output));
    drop(file);
    let cleanup = fs::remove_file(&temporary);
    result.and(cleanup)
}
