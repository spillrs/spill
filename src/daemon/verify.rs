//! Offline verification delegates payload handling to the engine.
use spill::engine::{
    profile::{MAX_META, Profile},
    provisioning::NamedDestination,
};
use std::{
    io::{self, Read},
    path::PathBuf,
};

#[derive(clap::Args)]
pub struct Arguments {
    /// Local .torrent file describing the payload.
    torrent: PathBuf,
    /// Parent download directory, containing NAME or NAME/ as in spill add.
    #[arg(long)]
    path: PathBuf,
}

pub fn run(args: &Arguments, json: bool) -> io::Result<()> {
    let mut bytes = Vec::new();
    std::fs::File::open(&args.torrent)?
        .take(MAX_META as u64 + 1)
        .read_to_end(&mut bytes)?;
    let meta = Profile::default()
        .parse_owned(&bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    NamedDestination::verify(&args.path, &meta).map_err(io::Error::other)?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "info_hash": meta.info_hash().to_string(),
                "verified": true,
            })
        );
    } else {
        println!("Verified {}", meta.info_hash());
    }
    Ok(())
}
