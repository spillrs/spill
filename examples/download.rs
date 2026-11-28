//! cargo run --no-default-features --example download -- FILE.torrent DIRECTORY
//! cargo run --no-default-features --example download -- --magnet URI DIRECTORY
//! Keeps no recovery record: after an interruption, rerunning refuses to
//! overwrite the partial files. See the progress example for resumable downloads.
use spill::engine::{Destination, TorrentSource, download};
use std::{io, path::PathBuf};

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let (source, destination) = match args.as_slice() {
        [file, destination] if file != "--magnet" => {
            let bytes = tokio::fs::read(file).await?;
            (TorrentSource::from_metainfo_bytes(&bytes)?, destination)
        }
        [flag, uri, destination] if flag == "--magnet" => {
            let uri = uri.to_str().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "magnet must be UTF-8")
            })?;
            (TorrentSource::from_magnet(uri)?, destination)
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: download FILE.torrent DIRECTORY | download --magnet URI DIRECTORY",
            ));
        }
    };
    tokio::fs::create_dir_all(destination).await?;
    download(source, Destination::Open(PathBuf::from(destination))).await
}
