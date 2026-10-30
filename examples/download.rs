//! cargo run --no-default-features --example download -- FILE.torrent DIRECTORY
//! cargo run --no-default-features --example download -- --magnet URI DIRECTORY
//! Keeps no recovery record: after an interruption, rerunning refuses to
//! overwrite the partial files. See the progress example for resumable downloads.
use spill::{
    engine::{
        download,
        profile::Profile,
        torrent::{Destination, TorrentSource},
    },
    protocol::magnet::Magnet,
};
use std::{io, path::PathBuf};

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let (source, destination) = match args.as_slice() {
        [file, destination] if file != "--magnet" => {
            let bytes = tokio::fs::read(file).await?;
            (
                TorrentSource::Metainfo(Profile::default().parse_owned(&bytes)?),
                destination,
            )
        }
        [flag, uri, destination] if flag == "--magnet" => {
            let uri = uri.to_str().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "magnet must be UTF-8")
            })?;
            let magnet = Magnet::parse(uri, Default::default())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            (TorrentSource::Magnet(magnet), destination)
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: download FILE.torrent DIRECTORY | download --magnet URI DIRECTORY",
            ));
        }
    };
    download(source, Destination::Open(PathBuf::from(destination))).await
}
