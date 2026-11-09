//! Offline inspection of untrusted metainfo and magnet links.
use serde_json::{Value, json};
use spill::protocol::{
    bencode::{Limits, Node, Value as Bencode},
    identity::TorrentIdentity,
    magnet::{self, Magnet},
    metainfo::{FileKind, Metainfo},
};
use std::io::{self, Read};

#[derive(clap::Args)]
pub struct Arguments {
    /// Local .torrent path, magnet link, or - to read metainfo from stdin.
    source: String,
}

pub fn run(args: &Arguments, json: bool) -> io::Result<()> {
    let data = if args
        .source
        .get(..8)
        .is_some_and(|s| s.eq_ignore_ascii_case("magnet:?"))
    {
        let magnet = Magnet::parse(&args.source, magnet::Limits::default())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let mut data = identity(magnet.info_hash());
        data["kind"] = json!("magnet");
        data["name"] = magnet.display_name().map(String::from_utf8_lossy).into();
        data["parameters"] = magnet
            .parameters()
            .iter()
            .map(|p| {
                json!({
                    "key": String::from_utf8_lossy(p.key()),
                    "value": String::from_utf8_lossy(p.value()),
                })
            })
            .collect();
        data
    } else {
        let input: Box<dyn Read> = if args.source == "-" {
            Box::new(io::stdin())
        } else {
            Box::new(std::fs::File::open(&args.source)?)
        };
        let limits = Limits::default();
        let mut bytes = Vec::new();
        input
            .take(limits.max_input_len as u64 + 1)
            .read_to_end(&mut bytes)?;
        let meta = Metainfo::parse(&bytes, limits)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let layout = meta.layout();
        let mut data = identity(meta.info_hash());
        data["kind"] = json!("metainfo");
        data["name"] = json!(String::from_utf8_lossy(meta.name()));
        data["private"] = json!(meta.is_private());
        data["multi_file"] = json!(layout.is_multi_file());
        data["total_bytes"] = json!(layout.total_length());
        data["data_bytes"] = json!(
            layout
                .files()
                .iter()
                .filter(|f| f.kind() == FileKind::Data)
                .map(|f| f.length())
                .sum::<u64>()
        );
        data["piece_length"] = json!(layout.piece_length());
        data["piece_count"] = json!(layout.piece_count());
        data["layers_pending"] = json!(meta.layers_pending());
        data["files"] = layout.files().iter().enumerate().map(|(index, file)| json!({
            "index": index,
            "path": file.path().iter().map(|p| String::from_utf8_lossy(p)).collect::<Vec<_>>().join("/"),
            "length": file.length(),
            "offset": file.offset(),
            "padding": file.kind() == FileKind::Padding,
        })).collect();
        for (key, field) in [
            ("announce", "announce"),
            ("announce-list", "announce_list"),
            ("url-list", "url_list"),
            ("httpseeds", "httpseeds"),
            ("comment", "comment"),
            ("created by", "created_by"),
            ("creation date", "creation_date"),
        ] {
            if let Some(node) = meta.document().get(key.as_bytes()) {
                data[field] = value(node);
            }
        }
        if let Some(node) = meta
            .document()
            .get(b"info")
            .expect("validated info")
            .get(b"source")
        {
            data["source"] = value(node);
        }
        data
    };
    // JSON escaping also keeps untrusted names and URLs safe on terminals.
    println!(
        "{}",
        if json {
            serde_json::to_string(&data)
        } else {
            serde_json::to_string_pretty(&data)
        }
        .map_err(io::Error::other)?
    );
    Ok(())
}

fn identity(hash: TorrentIdentity) -> Value {
    json!({
        "info_hash": hash.to_string(),
        "format": match hash {
            TorrentIdentity::V1(_) => "v1",
            TorrentIdentity::V2(_) => "v2",
            TorrentIdentity::Hybrid(..) => "hybrid",
        },
        "v1": hash.v1().map(|h| h.to_string()),
        "v2": hash.v2().map(|h| h.to_string()),
    })
}

fn value(node: &Node<'_>) -> Value {
    match node.value() {
        Bencode::Bytes(bytes) => json!(String::from_utf8_lossy(bytes)),
        Bencode::Integer(n) => json!(n),
        Bencode::List(nodes) => nodes.iter().map(value).collect(),
        Bencode::Dictionary(entries) => Value::Object(
            entries
                .iter()
                .map(|(key, node)| (String::from_utf8_lossy(key).into_owned(), value(node)))
                .collect(),
        ),
    }
}
