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
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("magnet:"))
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
    if json {
        println!(
            "{}",
            serde_json::to_string(&data).map_err(io::Error::other)?
        );
    } else {
        print_summary(&data);
    }
    Ok(())
}

fn clean(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { '\u{FFFD}' } else { c })
        .collect()
}

fn print_summary(data: &Value) {
    let kind = data["kind"].as_str().unwrap_or("");
    if let Some(name) = data["name"].as_str() {
        println!("{:<16} {}", "Name:", clean(name));
    }
    println!(
        "{:<16} {}",
        "Info hash:",
        data["info_hash"].as_str().unwrap_or("")
    );
    println!(
        "{:<16} {}",
        "Format:",
        data["format"].as_str().unwrap_or("")
    );
    if let Some(v1) = data["v1"].as_str() {
        println!("{:<16} {v1}", "SHA-1:");
    }
    if let Some(v2) = data["v2"].as_str() {
        println!("{:<16} {v2}", "SHA-256:");
    }
    if kind == "magnet" {
        let params = data["parameters"].as_array().map_or(0, Vec::len);
        println!("{:<16} {params}", "Parameters:");
        if let Some(list) = data["parameters"].as_array() {
            for p in list {
                let key = p["key"].as_str().unwrap_or("");
                if matches!(key, "tr" | "ws" | "so") {
                    println!("  {key}={}", clean(p["value"].as_str().unwrap_or("")));
                }
            }
        }
        return;
    }
    println!(
        "{:<16} {}",
        "Private:",
        if data["private"] == true { "yes" } else { "no" }
    );
    let data_bytes = data["data_bytes"].as_u64().unwrap_or(0);
    let total_bytes = data["total_bytes"].as_u64().unwrap_or(0);
    if data_bytes != total_bytes {
        println!(
            "{:<16} {} ({} with padding)",
            "Size:",
            crate::cli::human_bytes(data_bytes),
            crate::cli::human_bytes(total_bytes)
        );
    } else {
        println!("{:<16} {}", "Size:", crate::cli::human_bytes(data_bytes));
    }
    println!(
        "{:<16} {} × {}",
        "Pieces:",
        data["piece_count"],
        crate::cli::human_bytes(data["piece_length"].as_u64().unwrap_or(0))
    );
    if let Some(files) = data["files"].as_array() {
        let data_files = files.iter().filter(|f| f["padding"] != true).count();
        let padding_files = files.len() - data_files;
        if padding_files > 0 {
            println!("{:<16} {data_files} ({padding_files} padding)", "Files:");
        } else {
            println!("{:<16} {data_files}", "Files:");
        }
        for file in files.iter().take(20) {
            let tag = if file["padding"] == true {
                " [padding]"
            } else {
                ""
            };
            println!(
                "  {:>4}  {:>10}  {}{tag}",
                file["index"],
                crate::cli::human_bytes(file["length"].as_u64().unwrap_or(0)),
                clean(file["path"].as_str().unwrap_or(""))
            );
        }
        if files.len() > 20 {
            println!("  … {} more (use --json for full list)", files.len() - 20);
        }
    }
    if let Some(announce) = data["announce"].as_str() {
        println!("{:<16} {}", "Tracker:", clean(announce));
    }
    if let Some(comment) = data["comment"].as_str() {
        println!("{:<16} {}", "Comment:", clean(comment));
    }
    if let Some(source) = data["source"].as_str() {
        println!("{:<16} {}", "Source:", clean(source));
    }
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
