use std::{env, fs, path::PathBuf};

fn main() {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("src/daemon/web");
    println!("cargo:rerun-if-changed={}", root.display());
    let mut files: Vec<_> = fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();
    let mut assets = String::from("&[\n");
    for path in files {
        let name = path.file_name().unwrap().to_str().unwrap();
        assets.push_str(&format!("({name:?}, include_bytes!({path:?}) as &[u8]),\n"));
    }
    assets.push(']');
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("web_assets.rs"),
        assets,
    )
    .unwrap();
}
