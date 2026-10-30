use sha1::{Digest, Sha1};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Temp(pub PathBuf);
impl Temp {
    pub fn new() -> Self {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "spill-e2e-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn seed_metainfo(name: &str, payload: &[u8], tracker: Option<&str>) -> Vec<u8> {
    let mut bytes = b"d".to_vec();
    if let Some(url) = tracker {
        bytes.extend_from_slice(format!("8:announce{}:{url}", url.len()).as_bytes());
    }
    bytes.extend_from_slice(
        format!(
            "4:infod6:lengthi{}e4:name{}:{name}12:piece lengthi{}e6:pieces20:",
            payload.len(),
            name.len(),
            payload.len()
        )
        .as_bytes(),
    );
    bytes.extend_from_slice(&Sha1::digest(payload));
    bytes.extend_from_slice(b"ee");
    bytes
}
