//! Bounded, cancellable verification of existing payloads for read-only adoption.
use super::*;
use std::io::{Read, Seek, SeekFrom};

pub(super) fn verify_content_cancellable(
    meta: &OwnedMetainfo,
    files: &mut [Option<File>],
    canceled: &dyn Fn() -> bool,
) -> io::Result<()> {
    use sha1::{Digest, Sha1};
    let mut bytes = vec![0; 64 * 1024];
    for piece in 0..meta.layout().piece_count() {
        check_canceled(canceled)?;
        let range = meta.layout().piece_range(piece).expect("piece range");
        let mut hash = Sha1::new();
        let mut offset = 0;
        for (entry, file) in meta.layout().files().iter().zip(&mut *files) {
            let a = range.start.max(offset);
            let b = range.end.min(offset + entry.length());
            if a < b {
                if let Some(file) = file.as_mut() {
                    file.seek(SeekFrom::Start(a - offset))?;
                }
                let mut remaining = b - a;
                while remaining != 0 {
                    check_canceled(canceled)?;
                    let count = remaining.min(bytes.len() as u64) as usize;
                    let chunk = &mut bytes[..count];
                    if let Some(file) = file.as_mut() {
                        file.read_exact(chunk)?;
                    } else {
                        chunk.fill(0);
                    }
                    hash.update(chunk);
                    remaining -= count as u64;
                }
            }
            offset += entry.length();
        }
        check_canceled(canceled)?;
        if hash.finalize()[..] != meta.piece_hash(piece).expect("piece hash")[..] {
            return Err(invalid(
                "existing payload contents do not match this torrent; choose another output directory",
            ));
        }
    }
    Ok(())
}

pub(super) fn check_canceled(canceled: &dyn Fn() -> bool) -> io::Result<()> {
    if canceled() {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "payload verification stopped",
        ))
    } else {
        Ok(())
    }
}
