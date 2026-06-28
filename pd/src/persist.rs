//! Crash-atomic small-file persistence, shared by the TSO watermark and the region
//! table. Mirrors the engine manifest's discipline: write a `.tmp`, fsync it, then
//! `rename` over the target. Because `rename` is atomic, a reader (or a recovery after
//! a crash) sees either the old image or the new one — never a torn write.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;

/// Atomically replace `path`'s contents with `bytes`.
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    // Best-effort directory fsync so the rename itself is durable.
    if let Some(dir) = path.parent() {
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

/// Read a whole file, returning `None` if it does not exist yet.
pub(crate) fn read_optional(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match File::open(path) {
        Ok(mut f) => {
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            Ok(Some(buf))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

// --- minimal length-prefixed byte-string codec (used by the region table) ---

/// Append `bytes` as a u32-BE length followed by the raw bytes.
pub(crate) fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Read a length-prefixed byte-string at `*pos`, advancing it. `None` if truncated.
pub(crate) fn get_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let len = get_u32(buf, pos)? as usize;
    let end = pos.checked_add(len)?;
    let slice = buf.get(*pos..end)?;
    *pos = end;
    Some(slice)
}

pub(crate) fn get_u32(buf: &[u8], pos: &mut usize) -> Option<u32> {
    let end = pos.checked_add(4)?;
    let v = u32::from_be_bytes(buf.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(v)
}

pub(crate) fn get_u64(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let end = pos.checked_add(8)?;
    let v = u64::from_be_bytes(buf.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(v)
}