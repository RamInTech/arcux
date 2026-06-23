//! Write-ahead log: durable record format + replay.
//!
//! This module is pure I/O and framing — it knows nothing about memtables or
//! threading. The group-commit *committer* that batches concurrent writers and
//! issues one `fsync` per group lives in the engine core ([`crate::db`]); it
//! drives a [`WalWriter`] and, on startup, a [`WalReader`] per segment.
//!
//! ## Record frame
//!
//! ```text
//! [len:u32][crc32c:u32][seq:u64][payload...]
//!  \_______________/    \__________________/
//!     header (8B)         body (len bytes), crc covers the body
//! ```
//!
//! `payload` is an opaque batch body (see [`crate::batch::WriteBatch::encode`]).
//! `len = 8 + payload.len()`. A record is *intact* iff its full body is present
//! and the CRC matches; the first non-intact record marks the **torn tail** and
//! ends replay. Because a write is acknowledged only after its group's `fsync`,
//! a torn tail can only be an un-acknowledged in-flight write, so discarding it
//! loses nothing acknowledged.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::options::FsyncMode;

/// `len(4) + crc(4)`.
const HEADER_LEN: usize = 8;
/// `seq(8)`.
const SEQ_LEN: usize = 8;

/// An append handle to a single WAL segment file.
pub struct WalWriter {
    file: File,
    path: PathBuf,
    written: usize,
}

impl WalWriter {
    /// Create (or truncate) a fresh segment at `path`.
    pub fn create(path: impl Into<PathBuf>) -> Result<WalWriter> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        Ok(WalWriter { file, path, written: 0 })
    }

    /// Append one framed record carrying `seq` and `payload`.
    pub fn append(&mut self, seq: u64, payload: &[u8]) -> Result<()> {
        let body_len = SEQ_LEN + payload.len();
        let mut frame = Vec::with_capacity(HEADER_LEN + body_len);
        frame.extend_from_slice(&(body_len as u32).to_be_bytes());
        let crc_pos = frame.len();
        frame.extend_from_slice(&[0u8; 4]); // crc placeholder
        let body_start = frame.len();
        frame.extend_from_slice(&seq.to_be_bytes());
        frame.extend_from_slice(payload);
        let crc = crc32c::crc32c(&frame[body_start..]);
        frame[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
        self.file.write_all(&frame)?;
        self.written += frame.len();
        Ok(())
    }

    /// Flush to stable storage per the configured [`FsyncMode`]. This is the
    /// durability point: callers must only acknowledge a write after this returns.
    pub fn sync(&self, mode: FsyncMode) -> Result<()> {
        match mode {
            FsyncMode::None => Ok(()),
            FsyncMode::Fsync => Ok(self.file.sync_all()?),
            FsyncMode::FullFsync => full_fsync(&self.file),
        }
    }

    /// Bytes written to this segment so far (drives rotation).
    pub fn size(&self) -> usize {
        self.written
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Reads a WAL segment, yielding intact records and stopping at the torn tail.
pub struct WalReader {
    data: Vec<u8>,
    pos: usize,
}

impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> Result<WalReader> {
        let data = std::fs::read(path.as_ref())?;
        Ok(WalReader { data, pos: 0 })
    }

    /// Byte offset of the first non-intact record (the point a fresh writer would
    /// truncate to). Equals file length when the whole segment is intact.
    pub fn valid_len(&self) -> usize {
        self.pos
    }

    /// Next intact `(seq, payload)`, or `None` at clean EOF or a torn tail.
    pub fn next_record(&mut self) -> Option<(u64, Vec<u8>)> {
        let buf = &self.data;
        let start = self.pos;
        if start + HEADER_LEN > buf.len() {
            return None; // truncated header
        }
        let len = u32::from_be_bytes(buf[start..start + 4].try_into().unwrap()) as usize;
        let crc = u32::from_be_bytes(buf[start + 4..start + 8].try_into().unwrap());
        let body_start = start + HEADER_LEN;
        let body_end = body_start.checked_add(len)?;
        if body_end > buf.len() {
            return None; // truncated body
        }
        let body = &buf[body_start..body_end];
        if crc32c::crc32c(body) != crc {
            return None; // corrupt → torn tail
        }
        if body.len() < SEQ_LEN {
            return None;
        }
        let seq = u64::from_be_bytes(body[0..SEQ_LEN].try_into().unwrap());
        let payload = body[SEQ_LEN..].to_vec();
        self.pos = body_end;
        Some((seq, payload))
    }
}

#[cfg(target_os = "macos")]
fn full_fsync(file: &File) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    // F_FULLFSYNC asks the drive to flush its write cache (true power-loss barrier).
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
    if ret == -1 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn full_fsync(file: &File) -> Result<()> {
    Ok(file.sync_all()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::FsyncMode;

    fn tmp() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000.wal");
        (dir, path)
    }

    #[test]
    fn record_roundtrip() {
        let (_dir, path) = tmp();
        {
            let mut w = WalWriter::create(&path).unwrap();
            w.append(1, b"alpha").unwrap();
            w.append(2, b"").unwrap();
            w.append(3, b"gamma-payload").unwrap();
            w.sync(FsyncMode::None).unwrap();
        }
        let mut r = WalReader::open(&path).unwrap();
        assert_eq!(r.next_record(), Some((1, b"alpha".to_vec())));
        assert_eq!(r.next_record(), Some((2, b"".to_vec())));
        assert_eq!(r.next_record(), Some((3, b"gamma-payload".to_vec())));
        assert_eq!(r.next_record(), None);
        assert_eq!(r.valid_len(), std::fs::metadata(&path).unwrap().len() as usize);
    }
}
