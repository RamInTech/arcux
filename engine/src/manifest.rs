//! Minimal manifest: the durable record of which SSTables are live and how far
//! the WAL has been flushed.
//!
//! Phase-1 form: a single `MANIFEST` file rewritten atomically (write `MANIFEST.tmp`,
//! fsync, `rename`) on every flush. Because `rename` is atomic, `MANIFEST` is always
//! either the old or the new image — never torn. Recovery reads it to learn which
//! SSTables to open and the `last_flushed_seq` watermark below which WAL records are
//! already durable in SSTables and can be skipped.
//!
//! Deferred to Phase 1b: the LevelDB-style `CURRENT` pointer + append-only
//! version-edit log + multi-level version-set + obsolete-file GC.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

const MANIFEST_FILE: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";
const MANIFEST_MAGIC: u64 = 0x4d414e_49463031; // "MANIF01"

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// Highest WAL seq whose data is fully captured in some live SSTable.
    pub last_flushed_seq: u64,
    /// Live SSTable file numbers, oldest first.
    pub sstables: Vec<u64>,
}

impl Manifest {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join(MANIFEST_FILE)
    }

    /// Load the manifest, or a default (empty) one if none exists yet.
    pub fn load(dir: &Path) -> Result<Manifest> {
        match std::fs::read(Self::path(dir)) {
            Ok(data) => Self::decode(&data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::default()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// Atomically replace the on-disk manifest.
    pub fn store(&self, dir: &Path) -> Result<()> {
        use std::io::Write;
        let bytes = self.encode();
        let tmp = dir.join(MANIFEST_TMP);
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, Self::path(dir))?;
        // Best-effort: fsync the directory so the rename itself is durable.
        if let Ok(dirf) = std::fs::File::open(dir) {
            let _ = dirf.sync_all();
        }
        Ok(())
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&MANIFEST_MAGIC.to_be_bytes());
        buf.extend_from_slice(&self.last_flushed_seq.to_be_bytes());
        buf.extend_from_slice(&(self.sstables.len() as u32).to_be_bytes());
        for n in &self.sstables {
            buf.extend_from_slice(&n.to_be_bytes());
        }
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf
    }

    fn decode(data: &[u8]) -> Result<Manifest> {
        if data.len() < 8 + 8 + 4 + 4 {
            return Err(Error::corruption("manifest too small"));
        }
        let (body, crc_b) = data.split_at(data.len() - 4);
        if crc32c::crc32c(body) != u32::from_be_bytes(crc_b.try_into().unwrap()) {
            return Err(Error::corruption("manifest crc mismatch"));
        }
        if u64::from_be_bytes(body[0..8].try_into().unwrap()) != MANIFEST_MAGIC {
            return Err(Error::corruption("manifest bad magic"));
        }
        let last_flushed_seq = u64::from_be_bytes(body[8..16].try_into().unwrap());
        let count = u32::from_be_bytes(body[16..20].try_into().unwrap()) as usize;
        let mut sstables = Vec::with_capacity(count);
        let mut pos = 20;
        for _ in 0..count {
            if pos + 8 > body.len() {
                return Err(Error::corruption("manifest truncated"));
            }
            sstables.push(u64::from_be_bytes(body[pos..pos + 8].try_into().unwrap()));
            pos += 8;
        }
        Ok(Manifest { last_flushed_seq, sstables })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), Manifest::default());

        let m = Manifest { last_flushed_seq: 42, sstables: vec![1, 2, 7] };
        m.store(dir.path()).unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), m);

        // Overwrite atomically with a newer image.
        let m2 = Manifest { last_flushed_seq: 99, sstables: vec![1, 2, 7, 9] };
        m2.store(dir.path()).unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), m2);
    }

    #[test]
    fn detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let m = Manifest { last_flushed_seq: 5, sstables: vec![3] };
        m.store(dir.path()).unwrap();
        let mut bytes = std::fs::read(Manifest::path(dir.path())).unwrap();
        let n = bytes.len();
        bytes[n - 5] ^= 0xFF; // flip a byte in the body
        std::fs::write(Manifest::path(dir.path()), &bytes).unwrap();
        assert!(Manifest::load(dir.path()).is_err());
    }
}
