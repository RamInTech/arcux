//! `WalStorage` — a durable [`arcux_raft::Storage`] backed by the Phase-1 WAL.
//!
//! The Raft core writes its crash-critical state — `HardState` (term + vote) and the log —
//! through the [`Storage`] trait, and the safety proof requires it to be durable **before**
//! the node acts on it (vote before replying `RequestVote`, entry before replying
//! `AppendEntries`). This impl honours that: every `save_hard_state` / `append` `fsync`s
//! before returning. It is the engine-backed replacement for the in-memory `MemStorage`
//! the deterministic tests use — `node.rs` is unchanged by the swap.
//!
//! - **Log** — appended through [`arcux_engine::wal::WalWriter`] (the same framed,
//!   CRC32C-checked, torn-tail-recoverable format as the engine WAL); replayed on open
//!   with [`arcux_engine::wal::WalReader`]. A record's `seq` is the entry's 1-based index;
//!   its payload is `[term:u64][data]`.
//! - **Hard state** — a tiny file written atomically (temp + `fsync` + rename).
//!
//! Durability failures are treated as fatal (the trait can't surface a `Result`, and the
//! node must not proceed as if a write were durable when it wasn't).

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use arcux_engine::wal::{WalReader, WalWriter};
use arcux_engine::FsyncMode;
use arcux_raft::{Entry, HardState, Storage};

const LOG_FILE: &str = "raft.log";
const HARDSTATE_FILE: &str = "raft.hardstate";
/// Raft durability demands a real flush before the core acts on a write.
const SYNC: FsyncMode = FsyncMode::Fsync;

/// A durable Raft `Storage`: term/vote in an atomically-rewritten file, the log in a
/// framed WAL segment. The in-memory `log`/`hard` mirror the durable state for fast reads.
pub struct WalStorage {
    dir: PathBuf,
    hard: HardState,
    log: Vec<Entry>,
    writer: WalWriter,
}

impl WalStorage {
    /// Open (creating if absent) a durable store under `dir`, recovering term/vote and the
    /// log. A torn tail in the log (an un-acknowledged in-flight append cut short by a
    /// crash) is discarded on replay, losing nothing acknowledged.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<WalStorage> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let log_path = dir.join(LOG_FILE);

        // Replay the log, stopping at the first non-intact record (the torn tail).
        let mut log = Vec::new();
        let mut valid_len = 0u64;
        if log_path.exists() {
            let mut reader = WalReader::open(&log_path).map_err(to_io)?;
            while let Some((seq, payload)) = reader.next_record() {
                match decode_entry(seq, &payload) {
                    Some(e) => log.push(e),
                    None => break, // malformed payload — treat as the tail
                }
            }
            valid_len = reader.valid_len() as u64;
        }
        // Drop any torn tail so future appends land on a clean boundary.
        if log_path.exists() {
            let f = File::options().write(true).open(&log_path)?;
            f.set_len(valid_len)?;
            f.sync_all()?;
        }

        let hard = match read_optional(&dir.join(HARDSTATE_FILE))? {
            Some(bytes) => decode_hard_state(&bytes),
            None => HardState::default(),
        };
        let writer = WalWriter::open_append(&log_path).map_err(to_io)?;
        Ok(WalStorage { dir, hard, log, writer })
    }

    fn hardstate_path(&self) -> PathBuf {
        self.dir.join(HARDSTATE_FILE)
    }

    /// Rewrite the whole log file from the in-memory `log` (used by `truncate_suffix`),
    /// atomically via a temp segment + rename, then reopen the append handle.
    fn rewrite_log(&mut self) {
        let log_path = self.dir.join(LOG_FILE);
        let tmp = self.dir.join("raft.log.tmp");
        let mut w = WalWriter::create(&tmp).expect("create temp raft log");
        for e in &self.log {
            w.append(e.index, &encode_entry(e)).expect("rewrite raft log");
        }
        w.sync(SYNC).expect("sync rewritten raft log");
        std::fs::rename(&tmp, &log_path).expect("rename rewritten raft log");
        self.writer = WalWriter::open_append(&log_path).expect("reopen raft log");
    }
}

impl Storage for WalStorage {
    fn hard_state(&self) -> HardState {
        self.hard
    }

    fn save_hard_state(&mut self, hs: HardState) {
        self.hard = hs;
        atomic_write(&self.hardstate_path(), &encode_hard_state(&hs))
            .expect("persist raft hard state");
    }

    fn last_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    fn term(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log.get((index - 1) as usize).map(|e| e.term)
    }

    fn entries(&self, low: u64, high: u64) -> Vec<Entry> {
        if low == 0 || low > high {
            return Vec::new();
        }
        let lo = (low - 1) as usize;
        let hi = (high as usize).min(self.log.len());
        if lo >= hi {
            return Vec::new();
        }
        self.log[lo..hi].to_vec()
    }

    fn append(&mut self, entries: &[Entry]) {
        for e in entries {
            debug_assert_eq!(
                e.index,
                self.last_index() + 1,
                "Storage::append requires contiguous indices"
            );
            self.writer.append(e.index, &encode_entry(e)).expect("append raft log entry");
            self.log.push(e.clone());
        }
        // fsync once for the whole batch — durable before the core replies.
        self.writer.sync(SYNC).expect("sync raft log");
    }

    fn truncate_suffix(&mut self, from: u64) {
        let keep = if from == 0 { 0 } else { (from - 1) as usize };
        if keep >= self.log.len() {
            return; // nothing to drop
        }
        self.log.truncate(keep);
        self.rewrite_log();
    }
}

// ---- encoding helpers ----------------------------------------------------------------

/// Entry payload in the WAL record: `[term:u64 BE][data...]`. The index is the record seq.
fn encode_entry(e: &Entry) -> Vec<u8> {
    let mut p = Vec::with_capacity(8 + e.data.len());
    p.extend_from_slice(&e.term.to_be_bytes());
    p.extend_from_slice(&e.data);
    p
}

fn decode_entry(index: u64, payload: &[u8]) -> Option<Entry> {
    if payload.len() < 8 {
        return None;
    }
    let term = u64::from_be_bytes(payload[..8].try_into().unwrap());
    Some(Entry { term, index, data: payload[8..].to_vec() })
}

/// Hard state file: `[current_term:u64 BE][has_vote:u8][voted_for:u64 BE]`.
fn encode_hard_state(hs: &HardState) -> [u8; 17] {
    let mut b = [0u8; 17];
    b[..8].copy_from_slice(&hs.current_term.to_be_bytes());
    if let Some(v) = hs.voted_for {
        b[8] = 1;
        b[9..17].copy_from_slice(&v.to_be_bytes());
    }
    b
}

fn decode_hard_state(b: &[u8]) -> HardState {
    if b.len() < 17 {
        return HardState::default();
    }
    let current_term = u64::from_be_bytes(b[..8].try_into().unwrap());
    let voted_for =
        if b[8] == 1 { Some(u64::from_be_bytes(b[9..17].try_into().unwrap())) } else { None };
    HardState { current_term, voted_for }
}

// ---- small fs utilities --------------------------------------------------------------

fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

fn read_optional(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn to_io(e: arcux_engine::Error) -> io::Error {
    io::Error::other(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: u64, index: u64, data: &[u8]) -> Entry {
        Entry { term, index, data: data.to_vec() }
    }

    #[test]
    fn append_read_back_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = WalStorage::open(dir.path()).unwrap();
            s.save_hard_state(HardState { current_term: 7, voted_for: Some(3) });
            s.append(&[entry(7, 1, b"a"), entry(7, 2, b"bb")]);
            s.append(&[entry(8, 3, b"ccc")]);
            assert_eq!(s.last_index(), 3);
            assert_eq!(s.term(2), Some(7));
            assert_eq!(s.term(3), Some(8));
            assert_eq!(s.term(0), Some(0), "the empty-log sentinel");
            assert_eq!(s.entries(1, 3).len(), 3);
        }
        // Reopen — a simulated restart: term, vote and log all recover.
        let s = WalStorage::open(dir.path()).unwrap();
        assert_eq!(s.hard_state(), HardState { current_term: 7, voted_for: Some(3) });
        assert_eq!(s.last_index(), 3);
        assert_eq!(s.entries(1, 3), vec![entry(7, 1, b"a"), entry(7, 2, b"bb"), entry(8, 3, b"ccc")]);
    }

    #[test]
    fn truncate_suffix_drops_tail_durably() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = WalStorage::open(dir.path()).unwrap();
            s.append(&[entry(1, 1, b"a"), entry(1, 2, b"b"), entry(2, 3, b"c"), entry(2, 4, b"d")]);
            s.truncate_suffix(3); // drop indices >= 3
            assert_eq!(s.last_index(), 2);
            // Re-append after truncation, mimicking a follower resolving a conflict.
            s.append(&[entry(5, 3, b"C")]);
            assert_eq!(s.term(3), Some(5));
        }
        let s = WalStorage::open(dir.path()).unwrap();
        assert_eq!(s.last_index(), 3);
        assert_eq!(s.entries(1, 3), vec![entry(1, 1, b"a"), entry(1, 2, b"b"), entry(5, 3, b"C")]);
    }

    #[test]
    fn torn_tail_is_discarded_on_replay() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = WalStorage::open(dir.path()).unwrap();
            s.append(&[entry(1, 1, b"a"), entry(1, 2, b"b"), entry(1, 3, b"c")]);
        }
        // Simulate a crash mid-append: garbage bytes appended after the last good record.
        {
            use std::io::Write;
            let mut f = File::options().append(true).open(dir.path().join(LOG_FILE)).unwrap();
            f.write_all(&[0xFF, 0xAB, 0x00, 0x01, 0x02]).unwrap();
        }
        // Replay discards the torn tail; only the three acknowledged records survive.
        let s = WalStorage::open(dir.path()).unwrap();
        assert_eq!(s.last_index(), 3);
        assert_eq!(s.entries(1, 3).len(), 3);
    }
}
