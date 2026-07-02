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
use arcux_raft::{Entry, EntryType, HardState, Snapshot, Storage};

const LOG_FILE: &str = "raft.log";
const HARDSTATE_FILE: &str = "raft.hardstate";
const SNAPSHOT_FILE: &str = "raft.snapshot";
/// Raft durability demands a real flush before the core acts on a write.
const SYNC: FsyncMode = FsyncMode::Fsync;

/// A durable Raft `Storage`: term/vote in an atomically-rewritten file, the log in a
/// framed WAL segment, and — once the log is **compacted** (Phase 4b++) — a snapshot file
/// holding the committed state below the log's start. The in-memory `log`/`hard`/`snap`
/// mirror the durable state for fast reads. Log indices are absolute: after compaction the
/// WAL record `seq` still equals the entry's index, but the segment only holds entries
/// **above** `snap.last_included_index`.
pub struct WalStorage {
    dir: PathBuf,
    hard: HardState,
    log: Vec<Entry>,
    snap: Option<Snapshot>,
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

        // Load the snapshot first: it fixes the log's start (`base`). A crash between
        // writing the snapshot and rewriting the log can leave superseded records in the
        // segment — anything at or below `base` is filtered on replay.
        let snap = read_optional(&dir.join(SNAPSHOT_FILE))?.and_then(|b| decode_snapshot(&b));
        let base = snap.as_ref().map(|s| s.last_included_index).unwrap_or(0);

        // Replay the log, stopping at the first non-intact record (the torn tail).
        let mut log = Vec::new();
        let mut valid_len = 0u64;
        if log_path.exists() {
            let mut reader = WalReader::open(&log_path).map_err(to_io)?;
            while let Some((seq, payload)) = reader.next_record() {
                match decode_entry(seq, &payload) {
                    Some(e) if e.index > base => log.push(e),
                    Some(_) => {} // superseded by the snapshot — drop it
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
        Ok(WalStorage { dir, hard, log, snap, writer })
    }

    /// The snapshot's `last_included_index` (0 when uncompacted) — the log's index offset.
    fn base(&self) -> u64 {
        self.snap.as_ref().map(|s| s.last_included_index).unwrap_or(0)
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join(SNAPSHOT_FILE)
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
        self.log.last().map(|e| e.index).unwrap_or_else(|| self.base())
    }

    fn term(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        let base = self.base();
        if index == base {
            return self.snap.as_ref().map(|s| s.last_included_term);
        }
        if index < base {
            return None; // compacted away
        }
        self.log.get((index - base - 1) as usize).map(|e| e.term)
    }

    fn entries(&self, low: u64, high: u64) -> Vec<Entry> {
        let base = self.base();
        let low = low.max(base + 1);
        if low > high {
            return Vec::new();
        }
        let lo = (low - base - 1) as usize;
        let hi = ((high - base) as usize).min(self.log.len());
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
        let base = self.base();
        let keep = if from <= base + 1 { 0 } else { ((from - base - 1) as usize).min(self.log.len()) };
        if keep >= self.log.len() {
            return; // nothing to drop
        }
        self.log.truncate(keep);
        self.rewrite_log();
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.snap.clone()
    }

    fn first_index(&self) -> u64 {
        self.base() + 1
    }

    fn compact(&mut self, index: u64, term: u64, conf_state: Vec<u64>, data: Vec<u8>) {
        if index <= self.base() {
            return; // already compacted at or past this point
        }
        let base = self.base();
        let drop = ((index - base) as usize).min(self.log.len());
        self.log.drain(0..drop);
        // Persist the snapshot first, then rewrite the (shortened) log. A crash in
        // between leaves superseded records that `open` filters against `base`.
        self.snap = Some(Snapshot {
            last_included_index: index,
            last_included_term: term,
            conf_state,
            data,
        });
        atomic_write(&self.snapshot_path(), &encode_snapshot(self.snap.as_ref().unwrap()))
            .expect("persist raft snapshot");
        self.rewrite_log();
    }

    fn apply_snapshot(&mut self, snap: Snapshot) {
        self.log.clear();
        self.snap = Some(snap);
        atomic_write(&self.snapshot_path(), &encode_snapshot(self.snap.as_ref().unwrap()))
            .expect("persist raft snapshot");
        self.rewrite_log();
    }
}

// ---- encoding helpers ----------------------------------------------------------------

/// Entry payload in the WAL record: `[term:u64 BE][entry_type:u8][data...]`. The index is the
/// record seq. `entry_type` 0 = normal, 1 = config change (Phase 4b++ rest).
fn encode_entry(e: &Entry) -> Vec<u8> {
    let mut p = Vec::with_capacity(9 + e.data.len());
    p.extend_from_slice(&e.term.to_be_bytes());
    p.push(match e.entry_type {
        EntryType::Normal => 0,
        EntryType::ConfChange => 1,
    });
    p.extend_from_slice(&e.data);
    p
}

fn decode_entry(index: u64, payload: &[u8]) -> Option<Entry> {
    if payload.len() < 9 {
        return None;
    }
    let term = u64::from_be_bytes(payload[..8].try_into().unwrap());
    let entry_type = if payload[8] == 1 { EntryType::ConfChange } else { EntryType::Normal };
    Some(Entry { term, index, entry_type, data: payload[9..].to_vec() })
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

/// Snapshot file: `[last_included_index:u64 BE][last_included_term:u64 BE][n:u32 BE]
/// [conf_voter:u64 BE * n][data...]`, where `conf_state` is the group membership (Phase 4b++
/// rest).
fn encode_snapshot(s: &Snapshot) -> Vec<u8> {
    let mut b = Vec::with_capacity(20 + s.conf_state.len() * 8 + s.data.len());
    b.extend_from_slice(&s.last_included_index.to_be_bytes());
    b.extend_from_slice(&s.last_included_term.to_be_bytes());
    b.extend_from_slice(&(s.conf_state.len() as u32).to_be_bytes());
    for v in &s.conf_state {
        b.extend_from_slice(&v.to_be_bytes());
    }
    b.extend_from_slice(&s.data);
    b
}

fn decode_snapshot(b: &[u8]) -> Option<Snapshot> {
    if b.len() < 20 {
        return None;
    }
    let last_included_index = u64::from_be_bytes(b[..8].try_into().unwrap());
    let last_included_term = u64::from_be_bytes(b[8..16].try_into().unwrap());
    let n = u32::from_be_bytes(b[16..20].try_into().unwrap()) as usize;
    let mut conf_state = Vec::with_capacity(n);
    let mut p = 20;
    for _ in 0..n {
        if p + 8 > b.len() {
            return None;
        }
        conf_state.push(u64::from_be_bytes(b[p..p + 8].try_into().unwrap()));
        p += 8;
    }
    Some(Snapshot { last_included_index, last_included_term, conf_state, data: b[p..].to_vec() })
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
        Entry::normal(term, index, data.to_vec())
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

    #[test]
    fn compact_persists_snapshot_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = WalStorage::open(dir.path()).unwrap();
            s.append(&[entry(1, 1, b"a"), entry(1, 2, b"b"), entry(2, 3, b"c"), entry(2, 4, b"d")]);
            s.compact(3, 2, vec![1, 2, 3], b"state@3".to_vec());

            assert_eq!(s.first_index(), 4);
            assert_eq!(s.last_index(), 4);
            assert_eq!(s.term(3), Some(2), "snapshot boundary term");
            assert_eq!(s.term(2), None, "compacted away");
            assert_eq!(s.entries(1, 4), vec![entry(2, 4, b"d")], "only the tail remains");
            assert_eq!(s.snapshot().unwrap().data, b"state@3");
        }
        // Restart: the snapshot and the surviving tail both recover; superseded
        // records left in the segment are filtered against the snapshot boundary.
        let mut s = WalStorage::open(dir.path()).unwrap();
        assert_eq!(s.first_index(), 4);
        assert_eq!(s.last_index(), 4);
        assert_eq!(s.term(3), Some(2));
        assert_eq!(s.entries(4, 4), vec![entry(2, 4, b"d")]);
        assert_eq!(
            s.snapshot().unwrap(),
            Snapshot {
                last_included_index: 3,
                last_included_term: 2,
                conf_state: vec![1, 2, 3],
                data: b"state@3".to_vec(),
            },
            "snapshot incl. membership survives restart",
        );

        // Replication continues above the boundary.
        s.append(&[entry(2, 5, b"e")]);
        assert_eq!(s.entries(4, 5), vec![entry(2, 4, b"d"), entry(2, 5, b"e")]);
    }

    #[test]
    fn apply_snapshot_supersedes_log_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = WalStorage::open(dir.path()).unwrap();
            s.append(&[entry(1, 1, b"a"), entry(1, 2, b"b")]);
            // A follower installs a snapshot from far ahead of its log.
            s.apply_snapshot(Snapshot {
                last_included_index: 10,
                last_included_term: 4,
                conf_state: vec![1, 2, 3],
                data: b"installed".to_vec(),
            });
            assert_eq!(s.first_index(), 11);
            assert_eq!(s.last_index(), 10);
            assert_eq!(s.term(10), Some(4));
            assert!(s.entries(1, 10).is_empty(), "old log superseded");
            s.append(&[entry(4, 11, b"k")]);
            assert_eq!(s.term(11), Some(4));
        }
        let s = WalStorage::open(dir.path()).unwrap();
        assert_eq!(s.first_index(), 11);
        assert_eq!(s.last_index(), 11);
        assert_eq!(s.term(10), Some(4), "installed snapshot boundary recovers");
        assert_eq!(s.entries(11, 11), vec![entry(4, 11, b"k")]);
    }
}
