//! The engine core: wires the WAL, memtables, SSTables and manifest into a single
//! durable store, and hosts the **group-commit committer**.
//!
//! ## Group commit
//!
//! All writes funnel through one committer thread. A writer enqueues its batch and
//! blocks on a one-shot ack. The committer drains every currently-queued batch into
//! a *group*, appends them to the WAL, issues a **single `fsync`**, then applies
//! them to the active memtable and acks every writer. A single writer thread makes
//! sequence-number assignment and memtable ordering trivially correct; the fsync
//! cost is amortized across the whole group.
//!
//! ## Flush
//!
//! When the active memtable crosses the size threshold the committer **freezes** it
//! (swaps in a fresh active, rotates the WAL) and flushes it to an SSTable inline,
//! then advances `last_flushed_seq` and rewrites the manifest. Inline (synchronous)
//! flush keeps Phase 1 deterministic and free of an extra thread; a background flush
//! thread is a Phase 1b refinement that arrives with compaction.
//!
//! Read precedence is newest→oldest: active memtable → immutable memtables →
//! SSTables (newest first). A tombstone at any level is authoritative and shadows
//! everything below it.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use parking_lot::{Mutex, RwLock};

use crate::batch::WriteBatch;
use crate::error::{Error, Result};
use crate::keys::{Cf, Lock, LockKind};
use crate::manifest::Manifest;
use crate::memtable::{MemValue, Memtable};
use crate::options::Options;
use crate::sstable::{SstReader, SstWriter};
use crate::wal::{WalReader, WalWriter};

/// Cap on how many batches one group commit will coalesce.
const MAX_GROUP: usize = 1024;

/// A Percolator prewrite, evaluated atomically by the committer: it conflict-checks
/// against the live state and, only if clear, writes the Default value + Lock.
struct PrewriteSpec {
    key: Vec<u8>,
    /// `crate::keys::Value`-encoded payload written to the Default CF at `start_ts`.
    value: Vec<u8>,
    primary: Vec<u8>,
    start_ts: u64,
    ttl: u64,
    kind: LockKind,
}

/// How to resolve a lock: complete its transaction's commit on this key, or roll it
/// back. Applied only if the key's lock still belongs to `start_ts` (see [`Op`]).
enum ResolveAction {
    Commit(u64), // commit_ts
    Rollback,
}

/// A conditional lock-resolution: re-check `key`'s lock at apply time and act only
/// if it still belongs to `start_ts`. This is essential for correctness — an
/// unconditional "delete the lock on `key`" would race with another transaction
/// re-locking `key`, deleting *its* lock and breaking mutual exclusion.
struct ResolveSpec {
    key: Vec<u8>,
    start_ts: u64,
    action: ResolveAction,
}

/// Work submitted to the committer. `Write` is unconditional; `Prewrite` and
/// `Resolve` are conditional check-and-sets (the check is serialized through the
/// single committer, making it atomic without per-key locks).
enum Op {
    Write(WriteBatch),
    Prewrite(PrewriteSpec),
    Resolve(ResolveSpec),
}

struct Request {
    op: Op,
    ack: Sender<Result<u64>>,
}

/// Shared engine state, held by both the public [`Engine`] handle and the
/// committer thread.
struct Core {
    opts: Options,
    /// The mutable memtable receiving new writes.
    active: RwLock<Arc<Memtable>>,
    /// Frozen memtables awaiting/undergoing flush (oldest first).
    immutable: Mutex<Vec<Arc<Memtable>>>,
    /// Live SSTables, oldest first; read newest-first via `.iter().rev()`.
    sstables: Mutex<Vec<Arc<SstReader>>>,
    /// The current WAL segment (only the committer appends; lock guards rotation).
    wal: Mutex<WalWriter>,
    /// Last assigned log sequence number.
    seq: AtomicU64,
    /// Highest seq durably captured in an SSTable.
    last_flushed_seq: AtomicU64,
    /// Next WAL segment file number.
    next_seg: AtomicU64,
    /// Next SSTable file number.
    next_sst: AtomicU64,
    /// Serializes manifest rewrites.
    manifest_lock: Mutex<()>,
    /// Makes each batch's memtable application atomic *across column families* with
    /// respect to concurrent readers. The committer holds the write side around a
    /// single batch apply (and structural swaps); readers hold the read side for the
    /// whole of a `get_cf`/`seek_cf`. Without this, the Lock/Default/Write CFs are
    /// independent skiplists, so a reader could see a commit's lock-delete without
    /// its Write-insert — reading a stale version and breaking snapshot isolation.
    read_gate: RwLock<()>,
}

impl Core {
    fn segment_path(&self, n: u64) -> PathBuf {
        self.opts.data_dir.join(format!("{n:020}.wal"))
    }
    fn sst_path(&self, n: u64) -> PathBuf {
        self.opts.data_dir.join(format!("{n:020}.sst"))
    }

    /// Process a group: per op, append to the WAL and apply to the memtable; one
    /// `fsync` for the whole group; then (if full) freeze + flush. Returns a result
    /// per request (a conflicting prewrite yields `Err(Conflict)` and writes nothing).
    ///
    /// Ops are applied to the memtable *before* the group fsync so a later op in the
    /// same group sees an earlier one (e.g. two prewrites of the same key conflict).
    /// This is safe: nothing is acked until the fsync, and a crash before it drops
    /// the whole un-acked group atomically (the memtable is in-process).
    fn process_group(&self, group: &[Request]) -> Vec<Result<u64>> {
        let mut results: Vec<Result<u64>> = Vec::with_capacity(group.len());
        let frozen: Option<Arc<Memtable>>;
        {
            let mut wal = self.wal.lock();
            let active = self.active.read().clone();
            let mut appended = false;

            for req in group {
                let batch = match &req.op {
                    Op::Write(b) => b.clone(),
                    Op::Prewrite(spec) => match self.check_prewrite(spec.key.as_slice(), spec.start_ts) {
                        Ok(()) => build_prewrite_batch(spec),
                        Err(e) => {
                            results.push(Err(e));
                            continue;
                        }
                    },
                    Op::Resolve(spec) => match self.current_lock(&spec.key) {
                        // Act only if the lock still belongs to this start_ts.
                        Ok(Some(lock)) if lock.start_ts == spec.start_ts => build_resolve_batch(spec),
                        Ok(_) => {
                            results.push(Ok(0)); // stale/gone → safe no-op
                            continue;
                        }
                        Err(e) => {
                            results.push(Err(e));
                            continue;
                        }
                    },
                };
                let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
                match wal.append(seq, &batch.encode()) {
                    Ok(()) => {
                        // Apply the whole batch atomically w.r.t. readers (cross-CF).
                        let _gate = self.read_gate.write();
                        active.apply(seq, &batch);
                        appended = true;
                        results.push(Ok(seq));
                    }
                    Err(e) => results.push(Err(e)),
                }
            }

            // The durability barrier: one fsync for the entire group.
            if appended {
                if let Err(e) = wal.sync(self.opts.fsync_mode) {
                    let msg = e.to_string();
                    // A sync failure invalidates every just-appended op in the group.
                    for r in results.iter_mut() {
                        if r.is_ok() {
                            *r = Err(Error::corruption(format!("group fsync failed: {msg}")));
                        }
                    }
                    return results;
                }
            }

            frozen = if active.approx_size() >= self.opts.memtable_size_threshold {
                Some(self.rotate(&mut wal, active))
            } else {
                None
            };
        }
        if let Some(m) = frozen {
            let _ = self.flush_memtable(m); // flush errors surface on the next op/read
        }
        results
    }

    /// The current lock on `key`, if any (decoded).
    fn current_lock(&self, key: &[u8]) -> Result<Option<Lock>> {
        match self.get_cf(Cf::Lock, key)? {
            Some(b) => Ok(Some(Lock::decode(&b).ok_or_else(|| Error::corruption("lock decode"))?)),
            None => Ok(None),
        }
    }

    /// Percolator prewrite conflict checks against the live state (all levels):
    /// abort if a commit newer than our snapshot exists, or any lock is present.
    fn check_prewrite(&self, key: &[u8], start_ts: u64) -> Result<()> {
        // Write-after-snapshot: newest commit on `key` with commit_ts >= start_ts.
        let newest = self.seek_cf(Cf::Write, &crate::keys::encode_data_key(key, u64::MAX))?;
        if let Some((wkey, _)) = newest {
            if let Some((wuser, commit_ts)) = crate::keys::decode_data_key(&wkey) {
                if wuser == key && commit_ts >= start_ts {
                    return Err(Error::conflict("write conflict: a newer commit exists"));
                }
            }
        }
        // Concurrent txn: any lock already on `key`.
        if self.get_cf(Cf::Lock, key)?.is_some() {
            return Err(Error::conflict("key is already locked by another transaction"));
        }
        Ok(())
    }

    /// Merged forward seek across all levels: the smallest CF key ≥ `from`, with the
    /// newest level winning on ties. Powers MVCC "newest visible version".
    fn seek_cf(&self, cf: Cf, from: &[u8]) -> Result<Option<(Vec<u8>, MemValue)>> {
        // Collect one candidate per source, tagged newest-first by `rank` (lower =
        // newer). The winner is the smallest key, ties broken by lower rank.
        let _gate = self.read_gate.read();
        let mut cands: Vec<(Vec<u8>, MemValue, u64)> = Vec::new();
        let mut rank = 0u64;

        let active = self.active.read().clone();
        if let Some((k, v)) = active.seek_cf(cf, from) {
            cands.push((k, v, rank));
        }
        rank += 1;
        {
            let imm = self.immutable.lock();
            for m in imm.iter().rev() {
                if let Some((k, v)) = m.seek_cf(cf, from) {
                    cands.push((k, v, rank));
                }
                rank += 1;
            }
        }
        {
            let ssts = self.sstables.lock();
            for s in ssts.iter().rev() {
                if let Some((k, v)) = s.seek(cf, from)? {
                    cands.push((k, v, rank));
                }
                rank += 1;
            }
        }
        Ok(cands
            .into_iter()
            .min_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)))
            .map(|(k, v, _)| (k, v)))
    }

    /// Swap in a fresh active memtable, queue the old one for flush, and rotate to a
    /// new WAL segment. Returns the frozen memtable. Called only by the committer,
    /// holding the WAL lock.
    fn rotate(&self, wal: &mut WalWriter, frozen: Arc<Memtable>) -> Arc<Memtable> {
        {
            // Swap active + enqueue the frozen memtable atomically w.r.t. readers.
            let _gate = self.read_gate.write();
            *self.active.write() = Arc::new(Memtable::new());
            self.immutable.lock().push(frozen.clone());
        }
        let n = self.next_seg.fetch_add(1, Ordering::SeqCst);
        // If rotation fails we keep writing to the old segment; surface lazily.
        if let Ok(w) = WalWriter::create(self.segment_path(n)) {
            *wal = w;
        }
        frozen
    }

    /// Persist a frozen memtable to an SSTable, install its reader, advance the
    /// flushed watermark, rewrite the manifest, then drop it from the immutable
    /// queue. Order matters: the SSTable is readable *before* the memtable is
    /// forgotten, so a concurrent reader never sees a gap.
    fn flush_memtable(&self, frozen: Arc<Memtable>) -> Result<()> {
        if !frozen.is_empty() {
            let n = self.next_sst.fetch_add(1, Ordering::SeqCst);
            let path = self.sst_path(n);
            write_sst(&path, &frozen)?; // slow IO, outside the gate
            let reader = Arc::new(SstReader::open(&path, n)?);
            let _gate = self.read_gate.write();
            self.sstables.lock().push(reader);
        }
        self.last_flushed_seq.fetch_max(frozen.max_seq(), Ordering::SeqCst);
        self.persist_manifest()?;
        // Now safe to forget the in-memory copy (atomic w.r.t. readers).
        let _gate = self.read_gate.write();
        let mut imm = self.immutable.lock();
        if let Some(pos) = imm.iter().position(|m| Arc::ptr_eq(m, &frozen)) {
            imm.remove(pos);
        }
        Ok(())
    }

    fn persist_manifest(&self) -> Result<()> {
        let _g = self.manifest_lock.lock();
        let sstables: Vec<u64> = self.sstables.lock().iter().map(|s| s.file_no()).collect();
        let m = Manifest {
            last_flushed_seq: self.last_flushed_seq.load(Ordering::SeqCst),
            sstables,
        };
        m.store(&self.opts.data_dir)
    }

    /// Read a CF key across all levels, returning the first authoritative hit.
    fn get_cf(&self, cf: Cf, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Hold the read gate for the whole traversal so no batch applies mid-read.
        let _gate = self.read_gate.read();
        let active = self.active.read().clone();
        if let Some(v) = active.get_cf(cf, key) {
            return Ok(v.into_present());
        }
        {
            let imm = self.immutable.lock();
            for m in imm.iter().rev() {
                if let Some(v) = m.get_cf(cf, key) {
                    return Ok(v.into_present());
                }
            }
        }
        let ssts = self.sstables.lock();
        for sst in ssts.iter().rev() {
            if let Some(v) = sst.get(cf, key)? {
                return Ok(v.into_present());
            }
        }
        Ok(None)
    }
}

/// Parse a zero-padded numeric filename like `00000000000000000007.wal`.
fn parse_numbered(path: &Path, ext: &str) -> Option<u64> {
    path.file_name()?.to_str()?.strip_suffix(ext)?.parse::<u64>().ok()
}

/// List `dir`'s files with the given extension (e.g. `".wal"`), sorted ascending
/// by their numeric stem.
fn list_numbered(dir: &Path, ext: &str) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if let Some(n) = parse_numbered(&path, ext) {
            out.push((n, path));
        }
    }
    out.sort_by_key(|(n, _)| *n);
    Ok(out)
}

/// Build an SSTable from a frozen memtable: CFs in id order (Default, Write, Lock),
/// each already key-sorted, prefixed with the CF id so the union is globally sorted.
fn write_sst(path: &Path, mem: &Memtable) -> Result<()> {
    let mut w = SstWriter::create(path)?;
    for cf in Cf::ALL {
        for (k, v) in mem.iter_cf(cf) {
            let mut sk = Vec::with_capacity(1 + k.len());
            sk.push(cf as u8);
            sk.extend_from_slice(&k);
            w.add(&sk, &v.encode())?;
        }
    }
    w.finish()
}

/// Build the batch for a validated lock resolution (commit-forward or rollback).
fn build_resolve_batch(spec: &ResolveSpec) -> WriteBatch {
    let mut b = WriteBatch::new();
    match spec.action {
        ResolveAction::Commit(commit_ts) => {
            b.put(
                Cf::Write,
                crate::keys::encode_data_key(&spec.key, commit_ts),
                crate::keys::encode_write_value(spec.start_ts).to_vec(),
            );
            b.delete(Cf::Lock, spec.key.clone());
        }
        ResolveAction::Rollback => {
            b.delete(Cf::Lock, spec.key.clone());
            b.delete(Cf::Default, crate::keys::encode_data_key(&spec.key, spec.start_ts));
        }
    }
    b
}

/// Build the atomic Default+Lock batch for a passing prewrite.
fn build_prewrite_batch(spec: &PrewriteSpec) -> WriteBatch {
    let mut b = WriteBatch::new();
    b.put(Cf::Default, crate::keys::encode_data_key(&spec.key, spec.start_ts), spec.value.clone());
    let lock = Lock {
        primary: spec.primary.clone(),
        start_ts: spec.start_ts,
        ttl: spec.ttl,
        kind: spec.kind,
    };
    b.put(Cf::Lock, spec.key.clone(), lock.encode());
    b
}

fn run_committer(core: Arc<Core>, rx: Receiver<Request>) {
    loop {
        // Block for the first request; a closed channel (all senders dropped) = shutdown.
        let first = match rx.recv() {
            Ok(r) => r,
            Err(_) => break,
        };
        let mut group = vec![first];
        while group.len() < MAX_GROUP {
            match rx.try_recv() {
                Ok(r) => group.push(r),
                Err(_) => break,
            }
        }
        let results = core.process_group(&group);
        for (req, res) in group.into_iter().zip(results) {
            let _ = req.ack.send(res);
        }
    }
}

/// A handle to an open engine. The committer thread is joined on drop.
pub struct Engine {
    core: Arc<Core>,
    submit: Option<Sender<Request>>,
    committer: Option<JoinHandle<()>>,
}

impl Engine {
    /// Open (creating if absent) an engine rooted at `opts.data_dir`, **recovering**
    /// any existing state:
    ///
    /// 1. load the manifest → the set of live SSTables and `last_flushed_seq`;
    /// 2. open those SSTables;
    /// 3. replay every WAL record with `seq > last_flushed_seq` (records at-or-below
    ///    the watermark are already in an SSTable) into a fresh active memtable,
    ///    stopping each segment at its torn tail;
    /// 4. reclaim fully-flushed WAL segments and orphan SSTables (an SSTable written
    ///    but not yet recorded in the manifest before a crash — its data is still in
    ///    the WAL and replays in step 3).
    ///
    /// The resulting state equals exactly the set of writes that were fsync-acked
    /// before the crash: no acknowledged write is lost, none is duplicated.
    pub fn open(opts: Options) -> Result<Engine> {
        std::fs::create_dir_all(&opts.data_dir)?;
        let dir = opts.data_dir.clone();
        let manifest = Manifest::load(&dir)?;
        let last_flushed = manifest.last_flushed_seq;

        // (1,2) Open live SSTables.
        let mut sstables = Vec::with_capacity(manifest.sstables.len());
        let mut max_sst_no = 0u64;
        for &n in &manifest.sstables {
            sstables.push(Arc::new(SstReader::open(dir.join(format!("{n:020}.sst")), n)?));
            max_sst_no = max_sst_no.max(n);
        }

        // (3) Replay WAL into a fresh active memtable.
        let active = Arc::new(Memtable::new());
        let segments = list_numbered(&dir, ".wal")?;
        let mut max_seq = last_flushed;
        let mut max_seg_no = 0u64;
        let mut reclaimable_segs: Vec<PathBuf> = Vec::new();
        for (segno, path) in &segments {
            max_seg_no = max_seg_no.max(*segno);
            let mut reader = WalReader::open(path)?;
            let mut has_live = false;
            while let Some((seq, payload)) = reader.next_record() {
                max_seq = max_seq.max(seq);
                if seq > last_flushed {
                    has_live = true;
                    let batch = WriteBatch::decode(&payload)
                        .ok_or_else(|| Error::corruption("WAL batch decode failed"))?;
                    active.apply(seq, &batch);
                }
            }
            // A segment with no records above the watermark is fully captured in
            // SSTables and safe to delete.
            if !has_live {
                reclaimable_segs.push(path.clone());
            }
        }

        // A fresh segment receives subsequent writes (never append to a torn tail).
        let new_seg = max_seg_no + 1;
        let wal = WalWriter::create(dir.join(format!("{new_seg:020}.wal")))?;

        // (4) Reclaim. Safe only after the new segment exists and replay is done.
        for p in reclaimable_segs {
            let _ = std::fs::remove_file(p);
        }
        for (n, path) in list_numbered(&dir, ".sst")? {
            if !manifest.sstables.contains(&n) {
                let _ = std::fs::remove_file(path); // orphan SSTable; data is in the WAL
            }
        }

        let core = Arc::new(Core {
            opts,
            active: RwLock::new(active),
            immutable: Mutex::new(Vec::new()),
            sstables: Mutex::new(sstables),
            wal: Mutex::new(wal),
            seq: AtomicU64::new(max_seq),
            last_flushed_seq: AtomicU64::new(last_flushed),
            next_seg: AtomicU64::new(new_seg + 1),
            next_sst: AtomicU64::new(max_sst_no + 1),
            manifest_lock: Mutex::new(()),
            read_gate: RwLock::new(()),
        });
        let (tx, rx) = unbounded();
        let committer = {
            let core = core.clone();
            thread::spawn(move || run_committer(core, rx))
        };
        Ok(Engine { core, submit: Some(tx), committer: Some(committer) })
    }

    fn submit(&self, op: Op) -> Result<u64> {
        let (ack_tx, ack_rx) = bounded(1);
        self.submit
            .as_ref()
            .expect("engine is open")
            .send(Request { op, ack: ack_tx })
            .map_err(|_| Error::corruption("engine committer stopped"))?;
        ack_rx
            .recv()
            .map_err(|_| Error::corruption("committer dropped ack"))?
    }

    /// Durably apply an atomic batch; returns the assigned log sequence number.
    /// Blocks until the batch is fsync'd (group-committed).
    pub fn write(&self, batch: WriteBatch) -> Result<u64> {
        if batch.is_empty() {
            return Ok(self.core.seq.load(Ordering::SeqCst));
        }
        self.submit(Op::Write(batch))
    }

    /// Percolator prewrite of a single key: atomically conflict-check then write the
    /// Default value + Lock. Returns `Err(Error::Conflict)` if another commit newer
    /// than `start_ts` exists or the key is already locked. `value` is a
    /// [`crate::keys::Value`] (Put or Delete tombstone).
    pub fn prewrite_one(
        &self,
        key: &[u8],
        value: &crate::keys::Value,
        primary: &[u8],
        start_ts: u64,
        ttl: u64,
    ) -> Result<u64> {
        let kind = match value {
            crate::keys::Value::Put(_) => LockKind::Put,
            crate::keys::Value::Delete => LockKind::Delete,
        };
        self.submit(Op::Prewrite(PrewriteSpec {
            key: key.to_vec(),
            value: value.encode(),
            primary: primary.to_vec(),
            start_ts,
            ttl,
            kind,
        }))
    }

    /// Conditionally complete a transaction's commit on `key` (write the Write
    /// record + drop the lock) — but only if `key`'s lock still belongs to
    /// `start_ts`. A no-op if the lock has changed or is gone. Used both for lazy
    /// secondary finalization and for reader-driven roll-forward.
    pub fn resolve_commit(&self, key: &[u8], start_ts: u64, commit_ts: u64) -> Result<()> {
        self.submit(Op::Resolve(ResolveSpec {
            key: key.to_vec(),
            start_ts,
            action: ResolveAction::Commit(commit_ts),
        }))?;
        Ok(())
    }

    /// Conditionally roll `key` back (drop the lock + uncommitted Default value) —
    /// only if its lock still belongs to `start_ts`. A no-op otherwise.
    pub fn resolve_rollback(&self, key: &[u8], start_ts: u64) -> Result<()> {
        self.submit(Op::Resolve(ResolveSpec {
            key: key.to_vec(),
            start_ts,
            action: ResolveAction::Rollback,
        }))?;
        Ok(())
    }

    /// Raw column-family point read across the active + immutable memtables and all
    /// SSTables (exact key). MVCC version resolution layers on top (see `mvcc`).
    pub fn get_cf_raw(&self, cf: Cf, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.core.get_cf(cf, key)
    }

    /// Merged forward seek: the smallest CF key ≥ `from` across all levels, newest
    /// level winning ties. Used by the MVCC read path.
    pub fn seek_cf_raw(&self, cf: Cf, from: &[u8]) -> Result<Option<(Vec<u8>, MemValue)>> {
        self.core.seek_cf(cf, from)
    }

    pub fn options(&self) -> &Options {
        &self.core.opts
    }

    /// The most recently assigned log sequence number.
    pub fn last_seq(&self) -> u64 {
        self.core.seq.load(Ordering::SeqCst)
    }

    /// Highest seq durably captured in an SSTable.
    pub fn last_flushed_seq(&self) -> u64 {
        self.core.last_flushed_seq.load(Ordering::SeqCst)
    }

    /// Number of live SSTables (test/introspection helper).
    pub fn sstable_count(&self) -> usize {
        self.core.sstables.lock().len()
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Close the submit channel so the committer's `recv` errors and it exits,
        // then join it so all queued writes have been persisted.
        self.submit.take();
        if let Some(h) = self.committer.take() {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::WriteBatch;

    fn open_tmp() -> (tempfile::TempDir, Engine) {
        let dir = tempfile::tempdir().unwrap();
        let eng = Engine::open(Options::new(dir.path())).unwrap();
        (dir, eng)
    }

    #[test]
    fn put_get_delete_raw() {
        let (_d, eng) = open_tmp();
        let mut b = WriteBatch::new();
        b.put(Cf::Default, b"k1".to_vec(), b"v1".to_vec());
        b.put(Cf::Lock, b"k1".to_vec(), b"lock".to_vec());
        assert_eq!(eng.write(b).unwrap(), 1);

        assert_eq!(eng.get_cf_raw(Cf::Default, b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(eng.get_cf_raw(Cf::Lock, b"k1").unwrap(), Some(b"lock".to_vec()));
        assert_eq!(eng.get_cf_raw(Cf::Write, b"k1").unwrap(), None);

        let mut b2 = WriteBatch::new();
        b2.delete(Cf::Lock, b"k1".to_vec());
        assert_eq!(eng.write(b2).unwrap(), 2);

        assert_eq!(eng.get_cf_raw(Cf::Lock, b"k1").unwrap(), None);
        assert_eq!(eng.get_cf_raw(Cf::Default, b"k1").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn concurrent_writers_get_unique_monotonic_seqs() {
        let dir = tempfile::tempdir().unwrap();
        let eng = Arc::new(Engine::open(Options::new(dir.path())).unwrap());
        let mut handles = vec![];
        for i in 0..16u64 {
            let eng = eng.clone();
            handles.push(thread::spawn(move || {
                let mut b = WriteBatch::new();
                b.put(Cf::Default, format!("k{i}").into_bytes(), b"v".to_vec());
                eng.write(b).unwrap()
            }));
        }
        let mut seqs: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        seqs.sort_unstable();
        seqs.dedup();
        assert_eq!(seqs.len(), 16, "every writer gets a unique seq");
        assert_eq!(eng.last_seq(), 16);
    }

    #[test]
    fn flush_persists_and_reads_back_from_sstables() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny threshold forces many freezes/flushes.
        let eng = Engine::open(Options::new(dir.path()).with_memtable_threshold(256)).unwrap();
        for i in 0..200u64 {
            let mut b = WriteBatch::new();
            b.put(Cf::Default, format!("key{i:04}").into_bytes(), vec![b'x'; 32]);
            eng.write(b).unwrap();
        }
        assert!(eng.sstable_count() > 1, "expected multiple SSTables");
        assert!(eng.last_flushed_seq() > 0);

        // Every key must still be readable — now served from SSTables.
        for i in 0..200u64 {
            assert_eq!(
                eng.get_cf_raw(Cf::Default, format!("key{i:04}").as_bytes()).unwrap(),
                Some(vec![b'x'; 32]),
                "key{i} lost after flush"
            );
        }

        // The manifest on disk reflects the live SSTables.
        let m = Manifest::load(dir.path()).unwrap();
        assert_eq!(m.sstables.len(), eng.sstable_count());
        assert_eq!(m.last_flushed_seq, eng.last_flushed_seq());
    }

    #[test]
    fn tombstone_in_sstable_shadows_older_sstable() {
        let dir = tempfile::tempdir().unwrap();
        let eng = Engine::open(Options::new(dir.path()).with_memtable_threshold(64)).unwrap();

        // First flush: put k -> v.
        let mut b = WriteBatch::new();
        b.put(Cf::Default, b"k".to_vec(), b"v".to_vec());
        b.put(Cf::Default, b"filler1".to_vec(), vec![b'a'; 64]); // force a freeze
        eng.write(b).unwrap();

        // Second flush: delete k.
        let mut b2 = WriteBatch::new();
        b2.delete(Cf::Default, b"k".to_vec());
        b2.put(Cf::Default, b"filler2".to_vec(), vec![b'b'; 64]); // force another freeze
        eng.write(b2).unwrap();

        assert!(eng.sstable_count() >= 2);
        assert_eq!(eng.get_cf_raw(Cf::Default, b"k").unwrap(), None, "newer tombstone must win");
    }
}
