//! MVCC snapshot reads and Percolator lock resolution.
//!
//! A read at `read_ts` resolves the correct committed version:
//!
//! 1. **Lock check** — if the Lock CF holds a lock on the key with `start_ts ≤
//!    read_ts`, the read can't proceed until that txn's fate is known, so it runs
//!    [`lock resolution`](Engine::resolve_lock) and retries.
//! 2. **Write CF** — seek the newest commit with `commit_ts ≤ read_ts` to obtain the
//!    transaction's `start_ts`.
//! 3. **Default CF** — read the value stored at `(key, start_ts)`.
//!
//! Lock resolution is the self-healing core: a reader that meets a leftover lock
//! follows it to the primary and either rolls the encountered key **forward** (the
//! primary committed), **back** (the primary's lock expired → the txn is dead), or
//! **waits** (the primary is still alive). Recovery is thus a property of the
//! protocol, not a separate subsystem.

use crate::db::Engine;
use crate::error::{Error, Result};
use crate::keys::{decode_data_key, decode_write_value, encode_data_key, Cf, Lock, Value};
use crate::memtable::MemValue;

/// A read view at a fixed timestamp.
pub struct Snapshot<'a> {
    engine: &'a Engine,
    read_ts: u64,
}

impl<'a> Snapshot<'a> {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.engine.mvcc_get(key, self.read_ts)
    }
    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }
}

impl Engine {
    /// A snapshot read view at `read_ts`.
    pub fn snapshot(&self, read_ts: u64) -> Snapshot<'_> {
        Snapshot { engine: self, read_ts }
    }

    /// Snapshot-isolated point read: the value committed for `user_key` as of `read_ts`,
    /// or `None` if there is no visible committed version (or it is a tombstone).
    pub fn mvcc_get(&self, user_key: &[u8], read_ts: u64) -> Result<Option<Vec<u8>>> {
        // 1. Resolve any lock that could shadow our snapshot.
        for _ in 0..8 {
            let Some(lock_bytes) = self.get_cf_raw(Cf::Lock, user_key)? else {
                break;
            };
            let lock = Lock::decode(&lock_bytes).ok_or_else(|| Error::corruption("lock decode"))?;
            if lock.start_ts > read_ts {
                break; // a txn started after our snapshot — invisible to us
            }
            self.resolve_lock(user_key, &lock, read_ts)?; // may return Err(KeyIsLocked)
        }
        self.read_committed(user_key, read_ts)
    }

    /// Like [`mvcc_get`](Self::mvcc_get) but **non-mutating**: a lock with `start_ts <=
    /// read_ts` is reported as `Err(KeyIsLocked)` rather than *resolved*. Resolution is a
    /// write, and in a replicated region every write must go through Raft (not a read) or
    /// replicas would diverge — so a replicated read defers to the locking txn's own
    /// commit/rollback command and the caller retries once it has applied.
    pub fn mvcc_get_unresolved(&self, user_key: &[u8], read_ts: u64) -> Result<Option<Vec<u8>>> {
        if let Some(lock_bytes) = self.get_cf_raw(Cf::Lock, user_key)? {
            let lock = Lock::decode(&lock_bytes).ok_or_else(|| Error::corruption("lock decode"))?;
            if lock.start_ts <= read_ts {
                return Err(Error::KeyIsLocked(format!(
                    "key locked by txn {} (a replicated read won't resolve it)",
                    lock.start_ts
                )));
            }
        }
        self.read_committed(user_key, read_ts)
    }

    /// Steps 2–3 of a snapshot read: the newest commit `<= read_ts` and its value. Assumes
    /// any shadowing lock has already been resolved or rejected by the caller.
    fn read_committed(&self, user_key: &[u8], read_ts: u64) -> Result<Option<Vec<u8>>> {
        // 2. Newest commit with commit_ts <= read_ts.
        let seek = encode_data_key(user_key, read_ts);
        let Some((wkey, wval)) = self.seek_cf_raw(Cf::Write, &seek)? else {
            return Ok(None);
        };
        let Some((wuser, _commit_ts)) = decode_data_key(&wkey) else {
            return Err(Error::corruption("write-cf key decode"));
        };
        if wuser != user_key {
            return Ok(None); // no committed version of this key at/below read_ts
        }
        let MemValue::Put(start_ts_bytes) = wval else {
            return Ok(None);
        };
        let start_ts =
            decode_write_value(&start_ts_bytes).ok_or_else(|| Error::corruption("write-cf value"))?;

        // 3. Value at (user_key, start_ts) in the Default CF.
        match self.get_cf_raw(Cf::Default, &encode_data_key(user_key, start_ts))? {
            Some(val_bytes) => {
                let v = Value::decode(&val_bytes).ok_or_else(|| Error::corruption("default-cf value"))?;
                Ok(v.into_option())
            }
            None => Ok(None), // committed pointer with no data — should not happen
        }
    }

    /// Resolve a leftover lock on `key` by consulting its `primary`.
    fn resolve_lock(&self, key: &[u8], lock: &Lock, now_ts: u64) -> Result<()> {
        let primary = &lock.primary;
        let primary_lock = match self.get_cf_raw(Cf::Lock, primary)? {
            Some(b) => Some(Lock::decode(&b).ok_or_else(|| Error::corruption("primary lock decode"))?),
            None => None,
        };

        // Primary still locked by *this* txn → the transaction is still pending.
        if let Some(plock) = &primary_lock {
            if plock.start_ts == lock.start_ts {
                if now_ts > plock.ttl {
                    // TTL expired → the txn is dead → roll back.
                    self.roll_back(primary, lock.start_ts)?;
                    if key != primary {
                        self.roll_back(key, lock.start_ts)?;
                    }
                    return Ok(());
                }
                return Err(Error::KeyIsLocked(format!(
                    "primary {primary:?} lock alive (ttl {})",
                    plock.ttl
                )));
            }
        }

        // Primary is not locked by this txn → it committed or aborted; ask the Write CF.
        // (Roll-forward is idempotent and conditional, so it is also correct when
        // `key == primary` — it just re-writes the existing commit and drops the lock.)
        match self.find_commit_ts(primary, lock.start_ts)? {
            Some(commit_ts) => self.roll_forward(key, lock.start_ts, commit_ts)?,
            None => self.roll_back(key, lock.start_ts)?,
        }
        Ok(())
    }

    /// Find the primary's commit record pointing at `start_ts`, scanning its Write CF
    /// versions newest→oldest. `None` ⇒ the txn never committed (aborted/pending).
    fn find_commit_ts(&self, primary: &[u8], start_ts: u64) -> Result<Option<u64>> {
        let mut seek = encode_data_key(primary, u64::MAX); // newest version first
        loop {
            let Some((wkey, wval)) = self.seek_cf_raw(Cf::Write, &seek)? else {
                return Ok(None);
            };
            let Some((wuser, commit_ts)) = decode_data_key(&wkey) else {
                return Err(Error::corruption("write-cf key decode"));
            };
            if wuser != primary {
                return Ok(None);
            }
            if let MemValue::Put(sb) = &wval {
                if decode_write_value(sb) == Some(start_ts) {
                    return Ok(Some(commit_ts));
                }
            }
            // Advance to the next-older version: smallest key strictly greater than wkey.
            seek = wkey;
            seek.push(0);
        }
    }

    /// Roll a committed secondary forward (conditional: only if `key`'s lock still
    /// belongs to `start_ts`, re-checked atomically in the committer).
    fn roll_forward(&self, key: &[u8], start_ts: u64, commit_ts: u64) -> Result<()> {
        self.resolve_commit(key, start_ts, commit_ts)
    }

    /// Roll a key back (conditional: only if its lock still belongs to `start_ts`).
    fn roll_back(&self, key: &[u8], start_ts: u64) -> Result<()> {
        self.resolve_rollback(key, start_ts)
    }
}

#[cfg(test)]
mod tests {
    use crate::db::Engine;
    use crate::error::Error;
    use crate::keys::Value;
    use crate::options::Options;

    #[test]
    fn unresolved_read_reports_a_lock_then_sees_the_commit() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(Options::new(dir.path())).unwrap();

        // Prewrite leaves a lock at start_ts=10 (the txn is in-flight, not yet committed).
        e.prewrite_one(b"k", &Value::Put(b"v".to_vec()), b"k", 10, 1_000_000).unwrap();

        // A non-mutating read at/after the lock's start_ts reports it instead of resolving.
        match e.mvcc_get_unresolved(b"k", 20) {
            Err(Error::KeyIsLocked(_)) => {}
            other => panic!("expected KeyIsLocked, got {other:?}"),
        }
        // A read *before* the lock exists is unshadowed and simply sees no committed version.
        assert_eq!(e.mvcc_get_unresolved(b"k", 5).unwrap(), None);

        // Once the txn commits, the lock is gone and the value is visible.
        e.resolve_commit(b"k", 10, 15).unwrap();
        assert_eq!(e.mvcc_get_unresolved(b"k", 20).unwrap(), Some(b"v".to_vec()));
    }
}
