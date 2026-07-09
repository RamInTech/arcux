//! Cross-region distributed Percolator — the coordinator + reader that generalize the
//! single-region kernel ([`arcux_engine::Transaction`]) across region boundaries.
//!
//! ## The idea in one sentence
//!
//! Cross-region atomicity is faked by making the whole transaction's fate hinge on **one
//! atomic single-region write** — committing the *primary* key in its region — and having
//! everyone who cares chase a pointer to that one bit, so N regions never have to agree with
//! each other, only ever with the primary.
//!
//! ## What actually changes vs. the single-region kernel
//!
//! The algorithm is unchanged; only *where each step runs* changes. A transaction's
//! mutations are split by the region that owns each key:
//!
//! * **Prewrite** each key in its own region (the primary's region first), so a reader that
//!   meets any secondary lock can follow it to a primary at least as far along. Every
//!   secondary lock records the *global* primary pointer.
//! * **Commit** the primary in its region — the single linearization point — then lazily
//!   finalize each secondary in its own region.
//! * **Read** resolves a leftover lock by consulting the **primary's** region (which may be
//!   a *different* region than the key being read) for the transaction's [`TxnStatus`], then
//!   rolls the local key forward or back. This is the cross-region generalization of
//!   [`arcux_engine::Engine`]'s local `resolve_lock`.
//!
//! ## The [`Regions`] seam
//!
//! This module is the deterministic **core**: a region is modeled as an independent
//! [`Engine`] behind the [`Regions`] trait (`region_of(key)` → which region; `engine(id)` →
//! its committed state machine). Because regions own disjoint key ranges, using a *separate*
//! engine per region here faithfully models the cross-node case — and proves resolution
//! works when the primary lives on another node.
//!
//! The **transport slice** replaces this seam with real region leaders: each prewrite /
//! commit / resolve becomes a Raft proposal in the key's region, and `check_txn_status`
//! becomes a `CheckTxnStatus` RPC to the primary's region leader. The orchestration below is
//! unchanged by that — it holds no durable state (the primary lock is the source of truth),
//! so a coordinator crash is recovered by any later reader via lock resolution.

use arcux_engine::{Cf, Engine, Error, Lock, Mutation, Result, Transaction, TxnStatus};

/// A set of regions addressable by key. Each key belongs to exactly one region; a region is
/// an independent replicated state machine, modeled here as an [`Engine`].
pub trait Regions {
    /// The id of the region that owns `key`.
    fn region_of(&self, key: &[u8]) -> u64;
    /// The engine backing region `id` (its committed state machine).
    fn engine(&self, id: u64) -> &Engine;
}

/// A transaction whose keys may span several regions. `mutations[0]` is the **primary**.
pub struct CrossTxn {
    start_ts: u64,
    primary: Vec<u8>,
    mutations: Vec<Mutation>,
}

impl CrossTxn {
    /// Begin at `start_ts`; `mutations[0]` becomes the primary. Keys may live in any regions.
    pub fn new(start_ts: u64, mutations: Vec<Mutation>) -> Result<CrossTxn> {
        let Some(first) = mutations.first() else {
            return Err(Error::invalid("transaction needs at least one mutation"));
        };
        let primary = first.key.clone();
        Ok(CrossTxn { start_ts, primary, mutations })
    }

    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }
    pub fn primary(&self) -> &[u8] {
        &self.primary
    }

    /// Phase 1: prewrite every mutation in **its own region**, primary first. On the first
    /// conflict, rolls back this txn's own partial locks (each in its region) and returns the
    /// error — so a clean abort never blocks others. (A *crash* mid-prewrite instead relies
    /// on TTL-based lock resolution at read time.)
    pub fn prewrite<R: Regions>(&self, regions: &R, ttl: u64) -> Result<()> {
        let mut acquired: Vec<Vec<u8>> = Vec::new();

        // Primary first: a reader that meets a secondary lock must be able to follow it to a
        // primary that is at least as far along.
        let mut result = regions
            .engine(regions.region_of(&self.primary))
            .prewrite_one(&self.primary, &self.mutations[0].value, &self.primary, self.start_ts, ttl)
            .map(|_| acquired.push(self.primary.clone()));

        if result.is_ok() {
            for m in &self.mutations[1..] {
                let eng = regions.engine(regions.region_of(&m.key));
                match eng.prewrite_one(&m.key, &m.value, &self.primary, self.start_ts, ttl) {
                    Ok(_) => acquired.push(m.key.clone()),
                    Err(e) => {
                        result = Err(e);
                        break;
                    }
                }
            }
        }

        if result.is_err() {
            // Drop our own partial locks, each in its region (conditional — never touch a
            // lock a concurrent txn may have since taken on the same key).
            for k in &acquired {
                let _ = regions.engine(regions.region_of(k)).resolve_rollback(k, self.start_ts);
            }
        }
        result
    }

    /// Phase 2a: commit the **primary** in its region — the atomic linearization point that
    /// decides the whole transaction. Once this returns, the transaction has committed even
    /// if every secondary is still locked.
    pub fn commit_primary<R: Regions>(&self, regions: &R, commit_ts: u64) -> Result<()> {
        let eng = regions.engine(regions.region_of(&self.primary));
        // Reuse the single-region kernel on a one-mutation view whose primary is *the*
        // primary — this writes the primary's Write record and drops its lock atomically.
        Transaction::new(eng, self.start_ts, vec![self.mutations[0].clone()])?.commit_primary(commit_ts)
    }

    /// Phase 2b: roll each secondary forward in **its own** region. Conditional (only acts if
    /// the lock is still ours), so it is safe against a reader that finalized the same
    /// secondary first. Skipping this entirely is harmless — a later reader resolves it.
    pub fn finalize_secondaries<R: Regions>(&self, regions: &R, commit_ts: u64) -> Result<()> {
        for m in &self.mutations {
            if m.key == self.primary {
                continue;
            }
            regions
                .engine(regions.region_of(&m.key))
                .resolve_commit(&m.key, self.start_ts, commit_ts)?;
        }
        Ok(())
    }

    /// Commit: primary (the linearization point) then secondaries.
    pub fn commit<R: Regions>(&self, regions: &R, commit_ts: u64) -> Result<()> {
        self.commit_primary(regions, commit_ts)?;
        self.finalize_secondaries(regions, commit_ts)
    }
}

/// A snapshot read at `read_ts` that resolves leftover locks **across regions**.
///
/// When the key it reads is locked, it follows the lock to the *primary's* region — which
/// may differ from the key's — asks that region for the transaction's [`TxnStatus`], and
/// rolls the local key forward (committed) or back (dead) before reading. A still-alive
/// primary lock surfaces as [`Error::KeyIsLocked`] (the caller waits and retries).
pub fn cross_get<R: Regions>(regions: &R, key: &[u8], read_ts: u64) -> Result<Option<Vec<u8>>> {
    let eng = regions.engine(regions.region_of(key));
    for _ in 0..8 {
        let Some(lock_bytes) = eng.get_cf_raw(Cf::Lock, key)? else {
            break;
        };
        let lock = Lock::decode(&lock_bytes).ok_or_else(|| Error::corruption("lock decode"))?;
        if lock.start_ts > read_ts {
            break; // a txn started after our snapshot — invisible to us
        }
        // Consult the PRIMARY's region for the txn's fate (this is the cross-region hop).
        let primary_status = regions
            .engine(regions.region_of(&lock.primary))
            .check_txn_status(&lock.primary, lock.start_ts, read_ts)?;
        match primary_status {
            TxnStatus::Committed(commit_ts) => eng.resolve_commit(key, lock.start_ts, commit_ts)?,
            TxnStatus::RolledBack => eng.resolve_rollback(key, lock.start_ts)?,
            TxnStatus::Locked { ttl } => {
                return Err(Error::KeyIsLocked(format!(
                    "key {key:?} awaits primary {:?} (ttl {ttl})",
                    lock.primary
                )));
            }
        }
    }
    // The lock on `key` is now resolved; read the committed version at our snapshot.
    eng.mvcc_get_unresolved(key, read_ts)
}
