//! Single-node Percolator transactions.
//!
//! A transaction is a set of [`Mutation`]s with one designated **primary** key
//! (`mutations[0]`). Commit is two-phase:
//!
//! * **Prewrite** every key (primary first): atomically conflict-check and, if clear,
//!   write the value to the Default CF at `start_ts` and a lock to the Lock CF. Any
//!   conflict aborts.
//! * **Commit**: write the **primary**'s Write record at `commit_ts` and drop its
//!   lock as a single atomic batch — *this is the transaction's linearization point*.
//!   Then lazily roll the secondaries forward. A crash after the primary commit but
//!   before the secondaries are finalized is harmless: a later reader completes them
//!   via lock resolution (see [`crate::mvcc`]).
//!
//! This is the Phase-1 single-node kernel; Phase 5 generalizes it across region
//! leaders (each mutation becoming a Raft proposal in the key's region).

use crate::batch::WriteBatch;
use crate::db::Engine;
use crate::error::{Error, Result};
use crate::keys::{encode_data_key, encode_write_value, Cf, Value};

/// One key/value mutation within a transaction.
#[derive(Debug, Clone)]
pub struct Mutation {
    pub key: Vec<u8>,
    pub value: Value,
}

impl Mutation {
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Mutation {
        Mutation { key: key.into(), value: Value::Put(value.into()) }
    }
    pub fn delete(key: impl Into<Vec<u8>>) -> Mutation {
        Mutation { key: key.into(), value: Value::Delete }
    }
}

pub struct Transaction<'a> {
    engine: &'a Engine,
    start_ts: u64,
    primary: Vec<u8>,
    mutations: Vec<Mutation>,
}

impl<'a> Transaction<'a> {
    /// Begin a transaction at `start_ts`. `mutations[0]` becomes the primary.
    pub fn new(engine: &'a Engine, start_ts: u64, mutations: Vec<Mutation>) -> Result<Transaction<'a>> {
        let Some(first) = mutations.first() else {
            return Err(Error::invalid("transaction needs at least one mutation"));
        };
        let primary = first.key.clone();
        Ok(Transaction { engine, start_ts, primary, mutations })
    }

    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }
    pub fn primary(&self) -> &[u8] {
        &self.primary
    }

    /// Phase 1: prewrite all keys (primary first). On the first conflict, rolls back
    /// this transaction's own partial locks (so they don't block others) and returns
    /// the error. (A *crash* mid-prewrite instead relies on TTL-based lock resolution.)
    pub fn prewrite(&self, ttl: u64) -> Result<()> {
        let mut acquired: Vec<Vec<u8>> = Vec::new();
        let mut result = Ok(());

        // Primary first, then the rest, so a reader that finds a secondary lock can
        // always follow it to a primary that is at least as far along.
        match self
            .engine
            .prewrite_one(&self.primary, &self.mutations[0].value, &self.primary, self.start_ts, ttl)
        {
            Ok(_) => acquired.push(self.primary.clone()),
            Err(e) => result = Err(e),
        }
        if result.is_ok() {
            for m in &self.mutations[1..] {
                match self.engine.prewrite_one(&m.key, &m.value, &self.primary, self.start_ts, ttl) {
                    Ok(_) => acquired.push(m.key.clone()),
                    Err(e) => {
                        result = Err(e);
                        break;
                    }
                }
            }
        }
        if result.is_err() {
            // Drop our own partial locks (conditionally — never touch a lock that a
            // concurrent txn may have since taken on the same key).
            for k in &acquired {
                let _ = self.engine.resolve_rollback(k, self.start_ts);
            }
        }
        result
    }

    /// Phase 2a: commit the primary — the atomic linearization point.
    pub fn commit_primary(&self, commit_ts: u64) -> Result<()> {
        let mut b = WriteBatch::new();
        b.put(
            Cf::Write,
            encode_data_key(&self.primary, commit_ts),
            encode_write_value(self.start_ts).to_vec(),
        );
        b.delete(Cf::Lock, self.primary.clone());
        self.engine.write(b)?;
        Ok(())
    }

    /// Phase 2b: roll the secondaries forward. Conditional (only acts if the lock is
    /// still ours), so it is safe against a reader that finalized the same secondary
    /// first and a third txn that then re-locked the key.
    pub fn finalize_secondaries(&self, commit_ts: u64) -> Result<()> {
        for m in &self.mutations {
            if m.key == self.primary {
                continue;
            }
            self.engine.resolve_commit(&m.key, self.start_ts, commit_ts)?;
        }
        Ok(())
    }

    /// Commit: primary then secondaries.
    pub fn commit(&self, commit_ts: u64) -> Result<()> {
        self.commit_primary(commit_ts)?;
        self.finalize_secondaries(commit_ts)
    }
}
