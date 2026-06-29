//! The replicated command model — what a Raft log entry carries.
//!
//! Phase 4b replicated only a pre-built committed `WriteBatch` (autocommit). Phase 4b+
//! replicates the **Percolator step itself** so the conflict-check runs at *apply* time,
//! deterministically on every replica (they all apply the same log prefix against identical
//! state). A log entry is therefore one of:
//!
//! - [`Command::Autocommit`] — the 4b fast path: a finished single-key write, applied as-is;
//! - [`Command::Prewrite`] — conflict-check + lock + stage the value (the first Percolator
//!   phase), run via [`arcux_engine::Transaction::prewrite`] at apply;
//! - [`Command::Commit`] — write the commit record + drop the lock (the second phase), run
//!   via [`arcux_engine::Transaction::commit`] at apply.
//!
//! An empty entry (the election no-op) decodes to nothing and applies to nothing.

use arcux_engine::encoding::{get_length_prefixed, put_length_prefixed};
use arcux_engine::{Mutation, Value, WriteBatch};

/// A command carried by one Raft log entry.
pub enum Command {
    /// A finished single-key write (autocommit): the committed `WriteBatch` applied directly.
    Autocommit(WriteBatch),
    /// Percolator phase 1: prewrite every mutation (primary first) at `start_ts`.
    Prewrite { mutations: Vec<Mutation>, primary: Vec<u8>, start_ts: u64, ttl: u64 },
    /// Percolator phase 2: commit the transaction at `commit_ts` (allocated by the leader,
    /// so every replica commits at the same timestamp).
    Commit { primary: Vec<u8>, keys: Vec<Vec<u8>>, start_ts: u64, commit_ts: u64 },
}

const TAG_AUTOCOMMIT: u8 = 0;
const TAG_PREWRITE: u8 = 1;
const TAG_COMMIT: u8 = 2;

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Command::Autocommit(batch) => {
                buf.push(TAG_AUTOCOMMIT);
                buf.extend_from_slice(&batch.encode());
            }
            Command::Prewrite { mutations, primary, start_ts, ttl } => {
                buf.push(TAG_PREWRITE);
                buf.extend_from_slice(&start_ts.to_be_bytes());
                buf.extend_from_slice(&ttl.to_be_bytes());
                put_length_prefixed(&mut buf, primary);
                buf.extend_from_slice(&(mutations.len() as u32).to_be_bytes());
                for m in mutations {
                    put_length_prefixed(&mut buf, &m.key);
                    put_length_prefixed(&mut buf, &m.value.encode());
                }
            }
            Command::Commit { primary, keys, start_ts, commit_ts } => {
                buf.push(TAG_COMMIT);
                buf.extend_from_slice(&start_ts.to_be_bytes());
                buf.extend_from_slice(&commit_ts.to_be_bytes());
                put_length_prefixed(&mut buf, primary);
                buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());
                for k in keys {
                    put_length_prefixed(&mut buf, k);
                }
            }
        }
        buf
    }

    /// Inverse of [`encode`](Self::encode); `None` on malformed input.
    pub fn decode(buf: &[u8]) -> Option<Command> {
        let (&tag, rest) = buf.split_first()?;
        let mut pos = 0;
        match tag {
            TAG_AUTOCOMMIT => Some(Command::Autocommit(WriteBatch::decode(rest)?)),
            TAG_PREWRITE => {
                let start_ts = get_u64(rest, &mut pos)?;
                let ttl = get_u64(rest, &mut pos)?;
                let primary = get_length_prefixed(rest, &mut pos)?.to_vec();
                let count = get_u32(rest, &mut pos)? as usize;
                let mut mutations = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = get_length_prefixed(rest, &mut pos)?.to_vec();
                    let value = Value::decode(get_length_prefixed(rest, &mut pos)?)?;
                    mutations.push(Mutation { key, value });
                }
                Some(Command::Prewrite { mutations, primary, start_ts, ttl })
            }
            TAG_COMMIT => {
                let start_ts = get_u64(rest, &mut pos)?;
                let commit_ts = get_u64(rest, &mut pos)?;
                let primary = get_length_prefixed(rest, &mut pos)?.to_vec();
                let count = get_u32(rest, &mut pos)? as usize;
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(get_length_prefixed(rest, &mut pos)?.to_vec());
                }
                Some(Command::Commit { primary, keys, start_ts, commit_ts })
            }
            _ => None,
        }
    }
}

fn get_u64(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let end = pos.checked_add(8)?;
    let v = u64::from_be_bytes(buf.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(v)
}

fn get_u32(buf: &[u8], pos: &mut usize) -> Option<u32> {
    let end = pos.checked_add(4)?;
    let v = u32::from_be_bytes(buf.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arcux_engine::Cf;

    /// Round-trip via re-encode (the engine `Mutation`/`WriteBatch` don't impl `PartialEq`
    /// uniformly, so compare the encodings).
    fn assert_round_trips(cmd: Command) {
        let bytes = cmd.encode();
        let decoded = Command::decode(&bytes).expect("decode");
        assert_eq!(decoded.encode(), bytes, "command must round-trip");
    }

    #[test]
    fn autocommit_round_trips() {
        let mut b = WriteBatch::new();
        b.put(Cf::Default, b"k".to_vec(), b"v".to_vec());
        b.put(Cf::Write, b"k".to_vec(), 7u64.to_be_bytes().to_vec());
        assert_round_trips(Command::Autocommit(b));
    }

    #[test]
    fn prewrite_round_trips() {
        assert_round_trips(Command::Prewrite {
            mutations: vec![Mutation::put(b"a".to_vec(), b"1".to_vec()), Mutation::delete(b"b".to_vec())],
            primary: b"a".to_vec(),
            start_ts: 42,
            ttl: 1_000,
        });
    }

    #[test]
    fn commit_round_trips() {
        assert_round_trips(Command::Commit {
            primary: b"a".to_vec(),
            keys: vec![b"a".to_vec(), b"b".to_vec()],
            start_ts: 42,
            commit_ts: 50,
        });
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(Command::decode(&[]).is_none());
        assert!(Command::decode(&[9, 9, 9]).is_none()); // unknown tag
    }
}
