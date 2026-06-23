//! Column families and the MVCC key/value codecs (Percolator layout).
//!
//! Three column families, mirroring TiKV/Percolator:
//!
//! | CF      | key                         | value                      |
//! |---------|-----------------------------|----------------------------|
//! | Default | `user_key ‖ encode_ts(start_ts)` | [`Value`] (Put/Delete) |
//! | Write   | `user_key ‖ encode_ts(commit_ts)`| `start_ts` (u64 BE)    |
//! | Lock    | `user_key`                  | [`Lock`]                   |
//!
//! Default and Write keys embed a descending-encoded timestamp (see
//! [`crate::encoding`]) so a single forward seek finds the newest visible version.

use crate::encoding::{decode_ts, encode_ts, get_length_prefixed, put_length_prefixed, TS_LEN};

/// The three MVCC column families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Cf {
    /// `(user_key, start_ts) -> value`
    Default = 0,
    /// `(user_key, commit_ts) -> start_ts`
    Write = 1,
    /// `user_key -> Lock`
    Lock = 2,
}

impl Cf {
    #[inline]
    pub fn from_u8(v: u8) -> Option<Cf> {
        match v {
            0 => Some(Cf::Default),
            1 => Some(Cf::Write),
            2 => Some(Cf::Lock),
            _ => None,
        }
    }

    /// All column families, in id order (used to fan out flush/recovery).
    pub const ALL: [Cf; 3] = [Cf::Default, Cf::Write, Cf::Lock];
}

/// Build a Default/Write-CF storage key: `user_key ‖ encode_ts(ts)`.
pub fn encode_data_key(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(user_key.len() + TS_LEN);
    k.extend_from_slice(user_key);
    k.extend_from_slice(&encode_ts(ts));
    k
}

/// Split a Default/Write-CF storage key back into `(user_key, ts)`.
pub fn decode_data_key(key: &[u8]) -> Option<(&[u8], u64)> {
    if key.len() < TS_LEN {
        return None;
    }
    let split = key.len() - TS_LEN;
    let (uk, ts_bytes) = key.split_at(split);
    Some((uk, decode_ts(ts_bytes)))
}

/// Write-CF value is just the `start_ts` pointer into the Default CF.
#[inline]
pub fn encode_write_value(start_ts: u64) -> [u8; 8] {
    start_ts.to_be_bytes()
}

#[inline]
pub fn decode_write_value(bytes: &[u8]) -> Option<u64> {
    let arr: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(arr))
}

/// A stored Default-CF value: a real value or a deletion tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Put(Vec<u8>),
    Delete,
}

const VTAG_PUT: u8 = 0;
const VTAG_DELETE: u8 = 1;

impl Value {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Value::Put(v) => {
                let mut out = Vec::with_capacity(1 + v.len());
                out.push(VTAG_PUT);
                out.extend_from_slice(v);
                out
            }
            Value::Delete => vec![VTAG_DELETE],
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<Value> {
        match bytes.split_first() {
            Some((&VTAG_PUT, rest)) => Some(Value::Put(rest.to_vec())),
            Some((&VTAG_DELETE, _)) => Some(Value::Delete),
            _ => None,
        }
    }

    #[inline]
    pub fn is_delete(&self) -> bool {
        matches!(self, Value::Delete)
    }

    /// Collapse to an optional value, treating a tombstone as "absent".
    #[inline]
    pub fn into_option(self) -> Option<Vec<u8>> {
        match self {
            Value::Put(v) => Some(v),
            Value::Delete => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockKind {
    Put = 0,
    Delete = 1,
}

/// A Percolator lock, living in the Lock CF keyed by `user_key`.
///
/// Secondary locks point at the transaction's `primary` key; the primary lock
/// points at itself. `ttl` is an absolute expiry in the same timestamp domain as
/// `start_ts` (single-node v1); a reader past it may roll the lock back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lock {
    pub primary: Vec<u8>,
    pub start_ts: u64,
    pub ttl: u64,
    pub kind: LockKind,
}

impl Lock {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8 + 8 + 4 + self.primary.len());
        out.push(self.kind as u8);
        out.extend_from_slice(&self.start_ts.to_be_bytes());
        out.extend_from_slice(&self.ttl.to_be_bytes());
        put_length_prefixed(&mut out, &self.primary);
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Lock> {
        if bytes.len() < 1 + 8 + 8 {
            return None;
        }
        let kind = match bytes[0] {
            0 => LockKind::Put,
            1 => LockKind::Delete,
            _ => return None,
        };
        let start_ts = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        let ttl = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
        let mut pos = 17;
        let primary = get_length_prefixed(bytes, &mut pos)?.to_vec();
        Some(Lock { primary, start_ts, ttl, kind })
    }

    /// Whether this lock belongs to its own transaction's primary key.
    #[inline]
    pub fn is_primary(&self, this_key: &[u8]) -> bool {
        self.primary == this_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_key_roundtrip() {
        let k = encode_data_key(b"account/alice", 1234);
        let (uk, ts) = decode_data_key(&k).unwrap();
        assert_eq!(uk, b"account/alice");
        assert_eq!(ts, 1234);
    }

    #[test]
    fn data_key_versions_sort_newest_first() {
        let older = encode_data_key(b"k", 5);
        let newer = encode_data_key(b"k", 9);
        assert!(newer < older, "newer version must sort before older");
    }

    #[test]
    fn value_roundtrip() {
        assert_eq!(Value::decode(&Value::Put(b"v".to_vec()).encode()), Some(Value::Put(b"v".to_vec())));
        assert_eq!(Value::decode(&Value::Delete.encode()), Some(Value::Delete));
        assert_eq!(Value::decode(&[]), None);
    }

    #[test]
    fn write_value_roundtrip() {
        assert_eq!(decode_write_value(&encode_write_value(777)), Some(777));
        assert_eq!(decode_write_value(&[1, 2, 3]), None);
    }

    #[test]
    fn lock_roundtrip() {
        let lock = Lock {
            primary: b"primary-key".to_vec(),
            start_ts: 100,
            ttl: 250,
            kind: LockKind::Put,
        };
        assert_eq!(Lock::decode(&lock.encode()), Some(lock.clone()));
        assert!(lock.is_primary(b"primary-key"));
        assert!(!lock.is_primary(b"secondary-key"));
    }

    #[test]
    fn cf_id_roundtrip() {
        for cf in Cf::ALL {
            assert_eq!(Cf::from_u8(cf as u8), Some(cf));
        }
        assert_eq!(Cf::from_u8(99), None);
    }
}
