//! [`WriteBatch`] — an atomic group of mutations across column families.
//!
//! A batch is the unit of atomicity in the engine: it is written to the WAL as a
//! single framed record and applied to the memtable all-or-nothing. Percolator
//! relies on this — e.g. "write the primary's Write-CF record AND erase its lock"
//! is one batch, so a crash can never leave the commit half-applied.

use crate::encoding::{get_length_prefixed, put_length_prefixed};
use crate::keys::Cf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put { cf: Cf, key: Vec<u8>, value: Vec<u8> },
    Delete { cf: Cf, key: Vec<u8> },
}

impl WriteOp {
    #[inline]
    pub fn cf(&self) -> Cf {
        match self {
            WriteOp::Put { cf, .. } | WriteOp::Delete { cf, .. } => *cf,
        }
    }
    #[inline]
    pub fn key(&self) -> &[u8] {
        match self {
            WriteOp::Put { key, .. } | WriteOp::Delete { key, .. } => key,
        }
    }
    fn approx_size(&self) -> usize {
        match self {
            WriteOp::Put { key, value, .. } => key.len() + value.len() + 16,
            WriteOp::Delete { key, .. } => key.len() + 16,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    pub ops: Vec<WriteOp>,
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch::default()
    }

    pub fn put(&mut self, cf: Cf, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.ops.push(WriteOp::Put { cf, key: key.into(), value: value.into() });
    }

    pub fn delete(&mut self, cf: Cf, key: impl Into<Vec<u8>>) {
        self.ops.push(WriteOp::Delete { cf, key: key.into() });
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
    pub fn len(&self) -> usize {
        self.ops.len()
    }
    /// Rough in-memory footprint, used to drive memtable freeze decisions.
    pub fn approx_size(&self) -> usize {
        self.ops.iter().map(WriteOp::approx_size).sum()
    }

    /// Serialize the batch body (no seq, no frame): `[count:u32]` then per op
    /// `[op:u8][cf:u8][klen:u32][key]([vlen:u32][value] for Put)`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.ops.len() as u32).to_be_bytes());
        for op in &self.ops {
            match op {
                WriteOp::Put { cf, key, value } => {
                    buf.push(0);
                    buf.push(*cf as u8);
                    put_length_prefixed(&mut buf, key);
                    put_length_prefixed(&mut buf, value);
                }
                WriteOp::Delete { cf, key } => {
                    buf.push(1);
                    buf.push(*cf as u8);
                    put_length_prefixed(&mut buf, key);
                }
            }
        }
        buf
    }

    /// Inverse of [`WriteBatch::encode`]. Returns `None` on malformed input.
    pub fn decode(buf: &[u8]) -> Option<WriteBatch> {
        if buf.len() < 4 {
            return None;
        }
        let count = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            let op_tag = *buf.get(pos)?;
            pos += 1;
            let cf = Cf::from_u8(*buf.get(pos)?)?;
            pos += 1;
            let key = get_length_prefixed(buf, &mut pos)?.to_vec();
            match op_tag {
                0 => {
                    let value = get_length_prefixed(buf, &mut pos)?.to_vec();
                    ops.push(WriteOp::Put { cf, key, value });
                }
                1 => ops.push(WriteOp::Delete { cf, key }),
                _ => return None,
            }
        }
        Some(WriteBatch { ops })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_roundtrip() {
        let mut b = WriteBatch::new();
        b.put(Cf::Default, b"k1".to_vec(), b"v1".to_vec());
        b.delete(Cf::Lock, b"k1".to_vec());
        b.put(Cf::Write, b"k1".to_vec(), 42u64.to_be_bytes().to_vec());
        assert_eq!(b.len(), 3);
        let decoded = WriteBatch::decode(&b.encode()).unwrap();
        assert_eq!(decoded, b);
    }

    #[test]
    fn empty_batch_roundtrip() {
        let b = WriteBatch::new();
        assert!(b.is_empty());
        assert_eq!(WriteBatch::decode(&b.encode()), Some(b));
    }

    #[test]
    fn decode_rejects_garbage() {
        assert_eq!(WriteBatch::decode(&[]), None);
        assert_eq!(WriteBatch::decode(&[0, 0, 0, 5]), None); // claims 5 ops, has none
    }
}
