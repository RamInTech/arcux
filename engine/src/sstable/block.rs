//! Data-block entry codec.
//!
//! A data block is a run of `[klen:u32][key][vlen:u32][value]` entries in ascending
//! key order. Keys here are *storage keys* (`cf_id ‖ cf_key`) and values are
//! [`crate::memtable::MemValue`] encodings. Prefix compression is deliberately
//! omitted in the Phase-1 slice (a space optimization, deferred with bloom filters
//! and the block cache); blocks are small, so a linear in-block scan is cheap.

use crate::encoding::{get_length_prefixed, put_length_prefixed};

/// Append one entry to an in-progress block buffer.
pub fn encode_entry(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    put_length_prefixed(buf, key);
    put_length_prefixed(buf, value);
}

/// Iterate `(key, value)` over a decoded (CRC already stripped) data block.
pub fn entries(block: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    let mut pos = 0usize;
    std::iter::from_fn(move || {
        let k = get_length_prefixed(block, &mut pos)?;
        let v = get_length_prefixed(block, &mut pos)?;
        Some((k, v))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_entries_roundtrip() {
        let mut buf = Vec::new();
        encode_entry(&mut buf, b"\x00aaa", b"\x00v1");
        encode_entry(&mut buf, b"\x00bbb", b"\x01"); // tombstone-encoded value
        let got: Vec<_> = entries(&buf).map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        assert_eq!(got, vec![
            (b"\x00aaa".to_vec(), b"\x00v1".to_vec()),
            (b"\x00bbb".to_vec(), b"\x01".to_vec()),
        ]);
    }
}
