//! Low-level encoding helpers shared by the WAL, SSTable, and key codecs.
//!
//! The most important convention here is **descending timestamp encoding**: a
//! timestamp `ts` is stored as `u64::MAX - ts` in big-endian byte order. Because
//! big-endian bytes compare lexicographically the same way the underlying integer
//! compares, taking the complement makes a *larger* `ts` sort *earlier*. So for a
//! fixed user key the newest version is the first key in ascending byte order, and
//! "latest visible at `read_ts`" is a single forward seek to `encode_ts(read_ts)`.

pub const TS_LEN: usize = 8;

/// Encode a timestamp so newer (larger) timestamps sort first.
#[inline]
pub fn encode_ts(ts: u64) -> [u8; TS_LEN] {
    (u64::MAX - ts).to_be_bytes()
}

/// Inverse of [`encode_ts`].
#[inline]
pub fn decode_ts(bytes: &[u8]) -> u64 {
    debug_assert_eq!(bytes.len(), TS_LEN);
    let mut buf = [0u8; TS_LEN];
    buf.copy_from_slice(bytes);
    u64::MAX - u64::from_be_bytes(buf)
}

/// Append a `u32`-length-prefixed byte slice to `buf`.
#[inline]
pub fn put_length_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(bytes);
}

/// Read a `u32`-length-prefixed byte slice starting at `*pos`, advancing `*pos`.
/// Returns `None` on truncation (used to detect torn WAL/SSTable tails).
pub fn get_length_prefixed<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let start = *pos;
    if start + 4 > buf.len() {
        return None;
    }
    let len = u32::from_be_bytes(buf[start..start + 4].try_into().unwrap()) as usize;
    let data_start = start + 4;
    let data_end = data_start.checked_add(len)?;
    if data_end > buf.len() {
        return None;
    }
    *pos = data_end;
    Some(&buf[data_start..data_end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_roundtrip_and_bounds() {
        for ts in [0u64, 1, 42, u64::MAX - 1, u64::MAX] {
            assert_eq!(decode_ts(&encode_ts(ts)), ts);
        }
    }

    #[test]
    fn newer_ts_sorts_first() {
        // ts=10 is newer than ts=5, so its encoding must be lexicographically smaller.
        assert!(encode_ts(10) < encode_ts(5));
        assert!(encode_ts(u64::MAX) < encode_ts(0));
    }

    #[test]
    fn length_prefixed_roundtrip() {
        let mut buf = Vec::new();
        put_length_prefixed(&mut buf, b"hello");
        put_length_prefixed(&mut buf, b"");
        put_length_prefixed(&mut buf, b"world!");
        let mut pos = 0;
        assert_eq!(get_length_prefixed(&buf, &mut pos), Some(&b"hello"[..]));
        assert_eq!(get_length_prefixed(&buf, &mut pos), Some(&b""[..]));
        assert_eq!(get_length_prefixed(&buf, &mut pos), Some(&b"world!"[..]));
        assert_eq!(get_length_prefixed(&buf, &mut pos), None); // exhausted
    }

    #[test]
    fn length_prefixed_detects_truncation() {
        let mut buf = Vec::new();
        put_length_prefixed(&mut buf, b"abcdef");
        buf.truncate(buf.len() - 2); // chop the tail
        let mut pos = 0;
        assert_eq!(get_length_prefixed(&buf, &mut pos), None);
    }
}
