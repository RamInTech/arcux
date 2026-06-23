//! Property tests for the MVCC encoders. Gated behind the `proptests` feature:
//!   cargo test --features proptests
#![cfg(feature = "proptests")]

use proptest::prelude::*;
use arcux_engine::encoding::{decode_ts, encode_ts};
use arcux_engine::keys::{decode_data_key, encode_data_key};

proptest! {
    #[test]
    fn ts_roundtrip(ts in any::<u64>()) {
        prop_assert_eq!(decode_ts(&encode_ts(ts)), ts);
    }

    /// Newer (larger) timestamps must produce lexicographically smaller encodings.
    #[test]
    fn ts_descending_order(a in any::<u64>(), b in any::<u64>()) {
        let (ea, eb) = (encode_ts(a), encode_ts(b));
        match a.cmp(&b) {
            std::cmp::Ordering::Greater => prop_assert!(ea < eb),
            std::cmp::Ordering::Less => prop_assert!(ea > eb),
            std::cmp::Ordering::Equal => prop_assert_eq!(ea, eb),
        }
    }

    #[test]
    fn data_key_roundtrip(
        uk in proptest::collection::vec(any::<u8>(), 0..32),
        ts in any::<u64>(),
    ) {
        let k = encode_data_key(&uk, ts);
        let (duk, dts) = decode_data_key(&k).unwrap();
        prop_assert_eq!(duk, &uk[..]);
        prop_assert_eq!(dts, ts);
    }

    /// For a fixed user key, a newer ts must sort before an older ts.
    #[test]
    fn data_key_version_order(
        uk in proptest::collection::vec(any::<u8>(), 1..16),
        a in any::<u64>(),
        b in any::<u64>(),
    ) {
        let (ka, kb) = (encode_data_key(&uk, a), encode_data_key(&uk, b));
        match a.cmp(&b) {
            std::cmp::Ordering::Greater => prop_assert!(ka < kb),
            std::cmp::Ordering::Less => prop_assert!(ka > kb),
            std::cmp::Ordering::Equal => prop_assert_eq!(ka, kb),
        }
    }
}
