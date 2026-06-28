//! Schema-stability smoke test: every wire message must encode→decode to itself. A
//! prost round-trip failure (or a drift in the generated types) trips this immediately.

use arcux_rpc::{kv, pd};
use prost::Message;

fn roundtrip<M: Message + Default + PartialEq + std::fmt::Debug>(m: &M) {
    let bytes = m.encode_to_vec();
    let back = M::decode(&bytes[..]).expect("decode");
    assert_eq!(m, &back, "round-trip mismatch");
}

#[test]
fn kv_messages_roundtrip() {
    roundtrip(&kv::BeginResponse { start_ts: 42 });

    roundtrip(&kv::PrewriteRequest {
        start_ts: 7,
        primary: b"p".to_vec(),
        mutations: vec![
            kv::Mutation { op: kv::Op::Put as i32, key: b"k".to_vec(), value: b"v".to_vec() },
            kv::Mutation { op: kv::Op::Delete as i32, key: b"d".to_vec(), value: vec![] },
        ],
        ttl: 100,
        context: Some(kv::Context { region_id: 4, region_epoch: 9 }),
    });

    roundtrip(&kv::CommitRequest {
        start_ts: 7,
        primary: b"p".to_vec(),
        keys: vec![b"p".to_vec(), b"s".to_vec()],
        context: None,
    });

    roundtrip(&kv::PutRequest {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        context: Some(kv::Context { region_id: 1, region_epoch: 1 }),
    });

    roundtrip(&kv::SplitRegionResponse {
        left: Some(kv::RegionInfo { id: 1, start_key: vec![], end_key: b"m".to_vec(), epoch: 2 }),
        right: Some(kv::RegionInfo { id: 2, start_key: b"m".to_vec(), end_key: vec![], epoch: 2 }),
    });

    roundtrip(&kv::GetResponse { found: true, value: b"v".to_vec(), error: None, read_ts: 9 });

    let conflict = kv::KeyError {
        kind: Some(kv::key_error::Kind::Conflict(kv::Conflict { detail: "boom".into() })),
    };
    roundtrip(&kv::CommitResponse { commit_ts: 0, error: Some(conflict) });

    let locked = kv::KeyError {
        kind: Some(kv::key_error::Kind::Locked(kv::Locked {
            primary: b"p".to_vec(),
            lock_ts: 3,
            ttl: 99,
            detail: "held".into(),
        })),
    };
    roundtrip(&kv::PrewriteResponse { errors: vec![locked] });

    let stale = kv::KeyError {
        kind: Some(kv::key_error::Kind::RegionStale(kv::RegionStale { new_epoch: 5 })),
    };
    roundtrip(&kv::PrewriteResponse { errors: vec![stale] });
}

#[test]
fn pd_messages_roundtrip() {
    roundtrip(&pd::GetTimestampResponse { timestamp: 1000, count: 16 });

    let regions = vec![
        pd::Region { id: 1, start_key: vec![], end_key: b"m".to_vec(), epoch: 2 },
        pd::Region { id: 2, start_key: b"m".to_vec(), end_key: vec![], epoch: 2 },
    ];
    roundtrip(&pd::ListRegionsResponse { regions: regions.clone() });
    roundtrip(&pd::HeartbeatRequest { node_id: 7, regions });
    roundtrip(&pd::GetRegionResponse {
        region_id: 2,
        start_key: b"m".to_vec(),
        end_key: vec![],
        epoch: 2,
    });
}

#[test]
fn version_is_pinned() {
    assert_eq!(arcux_rpc::VERSION, 2);
}
