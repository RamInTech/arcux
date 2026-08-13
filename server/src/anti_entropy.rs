//! AP anti-entropy — the reconcile core.
//!
//! The AP write path is leaderless and W=1: a write is acked after the *local* apply, then
//! fanned out best-effort to peers ([`crate::ap`]). That fan-out only reaches replicas that are
//! up *at write time* — a peer that was partitioned or crashed never receives the write, and
//! nothing in the write path re-delivers it. Anti-entropy is the background process that removes
//! this drift: replicas periodically compare their data and heal the differences, making AP
//! convergent after a partition heals, not just available during one.
//!
//! ## Comparison strategy
//!
//! Each replica summarizes its data with a Merkle tree over [`LEAVES`] buckets:
//!
//! * every key is bucketed by a hash of the key only, so both replicas bucket it identically;
//! * a leaf's hash folds in every `(key, version)` it contains, so it changes iff any key in it
//!   has a different latest version between replicas;
//! * internal nodes hash their children up to a single root.
//!
//! Replicas exchange the compact leaf hashes, [`diff`] descends the tree to find the differing
//! leaves, and only those leaves' entries are fetched and merged. The merge rule is
//! Last-Writer-Wins on the HLC ([`to_adopt`]): adopt a peer's version when it's newer than (or
//! missing from) our own — the same rule AP reads already use, so no new conflict resolver is
//! needed, only delivery of the winning version.
//!
//! This module is the transport-free core: it operates on `Vec<Version>` and hashes, with no
//! engine or gRPC dependency, so reconcile decisions are deterministically testable. The
//! `server` integration reads `Version`s via [`arcux_engine::Engine::scan_versions`] and moves
//! digests/entries over the `ApDigest` / `ApFetch` RPCs.

use std::collections::HashMap;

/// Number of Merkle leaves (buckets). A power of two so the tree is perfectly balanced;
/// larger ⇒ finer buckets ⇒ less data re-fetched per differing key, at a bigger digest.
pub const LEAVES: usize = 256;

/// One key's latest AP version on a replica: the value (or a tombstone) at an HLC `version`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub key: Vec<u8>,
    /// The HLC stamp of this version — the Last-Writer-Wins rank.
    pub version: u64,
    pub value: Vec<u8>,
    pub is_delete: bool,
}

/// The compact per-leaf digest of a replica's `[start, end)` state: one hash per bucket.
/// Exchanged over the wire; both replicas build a [`MerkleTree`] from it to compare.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Digest {
    pub leaves: Vec<u64>,
}

/// Which leaf a key falls in — a stable hash of the **key only** (never the version), so the
/// same key lands in the same bucket on every replica.
pub fn bucket_of(key: &[u8]) -> usize {
    (fnv1a(key) as usize) % LEAVES
}

/// Build the per-leaf digest of `versions`: each leaf's hash folds in the `(key, version)` of
/// every key bucketed there, in a canonical (key-sorted) order so it's replica-independent.
pub fn digest(versions: &[Version]) -> Digest {
    let mut buckets: Vec<Vec<(&[u8], u64)>> = vec![Vec::new(); LEAVES];
    for v in versions {
        buckets[bucket_of(&v.key)].push((&v.key, v.version));
    }
    let leaves = buckets
        .into_iter()
        .map(|mut entries| {
            entries.sort_unstable();
            let mut h = FNV_OFFSET;
            for (k, ver) in entries {
                h = fnv1a_step(h, k);
                h = fnv1a_step(h, &ver.to_be_bytes());
            }
            h
        })
        .collect();
    Digest { leaves }
}

/// A balanced binary Merkle tree over [`LEAVES`] leaf hashes, stored as a heap array
/// (`nodes[1]` is the root; node `i`'s children are `2i`, `2i+1`).
pub struct MerkleTree {
    nodes: Vec<u64>,
}

impl MerkleTree {
    pub fn from_digest(d: &Digest) -> MerkleTree {
        debug_assert_eq!(d.leaves.len(), LEAVES, "digest must have LEAVES leaves");
        let mut nodes = vec![0u64; 2 * LEAVES];
        nodes[LEAVES..(2 * LEAVES)].copy_from_slice(&d.leaves);
        for i in (1..LEAVES).rev() {
            nodes[i] = mix(nodes[2 * i], nodes[2 * i + 1]);
        }
        MerkleTree { nodes }
    }

    pub fn root(&self) -> u64 {
        self.nodes[1]
    }

    /// Descend from the root collecting the leaf indices whose hashes differ from `other`'s —
    /// pruning any subtree whose hashes already match (the whole point of the tree). The result
    /// is the exact set of buckets that need their entries reconciled.
    fn differing_leaves(&self, other: &MerkleTree) -> Vec<usize> {
        let mut out = Vec::new();
        let mut stack = vec![1usize];
        while let Some(i) = stack.pop() {
            if self.nodes[i] == other.nodes[i] {
                continue;
            }
            if i >= LEAVES {
                out.push(i - LEAVES);
            } else {
                stack.push(2 * i);
                stack.push(2 * i + 1);
            }
        }
        out.sort_unstable();
        out
    }
}

/// The leaf buckets whose contents differ between two replicas' digests (via a Merkle
/// descent). Empty ⇒ the replicas already agree on this range and nothing need be fetched.
pub fn diff(local: &Digest, remote: &Digest) -> Vec<usize> {
    MerkleTree::from_digest(local).differing_leaves(&MerkleTree::from_digest(remote))
}

/// Last-Writer-Wins merge: of the `remote` entries, the ones this replica should **adopt** —
/// a key we lack, or one where the remote's HLC `version` is strictly newer than ours. Equal
/// versions are the same write (HLCs are unique per write), so they're skipped. `local` maps
/// our keys to their current HLC version.
pub fn to_adopt(local: &HashMap<Vec<u8>, u64>, remote: &[Version]) -> Vec<Version> {
    remote
        .iter()
        .filter(|r| local.get(&r.key).is_none_or(|&mine| r.version > mine))
        .cloned()
        .collect()
}

// ---- hashing (FNV-1a — small, dependency-free, deterministic) --------------------------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_step(FNV_OFFSET, bytes)
}

fn fnv1a_step(mut h: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    // A length terminator so `[a][b]` and `[ab]` (concatenated fields) can't collide.
    h ^= 0xff;
    h.wrapping_mul(FNV_PRIME)
}

/// Combine two child hashes into a parent hash.
fn mix(a: u64, b: u64) -> u64 {
    let mut h = fnv1a_step(FNV_OFFSET, &a.to_be_bytes());
    h = fnv1a_step(h, &b.to_be_bytes());
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(key: &[u8], version: u64) -> Version {
        Version { key: key.to_vec(), version, value: b"x".to_vec(), is_delete: false }
    }

    fn map(versions: &[Version]) -> HashMap<Vec<u8>, u64> {
        versions.iter().map(|x| (x.key.clone(), x.version)).collect()
    }

    #[test]
    fn identical_replicas_have_no_diff() {
        let a = vec![v(b"k1", 10), v(b"k2", 20), v(b"post/99", 30)];
        let b = a.clone();
        assert_eq!(diff(&digest(&a), &digest(&b)), Vec::<usize>::new());
        assert_eq!(MerkleTree::from_digest(&digest(&a)).root(), MerkleTree::from_digest(&digest(&b)).root());
    }

    #[test]
    fn a_newer_version_shows_up_as_a_differing_leaf() {
        let a = vec![v(b"k1", 10), v(b"k2", 20)];
        let b = vec![v(b"k1", 10), v(b"k2", 99)]; // k2 newer on b
        let d = diff(&digest(&a), &digest(&b));
        assert_eq!(d, vec![bucket_of(b"k2")], "only k2's bucket differs");
    }

    #[test]
    fn a_missing_key_shows_up_as_a_differing_leaf() {
        let a = vec![v(b"k1", 10)];
        let b = vec![v(b"k1", 10), v(b"k2", 20)]; // b has an extra key
        assert_eq!(diff(&digest(&a), &digest(&b)), vec![bucket_of(b"k2")]);
    }

    #[test]
    fn to_adopt_takes_newer_and_missing_only() {
        let local = vec![v(b"a", 10), v(b"b", 50), v(b"c", 30)];
        let remote = vec![
            v(b"a", 10),  // same → skip
            v(b"b", 20),  // older than ours → skip
            v(b"c", 99),  // newer → adopt
            v(b"d", 5),   // missing locally → adopt
        ];
        let adopt = to_adopt(&map(&local), &remote);
        let keys: Vec<&[u8]> = adopt.iter().map(|x| x.key.as_slice()).collect();
        assert_eq!(keys, vec![b"c".as_slice(), b"d".as_slice()]);
    }

    #[test]
    fn tombstone_can_win_by_lww() {
        // A delete with a higher HLC than our live value must be adopted (it's the last write).
        let local = map(&[v(b"k", 10)]);
        let remote = vec![Version { key: b"k".to_vec(), version: 20, value: vec![], is_delete: true }];
        let adopt = to_adopt(&local, &remote);
        assert_eq!(adopt.len(), 1);
        assert!(adopt[0].is_delete && adopt[0].version == 20);
    }

    #[test]
    fn merkle_descent_returns_only_the_changed_buckets() {
        // Many identical keys + a few changed ones: the descent must pinpoint exactly the
        // changed buckets and prune everything else.
        let mut a: Vec<Version> = (0..1000u64).map(|i| v(format!("key{i}").as_bytes(), i)).collect();
        let mut b = a.clone();
        // Bump three keys on b.
        for i in [3u64, 500, 777] {
            let idx = b.iter().position(|x| x.key == format!("key{i}").as_bytes()).unwrap();
            b[idx].version += 1_000;
        }
        let mut expected: Vec<usize> =
            [3u64, 500, 777].iter().map(|i| bucket_of(format!("key{i}").as_bytes())).collect();
        expected.sort_unstable();
        expected.dedup();
        let mut got = diff(&digest(&a), &digest(&b));
        got.dedup();
        // Every changed key's bucket is reported (and only changed buckets).
        for e in &expected {
            assert!(got.contains(e), "bucket {e} should be flagged");
        }
        assert!(got.len() <= expected.len(), "no spurious buckets flagged");
        let _ = (&mut a, &mut b);
    }
}
