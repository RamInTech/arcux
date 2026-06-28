//! Regions: the unit of range-sharding, and the registry that owns them.
//!
//! A **region** owns a half-open key range `[start, end)` of the user keyspace (an
//! empty `start` is −∞, an empty `end` is +∞), identified by a stable `id` and carrying
//! an `epoch` (a monotonic version, bumped whenever the range changes). A fresh cluster
//! is one region covering the whole keyspace; [`RegionRegistry::split`] cuts a region in
//! two and bumps the epoch of both halves.
//!
//! The `epoch` is what makes stale routing detectable: a client caches a region's
//! `(id, epoch)` and stamps it on every request; once a split bumps the epoch, the
//! authoritative holder rejects the old epoch with `RegionStale`, and the client knows
//! to re-route. (Phase 4 makes a split a Raft command on the region leader; the epoch
//! contract here is unchanged by that.)

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::persist::{atomic_write, get_bytes, get_u32, get_u64, put_bytes, read_optional};

const REGIONS_FILE: &str = "regions";

/// A contiguous, epoch-versioned shard of the keyspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    pub id: u64,
    /// Inclusive lower bound; empty ⇒ −∞ (the start of the keyspace).
    pub start: Vec<u8>,
    /// Exclusive upper bound; empty ⇒ +∞ (the end of the keyspace).
    pub end: Vec<u8>,
    /// Monotonic version, bumped on every range change (split/merge).
    pub epoch: u64,
}

impl Region {
    /// Whether `key` falls in this region's half-open range.
    pub fn contains(&self, key: &[u8]) -> bool {
        // An empty `start` is −∞: every key is `>=` the empty slice, so the lower
        // bound needs no special case. An empty `end` is +∞.
        key >= self.start.as_slice() && (self.end.is_empty() || key < self.end.as_slice())
    }
}

struct State {
    /// Regions, kept sorted by `start` so routing is a simple scan and the ranges are
    /// guaranteed contiguous and non-overlapping.
    regions: Vec<Region>,
    /// Next region id to assign.
    next_id: u64,
}

/// The owner of a set of regions: a data node's authoritative table (with `split` +
/// persistence), or the PD's aggregated view of what nodes report (via `replace`).
pub struct RegionRegistry {
    state: Mutex<State>,
    /// Persistence target; `None` ⇒ in-memory only (PD's aggregated view, or tests).
    path: Option<PathBuf>,
}

impl RegionRegistry {
    /// An in-memory registry seeded with a single region covering the whole keyspace.
    pub fn in_memory() -> RegionRegistry {
        RegionRegistry {
            state: Mutex::new(State { regions: vec![whole_keyspace(1)], next_id: 2 }),
            path: None,
        }
    }

    /// An empty in-memory registry (PD's view before any node has reported in).
    pub fn empty() -> RegionRegistry {
        RegionRegistry { state: Mutex::new(State { regions: Vec::new(), next_id: 1 }), path: None }
    }

    /// Open a persistent registry under `dir`, loading the saved region table or, on
    /// first start, bootstrapping a single whole-keyspace region.
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<RegionRegistry> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join(REGIONS_FILE);
        let state = match read_optional(&path)? {
            Some(bytes) => decode_state(&bytes)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "region table corrupt"))?,
            None => State { regions: vec![whole_keyspace(1)], next_id: 2 },
        };
        let reg = RegionRegistry { state: Mutex::new(state), path: Some(path) };
        reg.persist()?; // materialize the bootstrap table on first start
        Ok(reg)
    }

    /// The region owning `key`, if the keyspace is covered here.
    pub fn route(&self, key: &[u8]) -> Option<Region> {
        let g = self.state.lock().expect("registry poisoned");
        g.regions.iter().find(|r| r.contains(key)).cloned()
    }

    /// Look up a region by id (used by the authoritative epoch check).
    pub fn by_id(&self, id: u64) -> Option<Region> {
        let g = self.state.lock().expect("registry poisoned");
        g.regions.iter().find(|r| r.id == id).cloned()
    }

    /// All regions, sorted by start key.
    pub fn list(&self) -> Vec<Region> {
        self.state.lock().expect("registry poisoned").regions.clone()
    }

    /// Split the region containing `split_key` at that key, bumping the epoch of both
    /// halves and assigning a fresh id to the right half. Returns `(left, right)`.
    /// Errors if `split_key` is not strictly inside some region (an empty or
    /// at-the-boundary key cannot split anything).
    pub fn split(&self, split_key: &[u8]) -> std::io::Result<(Region, Region)> {
        let result = {
            let mut g = self.state.lock().expect("registry poisoned");
            let Some(idx) = g.regions.iter().position(|r| r.contains(split_key)) else {
                return Err(invalid("split_key is outside every region"));
            };
            let target = g.regions[idx].clone();
            if split_key <= target.start.as_slice() {
                return Err(invalid("split_key must be strictly greater than the region start"));
            }
            let new_epoch = target.epoch + 1;
            let right_id = g.next_id;
            g.next_id += 1;

            let left = Region {
                id: target.id,
                start: target.start.clone(),
                end: split_key.to_vec(),
                epoch: new_epoch,
            };
            let right = Region {
                id: right_id,
                start: split_key.to_vec(),
                end: target.end.clone(),
                epoch: new_epoch,
            };
            g.regions[idx] = left.clone();
            g.regions.insert(idx + 1, right.clone());
            (left, right)
        };
        self.persist()?;
        Ok(result)
    }

    /// Replace the entire view with `regions` (PD applying a node's heartbeat report).
    /// Keeps the table sorted and advances `next_id` past every id seen so a later
    /// local split (if any) never collides.
    pub fn replace(&self, mut regions: Vec<Region>) {
        regions.sort_by(|a, b| a.start.cmp(&b.start));
        let mut g = self.state.lock().expect("registry poisoned");
        g.next_id = g.next_id.max(regions.iter().map(|r| r.id + 1).max().unwrap_or(1));
        g.regions = regions;
    }

    fn persist(&self) -> std::io::Result<()> {
        let Some(path) = &self.path else { return Ok(()) };
        let g = self.state.lock().expect("registry poisoned");
        atomic_write(path, &encode_state(&g))
    }
}

fn whole_keyspace(id: u64) -> Region {
    Region { id, start: Vec::new(), end: Vec::new(), epoch: 1 }
}

fn invalid(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, msg)
}

fn encode_state(s: &State) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&s.next_id.to_be_bytes());
    out.extend_from_slice(&(s.regions.len() as u32).to_be_bytes());
    for r in &s.regions {
        out.extend_from_slice(&r.id.to_be_bytes());
        out.extend_from_slice(&r.epoch.to_be_bytes());
        put_bytes(&mut out, &r.start);
        put_bytes(&mut out, &r.end);
    }
    out
}

fn decode_state(buf: &[u8]) -> Option<State> {
    let mut pos = 0;
    let next_id = get_u64(buf, &mut pos)?;
    let count = get_u32(buf, &mut pos)? as usize;
    let mut regions = Vec::with_capacity(count);
    for _ in 0..count {
        let id = get_u64(buf, &mut pos)?;
        let epoch = get_u64(buf, &mut pos)?;
        let start = get_bytes(buf, &mut pos)?.to_vec();
        let end = get_bytes(buf, &mut pos)?.to_vec();
        regions.push(Region { id, start, end, epoch });
    }
    Some(State { regions, next_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_keyspace_routes_everything() {
        let reg = RegionRegistry::in_memory();
        assert_eq!(reg.route(b"").unwrap().id, 1);
        assert_eq!(reg.route(b"anything").unwrap().id, 1);
        assert_eq!(reg.route(&[0xff; 32]).unwrap().id, 1);
    }

    #[test]
    fn split_partitions_keyspace_and_bumps_epoch() {
        let reg = RegionRegistry::in_memory();
        let (left, right) = reg.split(b"m").unwrap();

        assert_eq!(left.id, 1);
        assert_eq!(left.start, b"");
        assert_eq!(left.end, b"m");
        assert_eq!(left.epoch, 2, "split bumps the original region's epoch");
        assert_eq!(right.start, b"m");
        assert_eq!(right.end, b"");
        assert_eq!(right.epoch, 2);
        assert_ne!(right.id, left.id, "right half gets a fresh id");

        // Routing now respects the boundary.
        assert_eq!(reg.route(b"a").unwrap().id, left.id);
        assert_eq!(reg.route(b"m").unwrap().id, right.id, "boundary is inclusive on the right half");
        assert_eq!(reg.route(b"z").unwrap().id, right.id);

        // A second split nests correctly.
        let (l2, r2) = reg.split(b"t").unwrap();
        assert_eq!(l2.id, right.id);
        assert_eq!(l2.end, b"t");
        assert_eq!(r2.start, b"t");
        assert_eq!(reg.list().len(), 3);
    }

    #[test]
    fn split_rejects_boundary_and_outside_keys() {
        let reg = RegionRegistry::in_memory();
        reg.split(b"m").unwrap();
        // Splitting exactly at an existing region start is a no-op slice → rejected.
        assert!(reg.split(b"m").is_err());
        // Empty key cannot split (it equals every region's −∞ start).
        assert!(reg.split(b"").is_err());
    }

    #[test]
    fn persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        {
            let reg = RegionRegistry::open(dir.path()).unwrap();
            reg.split(b"m").unwrap();
            reg.split(b"t").unwrap();
        }
        let reg = RegionRegistry::open(dir.path()).unwrap();
        let regions = reg.list();
        assert_eq!(regions.len(), 3);
        // Epoch survives the reload — critical, or a restarted node could accept a
        // stale client's epoch.
        assert_eq!(reg.route(b"a").unwrap().epoch, 2);
        assert_eq!(reg.route(b"z").unwrap().end, b"");
    }

    #[test]
    fn replace_adopts_reported_regions() {
        let pd = RegionRegistry::empty();
        assert!(pd.route(b"k").is_none());
        pd.replace(vec![
            Region { id: 5, start: b"".to_vec(), end: b"m".to_vec(), epoch: 3 },
            Region { id: 9, start: b"m".to_vec(), end: b"".to_vec(), epoch: 3 },
        ]);
        assert_eq!(pd.route(b"a").unwrap().id, 5);
        assert_eq!(pd.route(b"z").unwrap().id, 9);
    }
}