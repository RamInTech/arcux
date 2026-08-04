//! Auto re-replication — PD's failure detector driving membership repair (Phase 4b++ rest).
//!
//! When a node dies, every region it replicated is left under-replicated (RF=3 → 2): still
//! serving — a majority survives — but one more failure from losing quorum. This module
//! closes the loop **without operator action**:
//!
//! ```text
//!   PD failure detector          repair driver                       region leader
//!   ───────────────────          ─────────────────────────────       ──────────────
//!   sweep() marks node D down →  pick a live spare S             →   AddLearner(S)   (no quorum impact)
//!                                S hosts the region (empty conf) →   S catches up (append / snapshot)
//!                                poll peer_caught_up(S)          →   AddNode(S)      (promote: instant, S is warm)
//!                                                                →   RemoveNode(D)   (RF=3 restored, live-only)
//! ```
//!
//! The **learner-first** order is the point: adding the cold spare as a *voter* would move
//! the majority to 3-of-4 while only 2 replicas are live — stalling every write until the
//! newcomer caught up. A learner has no quorum weight, so writes keep flowing during
//! catch-up, and promotion happens only when it is marginal (the learner already holds the
//! whole log). Proven deterministically in `raft/tests/cluster.rs`
//! (`dead_voter_is_replaced_via_learner_then_promotion`); this module runs the same sequence
//! against live groups over the real transport.
//!
//! The detector is the real PD one ([`arcux_pd::Membership`]: heartbeats + `sweep`). The
//! driver here holds in-process [`AppState`] handles — the same seam the membership e2e
//! uses — standing in for the admin RPCs a fully PD-hosted supervisor would issue.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arcux_pd::cluster::now_ms;
use arcux_pd::Membership;
use arcux_raft::ConfChange;

use crate::multiraft::{Regime, RegionPlacement};
use crate::raft_group::{ProposeResult, RaftGroup};
use crate::AppState;

/// A region the supervisor keeps at full replication (its identity + range, so a fresh
/// replica can be hosted on a spare node, and the replication factor to restore).
#[derive(Clone)]
pub struct WatchedRegion {
    pub region_id: u64,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub epoch: u64,
    /// The voter count to maintain (3 for RF=3).
    pub target_rf: usize,
}

/// One planned repair: replace `dead` with `spare` in `region_id`'s replica set.
#[derive(Debug, PartialEq, Eq)]
pub struct RepairPlan {
    pub region_id: u64,
    pub dead: u64,
    pub spare: u64,
}

/// The re-replication supervisor: PD's failure detector on one side, the regions' Raft
/// groups on the other. [`run`](Supervisor::run) loops detect → plan → repair.
pub struct Supervisor {
    /// The failure detector — PD's per-node liveness view, fed by node heartbeats.
    detector: Arc<Membership>,
    /// Silence longer than this ⇒ a node is declared down (passed to `sweep`).
    fd_timeout_ms: u64,
    /// In-process handles to the data nodes (node id → state).
    nodes: HashMap<u64, Arc<AppState>>,
    /// Every node's serving address — what PD hands out so replicas can reach each other.
    addresses: HashMap<u64, String>,
    /// The regions kept at full replication.
    regions: Vec<WatchedRegion>,
}

impl Supervisor {
    pub fn new(
        detector: Arc<Membership>,
        fd_timeout_ms: u64,
        nodes: HashMap<u64, Arc<AppState>>,
        addresses: HashMap<u64, String>,
        regions: Vec<WatchedRegion>,
    ) -> Supervisor {
        Supervisor { detector, fd_timeout_ms, nodes, addresses, regions }
    }

    /// The region's current leader group among nodes the detector considers live. (A dead
    /// node's stale handle may still claim leadership — its actor stopped publishing — so
    /// down nodes are skipped.)
    fn leader_of(&self, region_id: u64) -> Option<RaftGroup> {
        self.nodes
            .iter()
            .filter(|(id, _)| !self.detector.is_down(**id))
            .filter_map(|(_, s)| s.raft_group(region_id))
            .find(|g| g.is_leader())
    }

    /// Plan a repair for `region` if it has a **down voter**: pick the replacement. `None`
    /// ⇒ healthy, or nothing can be done (no live spare exists).
    ///
    /// The choice makes an interrupted repair **resumable** — [`repair`](Self::repair) skips
    /// steps the membership already reflects, so the spare is, in preference order:
    /// 1. a live **learner** (a previous attempt already added it → resume at catch-up);
    /// 2. a live **extra voter** beyond `target_rf` (already promoted → resume at removal);
    /// 3. a **fresh** live node with a handle and an address.
    fn plan(&self, region: &WatchedRegion) -> Option<RepairPlan> {
        let leader = self.leader_of(region.region_id)?;
        let voters = leader.voters();
        let learners = leader.learners();
        let live = |id: &u64| !self.detector.is_down(*id) && self.nodes.contains_key(id);
        let dead = *voters.iter().find(|v| self.detector.is_down(**v))?;
        let spare = learners
            .iter()
            .copied()
            .find(live)
            .or_else(|| {
                // Already promoted by an interrupted repair: only the dead one remains to drop.
                (voters.len() > region.target_rf)
                    .then(|| voters.iter().copied().find(|v| *v != dead && live(v)))
                    .flatten()
            })
            .or_else(|| {
                self.addresses
                    .keys()
                    .copied()
                    .find(|id| !voters.contains(id) && !learners.contains(id) && live(id))
            })?;
        Some(RepairPlan { region_id: region.region_id, dead, spare })
    }

    /// Propose one membership change on the region's **current** leader, retrying across
    /// re-elections and the one-in-flight-change gate until it commits (or attempts run out).
    async fn conf_change(&self, region_id: u64, cc: ConfChange) -> Result<(), String> {
        for _ in 0..400 {
            if let Some(leader) = self.leader_of(region_id) {
                match leader.propose_conf_change(cc).await {
                    ProposeResult::Applied(Ok(())) => return Ok(()),
                    ProposeResult::Applied(Err(e)) => {
                        return Err(format!("membership change failed to apply: {e}"));
                    }
                    // Not the leader anymore, or a change is still in flight — retry.
                    ProposeResult::NotLeader { .. } => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        Err(format!("no leader accepted {cc:?} for region {region_id}"))
    }

    /// Execute one repair end-to-end: host the region on the spare, add it as a learner,
    /// wait for catch-up, promote it, drop the dead voter. Steps the current membership
    /// already reflects are skipped, so an interrupted repair resumes instead of wedging.
    async fn repair(&self, region: &WatchedRegion, plan: &RepairPlan) -> Result<(), String> {
        let RepairPlan { region_id, dead, spare } = *plan;
        eprintln!(
            "[repair region {region_id}] node {dead} is down — re-replicating onto node {spare}"
        );
        let leader = self
            .leader_of(region_id)
            .ok_or_else(|| format!("region {region_id} has no live leader"))?;

        if !leader.voters().contains(&spare) {
            if !leader.learners().contains(&spare) {
                let spare_state = self
                    .nodes
                    .get(&spare)
                    .ok_or_else(|| format!("no handle for spare node {spare}"))?;
                let spare_addr = self.addresses.get(&spare).cloned().unwrap_or_default();

                // The spare hosts a blank replica of the region (empty bootstrap config — it
                // adopts the real membership from the log/snapshot once added), knowing every
                // peer address.
                let peers: HashMap<u64, String> = self
                    .addresses
                    .iter()
                    .filter(|(id, _)| **id != spare)
                    .map(|(id, a)| (*id, a.clone()))
                    .collect();
                spare_state
                    .host_region(RegionPlacement {
                        region_id,
                        start: region.start.clone(),
                        end: region.end.clone(),
                        epoch: region.epoch,
                        regime: Regime::Cp,
                        voters: Vec::new(),
                        peers,
                    })
                    .map_err(|e| format!("spare could not host region {region_id}: {e}"))?;
                // Every live replica learns the spare's address (whichever of them leads —
                // now or after a later election — must be able to replicate to it).
                for (id, state) in &self.nodes {
                    if *id != spare && !self.detector.is_down(*id) {
                        if let Some(g) = state.raft_group(region_id) {
                            g.add_peer(spare, &spare_addr);
                        }
                    }
                }

                // 1) Learner first: no quorum weight, so writes keep flowing while it's cold.
                self.conf_change(region_id, ConfChange::AddLearner(spare)).await?;
            }

            // 2) Wait until the learner holds the leader's whole log (append or snapshot).
            let mut caught_up = false;
            for _ in 0..600 {
                if self.leader_of(region_id).is_some_and(|g| g.peer_caught_up(spare)) {
                    caught_up = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            if !caught_up {
                return Err(format!("learner {spare} never caught up in region {region_id}"));
            }
            eprintln!("[repair region {region_id}] learner {spare} caught up — promoting");

            // 3) Promote — safe now: the learner acks instantly, so the grown quorum is whole.
            self.conf_change(region_id, ConfChange::AddNode(spare)).await?;
        }

        // 4) Drop the dead voter: the replica set is live-only again at full RF.
        self.conf_change(region_id, ConfChange::RemoveNode(dead)).await?;
        eprintln!(
            "[repair region {region_id}] done — node {spare} replaced node {dead} (RF restored)"
        );
        Ok(())
    }

    /// One supervision pass: run the failure detector, then repair any under-replicated
    /// watched region (one repair at a time — repairs are serialized deliberately, each is
    /// itself a sequence of one-at-a-time membership changes).
    pub async fn tick(&self) {
        for id in self.detector.sweep(now_ms(), self.fd_timeout_ms) {
            eprintln!("[repair] failure detector: node {id} marked down");
        }
        for region in &self.regions {
            if let Some(plan) = self.plan(region) {
                if let Err(e) = self.repair(region, &plan).await {
                    eprintln!("[repair region {}] failed (will retry): {e}", region.region_id);
                }
            }
        }
    }

    /// Supervise forever: detect → plan → repair, every `interval`. Run this as a task; it
    /// stops when the task is aborted/dropped.
    pub async fn run(self, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.tick().await;
        }
    }
}
