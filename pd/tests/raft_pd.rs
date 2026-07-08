//! Deterministic PD-on-Raft failover test.
//!
//! Drives a three-node PD Raft group entirely in-process — no clock, no sockets — the same
//! way `arcux-raft`'s own `tests/cluster.rs` exercises the core. It proves the two guarantees
//! that make PD safe to replicate: after the leader is killed, a new leader (1) still has the
//! committed **placement** view, and (2) resumes the **TSO** strictly above every timestamp the
//! old leader could have handed out — the no-regression property Percolator depends on.

use std::collections::{BTreeMap, BTreeSet};

use arcux_pd::{PdCmd, PdReplica, Region};

/// An in-process bus of PD replicas. Messages are delivered synchronously; a "down" node
/// receives nothing and its outbound messages are dropped, modelling a crash/partition.
struct Cluster {
    nodes: BTreeMap<u64, PdReplica>,
    down: BTreeSet<u64>,
    bus: Vec<arcux_raft::Message>,
}

impl Cluster {
    fn new(ids: &[u64]) -> Cluster {
        let voters: Vec<u64> = ids.to_vec();
        let nodes = ids.iter().map(|&id| (id, PdReplica::new(id, voters.clone()))).collect();
        Cluster { nodes, down: BTreeSet::new(), bus: Vec::new() }
    }

    /// Deliver every queued message (and the responses they generate) until the bus quiesces.
    fn pump(&mut self) {
        for _ in 0..100_000 {
            let batch = std::mem::take(&mut self.bus);
            if batch.is_empty() {
                break;
            }
            for m in batch {
                if self.down.contains(&m.to) {
                    continue;
                }
                if let Some(r) = self.nodes.get_mut(&m.to) {
                    r.step(m);
                    let out = r.ready().messages;
                    self.bus.extend(out);
                }
            }
        }
    }

    /// One logical tick on every live node, then settle all resulting traffic.
    fn tick(&mut self) {
        let down = self.down.clone();
        for (id, r) in self.nodes.iter_mut() {
            if down.contains(id) {
                continue;
            }
            r.tick();
            self.bus.extend(r.ready().messages);
        }
        self.pump();
    }

    /// Tick until some live node is leader (or panic), returning its id.
    fn elect_leader(&mut self) -> u64 {
        for _ in 0..500 {
            self.tick();
            if let Some(id) = self.leader() {
                // A couple more ticks so the fresh leader's no-op commits and it is read-ready.
                self.tick();
                self.tick();
                return id;
            }
        }
        panic!("no leader elected");
    }

    fn leader(&self) -> Option<u64> {
        self.nodes
            .iter()
            .find(|(id, r)| !self.down.contains(id) && r.is_leader())
            .map(|(id, _)| *id)
    }

    /// Propose a command on `leader`, then settle so a majority commits and applies it.
    fn commit(&mut self, leader: u64, cmd: &PdCmd) {
        {
            let r = self.nodes.get_mut(&leader).expect("leader exists");
            r.propose(cmd).expect("leader accepts the proposal");
            let out = r.ready().messages;
            self.bus.extend(out);
        }
        self.pump();
        // Followers learn the advanced commit index on the next heartbeat, so tick to flush it.
        self.tick();
        self.tick();
    }

    fn kill(&mut self, id: u64) {
        self.down.insert(id);
    }
}

fn region(id: u64, start: &[u8], end: &[u8], epoch: u64) -> Region {
    Region { id, start: start.to_vec(), end: end.to_vec(), epoch }
}

#[test]
fn pd_survives_leader_failover_without_losing_placement_or_regressing_tso() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.elect_leader();

    // The leader reserves a TSO window and records a data node's placement — both through Raft.
    let reserve = {
        let l = c.nodes.get(&leader).unwrap();
        l.reserve_ts_cmd(4)
    };
    let reserved_upper = match reserve {
        PdCmd::ReserveTs { upper } => upper,
        _ => unreachable!(),
    };
    c.commit(leader, &reserve);
    c.commit(
        leader,
        &PdCmd::Heartbeat {
            node_id: 7,
            address: "http://node7".into(),
            regions: vec![region(1, b"", b"m", 1)],
            now: 1_000,
        },
    );

    // Every live replica has converged on the same committed state.
    for (id, r) in c.nodes.iter() {
        assert_eq!(r.fsm().tso_upper(), reserved_upper, "node {id} has the reserved high-water");
        assert_eq!(r.fsm().route(b"k").unwrap().node_id, 7, "node {id} has the placement");
    }

    // The leader hands out a few timestamps from its reserved window; capture the largest.
    let mut highest_issued = 0;
    {
        let l = c.nodes.get_mut(&leader).unwrap();
        for _ in 0..3 {
            let ts = l.hand_out(1).expect("window has room");
            assert!(ts < reserved_upper, "issued timestamp stays under the committed high-water");
            highest_issued = highest_issued.max(ts);
        }
    }

    // Kill the leader. The surviving two (still a majority of three) elect a new leader.
    c.kill(leader);
    let new_leader = c.elect_leader();
    assert_ne!(new_leader, leader, "a different node took over");

    // 1) Placement survived the failover.
    let p = c.nodes.get(&new_leader).unwrap().fsm().route(b"k").unwrap();
    assert_eq!(p.node_id, 7, "the new leader still routes to the placed node");

    // 2) The TSO never regresses: the new leader resumes at the committed high-water, so the
    //    very next timestamp it hands out is strictly greater than anything the old leader did.
    // The new leader reset its cursor to the committed high-water on election, so it reserves a
    // fresh window before serving (discarding the old leader's unused tail — the "at most one
    // window skipped on failover" property).
    let resumed = match c.nodes.get_mut(&new_leader).unwrap().hand_out(1) {
        Some(ts) => ts,
        None => {
            let cmd = c.nodes.get(&new_leader).unwrap().reserve_ts_cmd(1);
            c.commit(new_leader, &cmd);
            c.nodes.get_mut(&new_leader).unwrap().hand_out(1).expect("new leader serves a timestamp")
        }
    };
    assert!(
        resumed >= reserved_upper && resumed > highest_issued,
        "resumed timestamp {resumed} must exceed the old high-water {reserved_upper} and every issued ts {highest_issued}",
    );

    // 3) The new leader can still drive fresh consensus (placement writes commit under the
    //    surviving majority).
    c.commit(
        new_leader,
        &PdCmd::Heartbeat {
            node_id: 8,
            address: "http://node8".into(),
            regions: vec![region(2, b"m", b"", 1)],
            now: 2_000,
        },
    );
    let survivors: Vec<u64> = c.nodes.keys().copied().filter(|id| !c.down.contains(id)).collect();
    for id in survivors {
        let r = c.nodes.get(&id).unwrap();
        assert_eq!(r.fsm().route(b"z").unwrap().node_id, 8, "node {id} applied the post-failover write");
    }
}
