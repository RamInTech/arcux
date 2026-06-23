//! Crash-recovery tests — the heart of the Phase-1 DoD: *no acknowledged write is
//! lost across a crash.*
//!
//! Two tests:
//!  * `reopen_preserves_mixed_sstable_and_wal_data` — deterministic: write data that
//!    spans flushed SSTables *and* the un-flushed WAL tail, drop the engine, reopen,
//!    and assert everything is present (exercises the manifest + WAL-replay path).
//!  * `recovers_acked_writes_after_sigkill` — the real thing: a child process writes
//!    keys and records each *acknowledged* write to an oracle file, then is `SIGKILL`-ed
//!    at a random point. The parent reopens the data dir and asserts every acked write
//!    survived with the correct value, repeated across several random kill points.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use arcux_engine::{Cf, Engine, FsyncMode, Options, WriteBatch};

const CHILD_ENV: &str = "arcux_CRASH_CHILD_DIR";

#[test]
fn reopen_preserves_mixed_sstable_and_wal_data() {
    if std::env::var(CHILD_ENV).is_ok() {
        return; // never run the deterministic test inside a spawned child
    }
    let dir = tempfile::tempdir().unwrap();
    // Small threshold so the first ~half flushes to SSTables and the rest stays in
    // the active memtable / WAL tail.
    let opts = || Options::new(dir.path()).with_memtable_threshold(512);

    {
        let eng = Engine::open(opts()).unwrap();
        for i in 0..300u64 {
            let mut b = WriteBatch::new();
            b.put(Cf::Default, format!("key{i:05}").into_bytes(), format!("val{i}").into_bytes());
            eng.write(b).unwrap();
        }
        assert!(eng.sstable_count() > 0, "expected some flushed SSTables");
        assert!(eng.last_seq() > eng.last_flushed_seq(), "expected un-flushed WAL tail");
        // Drop -> clean shutdown (committer joined). Data on disk = WAL + SSTables.
    }

    // Reopen: recovery must reconstruct every key from SSTables + replayed WAL.
    let eng = Engine::open(opts()).unwrap();
    for i in 0..300u64 {
        assert_eq!(
            eng.get_cf_raw(Cf::Default, format!("key{i:05}").as_bytes()).unwrap(),
            Some(format!("val{i}").into_bytes()),
            "key{i} missing after reopen"
        );
    }
    // Sequence numbers must not regress.
    assert_eq!(eng.last_seq(), 300);
}

/// Child entrypoint: when `CHILD_ENV` is set, become the writer and loop forever,
/// recording each acked write to `<dir>/oracle.log`, until the parent kills us.
/// When the env var is absent (normal `cargo test`), this is a no-op.
#[test]
fn crash_child_entrypoint() {
    let Ok(dir) = std::env::var(CHILD_ENV) else {
        return;
    };
    let opts = Options::new(&dir)
        .with_memtable_threshold(4096)
        .with_fsync_mode(FsyncMode::Fsync);
    let eng = Engine::open(opts).unwrap();

    let mut oracle = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(Path::new(&dir).join("oracle.log"))
        .unwrap();

    let mut i: u64 = 0;
    loop {
        let mut b = WriteBatch::new();
        b.put(Cf::Default, format!("key{i:08}").into_bytes(), format!("val{i}").into_bytes());
        eng.write(b).unwrap();
        // Record the ack *after* write returns: oracle ⊆ durably-acked writes.
        writeln!(oracle, "{i}").unwrap();
        oracle.flush().unwrap();
        i += 1;
    }
}

#[test]
fn recovers_acked_writes_after_sigkill() {
    if std::env::var(CHILD_ENV).is_ok() {
        return; // we're a child; do nothing here
    }
    let exe = std::env::current_exe().expect("test exe path");

    for round in 0..6u64 {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        let mut child = Command::new(&exe)
            .args(["--exact", "crash_child_entrypoint", "--test-threads=1"])
            .env(CHILD_ENV, &data_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child writer");

        // Sleep a pseudo-random 40–400 ms, then SIGKILL mid-flight.
        let jitter = pseudo_rand(round) % 360 + 40;
        std::thread::sleep(Duration::from_millis(jitter));
        child.kill().expect("kill child");
        let _ = child.wait();

        // Read the oracle: the set of acknowledged writes.
        let oracle_path = data_dir.join("oracle.log");
        let acked: Vec<u64> = match std::fs::read_to_string(&oracle_path) {
            Ok(s) => s.lines().filter_map(|l| l.trim().parse().ok()).collect(),
            Err(_) => Vec::new(),
        };

        // Reopen and verify every acked write survived with the right value.
        let eng = Engine::open(Options::new(&data_dir)).unwrap();
        for &i in &acked {
            assert_eq!(
                eng.get_cf_raw(Cf::Default, format!("key{i:08}").as_bytes()).unwrap(),
                Some(format!("val{i}").into_bytes()),
                "round {round}: acked write {i} lost after SIGKILL (jitter={jitter}ms)"
            );
        }
        eprintln!("round {round}: {} acked writes all recovered (jitter {jitter}ms)", acked.len());
    }
}

/// Tiny time-seeded PRNG — avoids pulling in the `rand` crate for one jitter value.
fn pseudo_rand(salt: u64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    let mut x = nanos ^ (salt.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}
