//! WAL durability tests: record round-trip and torn-tail truncation — the heart
//! of the "discard the un-acked tail, keep everything acknowledged" guarantee.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use arcux_engine::batch::WriteBatch;
use arcux_engine::keys::Cf;
use arcux_engine::options::FsyncMode;
use arcux_engine::wal::{WalReader, WalWriter};

fn wal_path() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    (dir, path)
}

#[test]
fn round_trips_real_batches() {
    let (_dir, path) = wal_path();

    let mut b1 = WriteBatch::new();
    b1.put(Cf::Default, b"alice".to_vec(), b"100".to_vec());
    let mut b2 = WriteBatch::new();
    b2.put(Cf::Write, b"alice".to_vec(), 7u64.to_be_bytes().to_vec());
    b2.delete(Cf::Lock, b"alice".to_vec());

    {
        let mut w = WalWriter::create(&path).unwrap();
        w.append(10, &b1.encode()).unwrap();
        w.append(11, &b2.encode()).unwrap();
        w.sync(FsyncMode::Fsync).unwrap();
    }

    let mut r = WalReader::open(&path).unwrap();
    let (s1, p1) = r.next_record().unwrap();
    let (s2, p2) = r.next_record().unwrap();
    assert_eq!((s1, WriteBatch::decode(&p1).unwrap()), (10, b1));
    assert_eq!((s2, WriteBatch::decode(&p2).unwrap()), (11, b2));
    assert_eq!(r.next_record(), None);
}

#[test]
fn truncated_tail_is_discarded() {
    let (_dir, path) = wal_path();
    {
        let mut w = WalWriter::create(&path).unwrap();
        w.append(1, b"first").unwrap();
        w.append(2, b"second").unwrap();
        w.append(3, b"third-record-that-will-be-torn").unwrap();
        w.sync(FsyncMode::None).unwrap();
    }
    // Simulate a crash mid-append: chop bytes off the end so the last record's
    // body is truncated.
    let full = std::fs::metadata(&path).unwrap().len();
    let f = OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(full - 6).unwrap();

    let mut r = WalReader::open(&path).unwrap();
    assert_eq!(r.next_record(), Some((1, b"first".to_vec())));
    assert_eq!(r.next_record(), Some((2, b"second".to_vec())));
    assert_eq!(r.next_record(), None, "torn third record must be dropped");
}

#[test]
fn bitflip_in_tail_record_fails_crc() {
    let (_dir, path) = wal_path();
    {
        let mut w = WalWriter::create(&path).unwrap();
        w.append(1, b"intact").unwrap();
        w.append(2, b"corruptme").unwrap();
        w.sync(FsyncMode::None).unwrap();
    }
    // Flip the very last byte of the file (inside record 2's payload).
    let len = std::fs::metadata(&path).unwrap().len();
    let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(len - 1)).unwrap();
    let mut byte = [0u8; 1];
    use std::io::Read;
    f.seek(SeekFrom::Start(len - 1)).unwrap();
    f.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xFF;
    f.seek(SeekFrom::Start(len - 1)).unwrap();
    f.write_all(&byte).unwrap();

    let mut r = WalReader::open(&path).unwrap();
    assert_eq!(r.next_record(), Some((1, b"intact".to_vec())));
    assert_eq!(r.next_record(), None, "CRC mismatch must stop replay at record 2");
}
