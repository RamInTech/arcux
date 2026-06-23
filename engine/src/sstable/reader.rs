//! SSTable reader: footer → index → block binary-search → in-block scan.
//!
//! The footer and index are loaded eagerly at `open`; data blocks are read on
//! demand via positioned reads (`pread`) and CRC-verified each time. (A block
//! cache is deferred to Phase 1b — this keeps memory bounded without one.)

use std::cmp::Ordering;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::error::{Error, Result};
use crate::keys::Cf;
use crate::memtable::MemValue;
use crate::sstable::block;
use crate::sstable::writer::{FOOTER_LEN, FOOTER_MAGIC};

struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

pub struct SstReader {
    file: File,
    index: Vec<IndexEntry>,
    file_no: u64,
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>, file_no: u64) -> Result<SstReader> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN as u64 {
            return Err(Error::corruption("sstable smaller than footer"));
        }
        let mut footer = [0u8; FOOTER_LEN];
        file.read_exact_at(&mut footer, file_len - FOOTER_LEN as u64)?;
        let index_offset = u64::from_be_bytes(footer[0..8].try_into().unwrap());
        let index_len = u32::from_be_bytes(footer[8..12].try_into().unwrap());
        let magic = u64::from_be_bytes(footer[12..20].try_into().unwrap());
        if magic != FOOTER_MAGIC {
            return Err(Error::corruption("bad sstable magic"));
        }

        let mut idx_buf = vec![0u8; index_len as usize];
        file.read_exact_at(&mut idx_buf, index_offset)?;
        if idx_buf.len() < 4 {
            return Err(Error::corruption("sstable index too small"));
        }
        let (body, crc_b) = idx_buf.split_at(idx_buf.len() - 4);
        if crc32c::crc32c(body) != u32::from_be_bytes(crc_b.try_into().unwrap()) {
            return Err(Error::corruption("sstable index crc mismatch"));
        }

        let mut index = Vec::new();
        let mut pos = 0usize;
        while pos < body.len() {
            let first_key = crate::encoding::get_length_prefixed(body, &mut pos)
                .ok_or_else(|| Error::corruption("sstable index entry"))?
                .to_vec();
            if pos + 12 > body.len() {
                return Err(Error::corruption("sstable index truncated"));
            }
            let offset = u64::from_be_bytes(body[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let len = u32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
            pos += 4;
            index.push(IndexEntry { first_key, offset, len });
        }

        Ok(SstReader { file, index, file_no })
    }

    pub fn file_no(&self) -> u64 {
        self.file_no
    }

    fn read_block(&self, e: &IndexEntry) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; e.len as usize];
        self.file.read_exact_at(&mut buf, e.offset)?;
        if buf.len() < 4 {
            return Err(Error::corruption("sstable block too small"));
        }
        let (data, crc_b) = buf.split_at(buf.len() - 4);
        if crc32c::crc32c(data) != u32::from_be_bytes(crc_b.try_into().unwrap()) {
            return Err(Error::corruption("sstable block crc mismatch"));
        }
        buf.truncate(buf.len() - 4);
        Ok(buf)
    }

    /// Index of the last block whose first key is ≤ `sk`, or `None` if `sk`
    /// precedes the whole table.
    fn candidate_block(&self, sk: &[u8]) -> Option<usize> {
        let pp = self.index.partition_point(|e| e.first_key.as_slice() <= sk);
        if pp == 0 {
            None
        } else {
            Some(pp - 1)
        }
    }

    /// Exact point lookup of a CF key. `Some(MemValue)` (Put or tombstone) is an
    /// authoritative hit; `None` means absent from this table.
    pub fn get(&self, cf: Cf, key: &[u8]) -> Result<Option<MemValue>> {
        let mut sk = Vec::with_capacity(1 + key.len());
        sk.push(cf as u8);
        sk.extend_from_slice(key);

        let Some(bi) = self.candidate_block(&sk) else {
            return Ok(None);
        };
        let block = self.read_block(&self.index[bi])?;
        for (ek, ev) in block::entries(&block) {
            match ek.cmp(sk.as_slice()) {
                Ordering::Less => {}
                Ordering::Equal => {
                    return Ok(Some(
                        MemValue::decode(ev).ok_or_else(|| Error::corruption("sstable value"))?,
                    ))
                }
                Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Forward seek within `cf`: the first `(cf_key, value)` with `cf_key ≥ from`,
    /// or `None` if the seek runs past the end of this CF. Used by MVCC version
    /// resolution (the descending-ts encoding makes this "newest at-or-after").
    pub fn seek(&self, cf: Cf, from: &[u8]) -> Result<Option<(Vec<u8>, MemValue)>> {
        let cf_byte = cf as u8;
        let mut sk = Vec::with_capacity(1 + from.len());
        sk.push(cf_byte);
        sk.extend_from_slice(from);

        let mut bi = self.candidate_block(&sk).unwrap_or(0);
        while bi < self.index.len() {
            let block = self.read_block(&self.index[bi])?;
            for (ek, ev) in block::entries(&block) {
                if ek >= sk.as_slice() {
                    if ek.first() != Some(&cf_byte) {
                        return Ok(None); // crossed into the next CF
                    }
                    let cf_key = ek[1..].to_vec();
                    let mv = MemValue::decode(ev).ok_or_else(|| Error::corruption("sstable value"))?;
                    return Ok(Some((cf_key, mv)));
                }
            }
            bi += 1;
        }
        Ok(None)
    }
}
