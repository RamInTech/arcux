//! SSTable writer.
//!
//! On-disk layout:
//!
//! ```text
//! [data block 0][data block 1] ... [data block N][index block][footer]
//! ```
//!
//! * **data block** on disk = `entries ‖ crc32c(entries)` (4-byte trailer).
//! * **index block** = one `[first_key (len-prefixed)][offset:u64][len:u32]` per
//!   data block, then `crc32c` of all of that. `len` is the data block's on-disk
//!   length *including* its CRC trailer.
//! * **footer** (fixed 20 bytes) = `[index_offset:u64][index_len:u32][magic:u64]`.
//!
//! Keys must be `add`-ed in ascending order (the engine feeds Default→Write→Lock,
//! each CF already sorted, with a 1-byte CF prefix making the union globally sorted).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::encoding::put_length_prefixed;
use crate::error::Result;
use crate::sstable::block::encode_entry;

pub const FOOTER_MAGIC: u64 = 0x5353_5442_5f56_3031; // "SSTB_V01"
pub const FOOTER_LEN: usize = 8 + 4 + 8; // index_offset + index_len + magic
const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

pub struct SstWriter {
    file: BufWriter<File>,
    offset: u64,
    block: Vec<u8>,
    cur_first_key: Option<Vec<u8>>,
    index: Vec<(Vec<u8>, u64, u32)>, // (first_key, offset, on-disk len)
    target_block_size: usize,
}

impl SstWriter {
    pub fn create(path: impl AsRef<Path>) -> Result<SstWriter> {
        let file = BufWriter::new(File::create(path)?);
        Ok(SstWriter {
            file,
            offset: 0,
            block: Vec::with_capacity(DEFAULT_BLOCK_SIZE + 256),
            cur_first_key: None,
            index: Vec::new(),
            target_block_size: DEFAULT_BLOCK_SIZE,
        })
    }

    /// Append an entry. Caller guarantees ascending `key` order across calls.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.block.is_empty() {
            self.cur_first_key = Some(key.to_vec());
        }
        encode_entry(&mut self.block, key, value);
        if self.block.len() >= self.target_block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let crc = crc32c::crc32c(&self.block);
        self.block.extend_from_slice(&crc.to_be_bytes());
        let len = self.block.len() as u32;
        self.file.write_all(&self.block)?;
        let first = self.cur_first_key.take().expect("first key set when block non-empty");
        self.index.push((first, self.offset, len));
        self.offset += len as u64;
        self.block.clear();
        Ok(())
    }

    /// Flush the final block, write the index + footer, and fsync the file.
    pub fn finish(mut self) -> Result<()> {
        self.flush_block()?;

        let mut idx = Vec::new();
        for (first, off, len) in &self.index {
            put_length_prefixed(&mut idx, first);
            idx.extend_from_slice(&off.to_be_bytes());
            idx.extend_from_slice(&len.to_be_bytes());
        }
        let idx_crc = crc32c::crc32c(&idx);
        idx.extend_from_slice(&idx_crc.to_be_bytes());
        let index_offset = self.offset;
        let index_len = idx.len() as u32;
        self.file.write_all(&idx)?;

        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.extend_from_slice(&index_offset.to_be_bytes());
        footer.extend_from_slice(&index_len.to_be_bytes());
        footer.extend_from_slice(&FOOTER_MAGIC.to_be_bytes());
        self.file.write_all(&footer)?;

        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }
}
