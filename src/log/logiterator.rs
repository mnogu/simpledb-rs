use std::{io::Error, sync::Arc};

use crate::file::{blockid::BlockId, filemgr::FileMgr, page::Page};

pub struct LogIterator {
    fm: Arc<FileMgr>,
    blk: BlockId,
    p: Page,
    currentpos: i32,
    boundary: i32,
}

impl LogIterator {
    pub fn new(fm: Arc<FileMgr>, blk: &BlockId) -> Result<LogIterator, Error> {
        let b: Vec<u8> = vec![0; fm.block_size()];
        let p = Page::with_vec(b);
        let mut l = LogIterator {
            fm,
            blk: blk.clone(),
            p,
            currentpos: 0,
            boundary: 0,
        };
        l.move_to_block(blk)?;
        Ok(l)
    }

    fn move_to_block(&mut self, blk: &BlockId) -> Result<(), Error> {
        self.fm.clone().read(blk, &mut self.p)?;
        let boundary = self.p.get_int(0);
        let currentpos = boundary;
        self.boundary = boundary;
        self.currentpos = currentpos;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.currentpos >= self.fm.block_size() as i32 && self.blk.number() <= 0 {
            return None;
        }
        if self.currentpos == self.fm.block_size() as i32 {
            self.blk = BlockId::new(self.blk.file_name(), self.blk.number() - 1);
            self.move_to_block(&self.blk.clone()).ok();
        }
        let rec = self.p.get_bytes(self.currentpos as usize);
        self.currentpos += 4 + rec.len() as i32;
        Some(rec.to_vec())
    }
}
