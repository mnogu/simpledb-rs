use std::{io::Error, rc::Rc};

use crate::file::{blockid::BlockId, filemgr::FileMgr, page::Page};

fn move_to_block(fm: Rc<FileMgr>, blk: &BlockId, p: &mut Page) -> Result<(i32, i32), Error> {
    fm.read(blk, p)?;
    let boundary = p.get_int(0);
    let currentpos = boundary;
    Ok((boundary, currentpos))
}

pub struct LogIterator {
    fm: Rc<FileMgr>,
    blk: BlockId,
    p: Page,
    currentpos: i32,
    boundary: i32,
}

impl LogIterator {
    pub fn new(fm: Rc<FileMgr>, blk: &BlockId) -> Result<LogIterator, Error> {
        let b: Vec<u8> = vec![0; fm.block_size()];
        let mut p = Page::with_vec(b);
        let (boundary, currentpos) = move_to_block(fm.clone(), blk, &mut p)?;
        Ok(LogIterator {
            fm,
            blk: blk.clone(),
            p,
            currentpos,
            boundary,
        })
    }

    fn move_to_block(&mut self, blk: &BlockId) -> Result<(), Error> {
        let (boundary, currentpos) = move_to_block(self.fm.clone(), blk, &mut self.p)?;
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
