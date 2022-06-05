use std::{
    io::Error,
    sync::{Arc, Mutex},
};

use crate::{
    file::{blockid::BlockId, filemgr::FileMgr, page::Page},
    log::logmgr::LogMgr,
};

pub struct Buffer {
    fm: Arc<FileMgr>,
    lm: Arc<Mutex<LogMgr>>,
    contents: Page,
    blk: Option<BlockId>,
    pins: i32,
    txnum: Option<usize>,
    lsn: Option<usize>,
}

impl Buffer {
    pub fn new(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>) -> Buffer {
        let blocksize = fm.block_size();
        Buffer {
            fm,
            lm,
            contents: Page::new(blocksize),
            blk: None,
            pins: 0,
            txnum: None,
            lsn: None,
        }
    }

    pub fn contents(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn block(&self) -> &Option<BlockId> {
        &self.blk
    }

    pub fn set_modified(&mut self, txnum: usize, lsn: i32) {
        self.txnum = Some(txnum);
        if lsn >= 0 {
            self.lsn = Some(lsn as usize);
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> Option<usize> {
        self.txnum
    }

    pub(in crate::buffer) fn assign_to_block(&mut self, b: BlockId) -> Result<(), Error> {
        self.flush()?;
        self.blk = Some(b.clone());
        self.fm.read(&b, &mut self.contents)?;
        self.pins = 0;
        Ok(())
    }

    pub(in crate::buffer) fn flush(&mut self) -> Result<(), Error> {
        if let Some(_) = self.txnum {
            if let Some(lsn) = self.lsn {
                self.lm.lock().unwrap().flush(lsn)?;
            }
            if let Some(ref blk) = self.blk {
                self.fm.write(&blk, &mut self.contents)?;
            }
            self.txnum = None;
        }
        Ok(())
    }

    pub(in crate::buffer) fn pin(&mut self) {
        self.pins += 1;
    }

    pub(in crate::buffer) fn unpin(&mut self) {
        self.pins -= 1;
    }
}
