use std::{
    io::Error,
    sync::{Arc, Mutex},
};

use crate::file::{blockid::BlockId, filemgr::FileMgr, page::Page};

use super::logiterator::LogIterator;

pub struct LogMgr {
    fm: Arc<FileMgr>,
    logfile: String,
    logpage: Page,
    currentblk: BlockId,
    lastest_lsn: usize,
    last_saved_lsn: usize,
}

fn append_new_block(fm: &FileMgr, logpage: &mut Page, logfile: &str) -> Result<BlockId, Error> {
    let blk = fm.append(logfile)?;
    logpage.set_int(0, fm.block_size() as i32);
    fm.write(&blk, logpage)?;
    Ok(blk)
}

impl LogMgr {
    pub fn new(fm: Arc<FileMgr>, logfile: &str) -> Result<LogMgr, Error> {
        let b = vec![0; fm.block_size()];
        let mut logpage = Page::with_vec(b);
        let logsize = fm.length(logfile)?;
        let currentblk = if logsize == 0 {
            append_new_block(&fm, &mut logpage, logfile)?
        } else {
            let currentblk = BlockId::new(logfile, logsize as i32 - 1);
            fm.read(&currentblk, &mut logpage)?;
            currentblk
        };
        Ok(LogMgr {
            fm,
            logfile: logfile.to_string(),
            logpage,
            currentblk,
            lastest_lsn: 0,
            last_saved_lsn: 0,
        })
    }

    pub fn flush(&mut self, lsn: usize) -> Result<(), Error> {
        if lsn >= self.last_saved_lsn {
            self.flush_impl()?
        }
        Ok(())
    }

    pub fn iterator(&mut self) -> Result<LogIterator, Error> {
        self.flush_impl()?;
        LogIterator::new(self.fm.clone(), &self.currentblk)
    }

    pub fn append(&mut self, logrec: &Vec<u8>) -> Result<usize, Error> {
        let m = Mutex::new(logrec);
        let rec = m.lock().unwrap();

        let mut boundary = self.logpage.get_int(0);
        let recsize = rec.len() as i32;
        let bytesneeded = recsize + 4;
        if boundary - bytesneeded < 4 {
            self.flush_impl()?;
            self.currentblk = self.append_new_block()?;
            boundary = self.logpage.get_int(0);
        }
        let recpos = boundary - bytesneeded;
        self.logpage.set_bytes(recpos as usize, &rec);
        self.logpage.set_int(0, recpos);
        self.lastest_lsn += 1;
        Ok(self.lastest_lsn)
    }

    fn append_new_block(&mut self) -> Result<BlockId, Error> {
        append_new_block(&self.fm, &mut self.logpage, self.logfile.as_str())
    }

    fn flush_impl(&mut self) -> Result<(), Error> {
        self.fm.write(&self.currentblk, &mut self.logpage)?;
        self.last_saved_lsn = self.lastest_lsn;
        Ok(())
    }
}
