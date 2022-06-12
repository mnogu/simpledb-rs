use std::io::Error;
use std::sync::{Arc, Mutex};

use crate::buffer::buffermgr::BufferMgr;
use crate::file::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;
use crate::tx::transaction::Transaction;

pub struct SimpleDB {
    fm: Arc<FileMgr>,
    lm: Arc<Mutex<LogMgr>>,
    bm: Arc<Mutex<BufferMgr>>,
}

impl SimpleDB {
    pub fn new(dirname: &str, blocksize: usize, buffsize: usize) -> Result<SimpleDB, Error> {
        let fm = Arc::new(FileMgr::new(dirname, blocksize));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "simpledb.log")?));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), buffsize)));
        Ok(SimpleDB { fm, lm, bm })
    }

    pub fn new_tx(&self) -> Result<Transaction, Error> {
        Transaction::new(self.fm.clone(), self.lm.clone(), self.bm.clone())
    }

    pub fn file_mgr(&self) -> Arc<FileMgr> {
        self.fm.clone()
    }

    pub fn log_mgr(&mut self) -> Arc<Mutex<LogMgr>> {
        self.lm.clone()
    }

    pub fn buffer_mgr(&mut self) -> Arc<Mutex<BufferMgr>> {
        self.bm.clone()
    }
}
