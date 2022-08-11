use std::io::Error;
use std::sync::{Arc, Mutex};

use crate::buffer::buffermgr::BufferMgr;
use crate::file::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;
use crate::metadata::metadatamgr::MetadataMgr;
use crate::plan::basicqueryplanner::BasicQueryPlanner;
use crate::plan::basicupdateplanner::BasicUpdatePlanner;
use crate::plan::planner::Planner;
use crate::tx::transaction::{Transaction, TransactionError};

pub struct SimpleDB {
    fm: Arc<FileMgr>,
    lm: Arc<Mutex<LogMgr>>,
    bm: Arc<Mutex<BufferMgr>>,
    planner: Option<Arc<Planner>>,
}

impl SimpleDB {
    const BLOCK_SIZE: usize = 400;
    const BUFFER_SIZE: usize = 8;
    const LOG_FILE: &'static str = "simpledb.log";

    pub fn with_params(
        dirname: &str,
        blocksize: usize,
        buffsize: usize,
    ) -> Result<SimpleDB, Error> {
        let fm = Arc::new(FileMgr::new(dirname, blocksize)?);
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), SimpleDB::LOG_FILE)?));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), buffsize)));
        Ok(SimpleDB {
            fm,
            lm,
            bm,
            planner: None,
        })
    }

    pub fn new(dirname: &str) -> Result<SimpleDB, TransactionError> {
        let mut sd = SimpleDB::with_params(dirname, SimpleDB::BLOCK_SIZE, SimpleDB::BUFFER_SIZE)?;
        let tx = Arc::new(Mutex::new(sd.new_tx()?));
        let isnew = sd.fm.is_new();
        if isnew {
            println!("creating new database");
        } else {
            println!("recovering existing database");
            tx.lock().unwrap().recover()?;
        }
        let mdm = Arc::new(Mutex::new(MetadataMgr::new(isnew, tx.clone())?));
        let qp = BasicQueryPlanner::new(mdm.clone()).into();
        let up = BasicUpdatePlanner::new(mdm).into();
        let planner = Planner::new(qp, up);
        sd.planner = Some(Arc::new(planner));
        tx.lock().unwrap().commit()?;
        Ok(sd)
    }

    pub fn new_tx(&self) -> Result<Transaction, Error> {
        Transaction::new(self.fm.clone(), self.lm.clone(), self.bm.clone())
    }

    pub fn planner(&self) -> Option<Arc<Planner>> {
        self.planner.clone()
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
