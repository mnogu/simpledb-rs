use std::cell::RefCell;
use std::io::Error;
use std::rc::Rc;

use crate::buffer::buffermgr::BufferMgr;
use crate::file::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;

pub struct SimpleDB {
    fm: Rc<FileMgr>,
    lm: Rc<RefCell<LogMgr>>,
    bm: Rc<RefCell<BufferMgr>>,
}

impl SimpleDB {
    pub fn new(dirname: &str, blocksize: usize, buffsize: usize) -> Result<SimpleDB, Error> {
        let fm = Rc::new(FileMgr::new(dirname, blocksize));
        let lm = Rc::new(RefCell::new(LogMgr::new(fm.clone(), "simpledb.log")?));
        let bm = Rc::new(RefCell::new(BufferMgr::new(
            fm.clone(),
            lm.clone(),
            buffsize,
        )));
        Ok(SimpleDB { fm, lm, bm })
    }

    pub fn file_mgr(&self) -> Rc<FileMgr> {
        self.fm.clone()
    }

    pub fn log_mgr(&mut self) -> Rc<RefCell<LogMgr>> {
        self.lm.clone()
    }

    pub fn buffer_mgr(&mut self) -> Rc<RefCell<BufferMgr>> {
        self.bm.clone()
    }
}
