use std::cell::RefCell;
use std::io::Error;
use std::rc::Rc;

use crate::buffer::buffermgr::BufferMgr;
use crate::file::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;

pub struct SimpleDB {
    fm: Rc<FileMgr>,
    lm: Rc<RefCell<LogMgr>>,
    bm: BufferMgr,
}

impl SimpleDB {
    pub fn new(dirname: &str, blocksize: usize, buffsize: usize) -> Result<SimpleDB, Error> {
        let fm = Rc::new(FileMgr::new(dirname, blocksize));
        let lm = Rc::new(RefCell::new(LogMgr::new(fm.clone(), "simpledb.log")?));
        let bm = BufferMgr::new(fm.clone(), lm.clone(), buffsize);
        Ok(SimpleDB { fm, lm, bm })
    }

    pub fn file_mgr(&self) -> &FileMgr {
        &self.fm
    }

    pub fn log_mgr(&mut self) -> &RefCell<LogMgr> {
        &self.lm
    }

    pub fn buffer_mgr(&mut self) -> &mut BufferMgr {
        &mut self.bm
    }
}
