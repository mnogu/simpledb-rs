use std::io::Error;
use std::rc::Rc;

use crate::file::filemgr;
use crate::log::logmgr;

pub struct SimpleDB {
    fm: Rc<filemgr::FileMgr>,
    lm: logmgr::LogMgr,
}

impl SimpleDB {
    pub fn new(dirname: &str, blocksize: usize, _buffsize: usize) -> Result<SimpleDB, Error> {
        let fm = Rc::new(filemgr::FileMgr::new(dirname, blocksize));
        let lm = logmgr::LogMgr::new(fm.clone(), "simpledb.log")?;
        Ok(SimpleDB { fm: fm.clone(), lm })
    }

    pub fn file_mgr(&self) -> &filemgr::FileMgr {
        &self.fm
    }

    pub fn log_mgr(&mut self) -> &mut logmgr::LogMgr {
        &mut self.lm
    }
}
