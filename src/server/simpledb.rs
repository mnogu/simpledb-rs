use crate::file::filemgr;

pub struct SimpleDB {
    fm: filemgr::FileMgr,
}

impl SimpleDB {
    pub fn new(dirname: &str, blocksize: usize, _buffsize: u64) -> SimpleDB {
        SimpleDB {
            fm: filemgr::FileMgr::new(dirname, blocksize),
        }
    }

    pub fn file_mgr(&self) -> &filemgr::FileMgr {
        &self.fm
    }
}
