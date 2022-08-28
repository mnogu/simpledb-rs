use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::Mutex};

use crate::{
    buffer::buffermgr::AbortError, file::blockid::BlockId, tx::concurrency::locktable::LockTable,
};

static LOCKTBL: Lazy<Mutex<LockTable>> = Lazy::new(|| Mutex::new(LockTable::new()));

pub struct ConcurrencyMgr {
    locks: HashMap<BlockId, String>,
}

impl ConcurrencyMgr {
    pub fn new() -> ConcurrencyMgr {
        ConcurrencyMgr {
            locks: HashMap::new(),
        }
    }

    pub fn s_lock(&mut self, blk: &BlockId) -> Result<(), AbortError> {
        if !self.locks.contains_key(blk) {
            LOCKTBL.lock().unwrap().s_lock(blk)?;
            self.locks.insert(blk.clone(), "S".to_string());
        }
        Ok(())
    }

    pub fn x_lock(&mut self, blk: &BlockId) -> Result<(), AbortError> {
        if !self.has_x_lock(blk) {
            self.s_lock(blk)?;
            self.locks.insert(blk.clone(), "X".to_string());
        }
        Ok(())
    }

    pub fn release(&mut self) {
        for (blk, _) in &self.locks {
            LOCKTBL.lock().unwrap().unlock(blk);
        }
        self.locks.clear();
    }

    fn has_x_lock(&self, blk: &BlockId) -> bool {
        if let Some(locktype) = self.locks.get(blk) {
            if locktype == "X" {
                return true;
            }
        }
        false
    }
}
