use std::{
    collections::HashMap,
    thread::{current, park_timeout},
    time::{Duration, SystemTime},
};

use crate::{
    buffer::buffermgr::{waiting_too_long, AbortError},
    file::blockid::BlockId,
};

pub struct LockTable {
    locks: HashMap<BlockId, i32>,
    max_time: u128,
}

const MAX_TIME: u128 = 10000;

impl LockTable {
    pub fn new() -> LockTable {
        LockTable {
            locks: HashMap::new(),
            max_time: MAX_TIME,
        }
    }

    pub fn s_lock(&mut self, blk: &BlockId) -> Result<(), AbortError> {
        let timestamp = SystemTime::now();
        while self.has_x_lock(blk) && !waiting_too_long(timestamp, self.max_time)? {
            park_timeout(Duration::from_millis(self.max_time as u64));
        }
        if self.has_x_lock(&blk) {
            return Err(AbortError::General);
        }
        let val = self.get_lock_val(&blk);
        self.locks.insert(blk.clone(), val + 1);
        Ok(())
    }

    pub(in crate::tx::concurrency) fn unlock(&mut self, blk: &BlockId) {
        let val = self.get_lock_val(blk);
        if val > 1 {
            self.locks.insert(blk.clone(), val - 1);
        } else {
            self.locks.remove(blk);
            current().unpark();
        }
    }

    fn has_x_lock(&self, blk: &BlockId) -> bool {
        self.get_lock_val(&blk) < 0
    }

    fn get_lock_val(&self, blk: &BlockId) -> i32 {
        if let Some(ival) = self.locks.get(blk) {
            return *ival;
        }
        0
    }
}
