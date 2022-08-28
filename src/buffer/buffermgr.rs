use std::{
    io::Error,
    sync::{Arc, Mutex},
    thread::{current, park_timeout},
    time::{Duration, SystemTime, SystemTimeError},
};

use crate::{
    file::{blockid::BlockId, filemgr::FileMgr},
    log::logmgr::LogMgr,
};

use super::buffer::Buffer;

#[derive(Debug)]
pub enum AbortError {
    Time(SystemTimeError),
    IO(Error),
    General,
}

impl From<SystemTimeError> for AbortError {
    fn from(e: SystemTimeError) -> Self {
        AbortError::Time(e)
    }
}

impl From<Error> for AbortError {
    fn from(e: Error) -> Self {
        AbortError::IO(e)
    }
}

pub struct BufferMgr {
    bufferpool: Vec<Buffer>,
    num_available: usize,
    max_time: u128,
}

pub fn waiting_too_long(starttime: SystemTime, max_time: u128) -> Result<bool, SystemTimeError> {
    Ok(SystemTime::now().duration_since(starttime)?.as_millis() > max_time)
}

impl BufferMgr {
    const MAX_TIME: u128 = 10000;

    pub fn new(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>, numbuffs: usize) -> BufferMgr {
        let mut bufferpool = Vec::with_capacity(numbuffs);
        let num_available = numbuffs;
        for _ in 0..numbuffs {
            bufferpool.push(Buffer::new(fm.clone(), lm.clone()));
        }
        BufferMgr {
            bufferpool,
            num_available,
            max_time: BufferMgr::MAX_TIME,
        }
    }

    pub fn available(&self) -> usize {
        self.num_available
    }

    pub fn flush_all(&mut self, txnum: usize) -> Result<(), Error> {
        for buff in self.bufferpool.iter_mut() {
            if let Some(t) = buff.modifying_tx() {
                if t == txnum {
                    buff.flush()?;
                }
            }
        }
        Ok(())
    }

    pub fn unpin(&mut self, idx: usize) {
        let buff = &mut self.bufferpool[idx];
        buff.unpin();
        if !buff.is_pinned() {
            self.num_available += 1;
            current().unpark();
        }
    }

    pub fn pin(&mut self, blk: &BlockId) -> Result<usize, AbortError> {
        let timestamp = SystemTime::now();
        let mut idx = self.try_to_pin(blk)?;
        while idx.is_none() && !waiting_too_long(timestamp, self.max_time)? {
            park_timeout(Duration::from_millis(self.max_time as u64));
            idx = self.try_to_pin(blk)?;
        }
        match idx {
            Some(idx) => Ok(idx),
            None => Err(AbortError::General),
        }
    }

    pub fn buffer(&mut self, idx: usize) -> &mut Buffer {
        &mut self.bufferpool[idx]
    }

    fn try_to_pin(&mut self, blk: &BlockId) -> Result<Option<usize>, Error> {
        let mut idx = self.find_existing_buffer(blk);
        if idx.is_none() {
            idx = self.choose_unpinned_buffer();
            if let Some(i) = idx {
                self.bufferpool[i].assign_to_block(blk.clone())?;
            } else {
                return Ok(None);
            }
        }
        if let Some(i) = idx {
            if !self.bufferpool[i].is_pinned() {
                self.num_available -= 1;
            }
            self.bufferpool[i].pin();
        }
        Ok(idx)
    }

    fn find_existing_buffer(&mut self, blk: &BlockId) -> Option<usize> {
        for (i, buffer) in self.bufferpool.iter().enumerate() {
            let o = buffer.block();
            if let Some(b) = o {
                if b == blk {
                    return Some(i);
                }
            }
        }
        None
    }

    fn choose_unpinned_buffer(&mut self) -> Option<usize> {
        for (i, buffer) in self.bufferpool.iter().enumerate() {
            if !buffer.is_pinned() {
                return Some(i);
            }
        }
        None
    }

    pub fn set_max_time(&mut self, max_time: u128) {
        self.max_time = max_time;
    }
}
