use std::{
    cell::RefCell,
    io::Error,
    rc::Rc,
    sync::Mutex,
    thread::{current, park_timeout},
    time::{Duration, SystemTime, SystemTimeError},
};

use crate::{
    file::{blockid::BlockId, filemgr::FileMgr},
    log::logmgr::LogMgr,
};

use super::buffer::Buffer;

const MAX_TIME: u128 = 10000;

#[derive(Debug)]
pub enum BufferAbortError {
    Time(SystemTimeError),
    IO(Error),
    General,
}

impl From<SystemTimeError> for BufferAbortError {
    fn from(e: SystemTimeError) -> Self {
        BufferAbortError::Time(e)
    }
}

impl From<Error> for BufferAbortError {
    fn from(e: Error) -> Self {
        BufferAbortError::IO(e)
    }
}

pub struct BufferMgr {
    bufferpool: Vec<Buffer>,
    num_available: usize,
}

fn waiting_too_long(starttime: SystemTime) -> Result<bool, SystemTimeError> {
    Ok(SystemTime::now().duration_since(starttime)?.as_millis() > MAX_TIME)
}

impl BufferMgr {
    pub fn new(fm: Rc<FileMgr>, lm: Rc<RefCell<LogMgr>>, numbuffs: usize) -> BufferMgr {
        let mut bufferpool = Vec::with_capacity(numbuffs);
        let num_available = numbuffs;
        for _ in 0..numbuffs {
            bufferpool.push(Buffer::new(fm.clone(), Rc::clone(&lm)));
        }
        BufferMgr {
            bufferpool,
            num_available,
        }
    }

    pub fn unpin(&mut self, idx: usize) {
        let m = Mutex::new(&mut self.bufferpool[idx]);
        let mut buff = m.lock().unwrap();

        buff.unpin();
        if !buff.is_pinned() {
            self.num_available += 1;
            current().unpark();
        }
    }

    pub fn pin(&mut self, blk: &BlockId) -> Result<usize, BufferAbortError> {
        let m = Mutex::new(SystemTime::now());
        let timestamp = m.lock().unwrap();

        let mut idx = self.try_to_pin(blk)?;
        while idx.is_none() && !waiting_too_long(*timestamp)? {
            park_timeout(Duration::from_millis(MAX_TIME as u64));
            idx = self.try_to_pin(blk)?;
        }
        match idx {
            Some(idx) => Ok(idx),
            None => Err(BufferAbortError::General),
        }
    }

    pub fn buffer(&mut self, idx: usize) -> &mut Buffer {
        &mut self.bufferpool[idx]
    }

    fn try_to_pin(&mut self, blk: &BlockId) -> Result<Option<usize>, Error> {
        let mut idx = self.find_existing_buffer(&blk);
        if let None = idx {
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
}
