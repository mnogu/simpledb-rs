use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::buffermgr::{AbortError, BufferMgr},
    file::blockid::BlockId,
};

pub struct BufferList {
    bm: Arc<Mutex<BufferMgr>>,
    buffers: HashMap<BlockId, usize>,
    pins: Vec<BlockId>,
}

impl BufferList {
    pub fn new(bm: Arc<Mutex<BufferMgr>>) -> BufferList {
        BufferList {
            bm,
            buffers: HashMap::new(),
            pins: Vec::new(),
        }
    }

    pub(in crate::tx) fn get_index(&self, blk: &BlockId) -> Option<usize> {
        let idx = self.buffers.get(blk);
        if let Some(idx) = idx {
            return Some(*idx);
        }
        None
    }

    pub(in crate::tx) fn pin(&mut self, blk: &BlockId) -> Result<(), AbortError> {
        let idx = self.bm.lock().unwrap().pin(&blk)?;
        self.buffers.insert(blk.clone(), idx);
        self.pins.push(blk.clone());
        Ok(())
    }

    pub(in crate::tx) fn unpin(&mut self, blk: &BlockId) -> Result<(), AbortError> {
        let idx = self.buffers.get(blk);
        if let Some(idx) = idx {
            self.bm.lock().unwrap().unpin(*idx);
            if !self.pins.contains(blk) {
                self.buffers.remove(blk);
            }
            return Ok(());
        }
        Err(AbortError::General)
    }

    pub(in crate::tx) fn unpin_all(&mut self) {
        for blk in &self.pins {
            if let Some(idx) = self.buffers.get(&blk) {
                self.bm.lock().unwrap().unpin(*idx);
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }
}
