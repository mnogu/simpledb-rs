use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    buffer::buffermgr::{AbortError, BufferMgr},
    file::blockid::BlockId,
};

pub struct BufferList {
    bm: Rc<RefCell<BufferMgr>>,
    buffers: HashMap<BlockId, usize>,
    pins: Vec<BlockId>,
}

impl BufferList {
    pub fn new(bm: Rc<RefCell<BufferMgr>>) -> BufferList {
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
        let idx = self.bm.borrow_mut().pin(&blk)?;
        self.buffers.insert(blk.clone(), idx);
        self.pins.push(blk.clone());
        Ok(())
    }

    pub(in crate::tx) fn unpin(&mut self, blk: &BlockId) -> Result<(), AbortError> {
        let idx = self.buffers.get(blk);
        if let Some(idx) = idx {
            self.bm.borrow_mut().unpin(*idx);
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
                self.bm.borrow_mut().unpin(*idx);
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }
}
