use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    file::blockid::BlockId,
    tx::transaction::{Transaction, TransactionError},
};

use super::{layout::Layout, schema::Type};

enum Flag {
    Empty = 0,
    Used = 1,
}

pub struct RecordPage {
    tx: Arc<Mutex<Transaction>>,
    blk: BlockId,
    layout: Arc<Layout>,
}

impl RecordPage {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        blk: BlockId,
        layout: Arc<Layout>,
    ) -> Result<RecordPage, AbortError> {
        tx.lock().unwrap().pin(&blk)?;
        Ok(RecordPage { tx, blk, layout })
    }

    pub fn get_int(&mut self, slot: usize, fldname: &str) -> Result<i32, TransactionError> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname);
        self.tx.lock().unwrap().get_int(&self.blk, fldpos)
    }

    pub fn get_string(&mut self, slot: usize, fldname: &str) -> Result<String, TransactionError> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname);
        self.tx.lock().unwrap().get_string(&self.blk, fldpos)
    }

    pub fn set_int(
        &mut self,
        slot: usize,
        fldname: &str,
        val: i32,
    ) -> Result<(), TransactionError> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname);
        self.tx
            .lock()
            .unwrap()
            .set_int(&self.blk, fldpos, val, true)
    }

    pub fn set_string(
        &mut self,
        slot: usize,
        fldname: &str,
        val: &str,
    ) -> Result<(), TransactionError> {
        let fldpos = self.offset(slot) + self.layout.offset(fldname);
        self.tx
            .lock()
            .unwrap()
            .set_string(&self.blk, fldpos, val, true)
    }

    pub fn delete(&mut self, slot: usize) -> Result<(), TransactionError> {
        self.set_flag(slot, Flag::Empty)
    }

    pub fn format(&mut self) -> Result<(), TransactionError> {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            self.tx.lock().unwrap().set_int(
                &self.blk,
                self.offset(slot),
                Flag::Empty as i32,
                false,
            )?;
            let sch = self.layout.schema();
            for fldname in sch.fields() {
                let fldpos = self.offset(slot) + self.layout.offset(fldname);
                match sch.type_(fldname) {
                    Type::Integer => {
                        self.tx
                            .lock()
                            .unwrap()
                            .set_int(&self.blk, fldpos, 0, false)?;
                    }
                    Type::Varchar => {
                        self.tx
                            .lock()
                            .unwrap()
                            .set_string(&self.blk, fldpos, "", false)?;
                    }
                }
            }
            slot += 1;
        }
        Ok(())
    }

    pub fn next_after(&mut self, slot: Option<usize>) -> Result<Option<usize>, TransactionError> {
        self.search_after(slot, Flag::Used)
    }

    pub fn insert_after(&mut self, slot: Option<usize>) -> Result<Option<usize>, TransactionError> {
        let newslot = self.search_after(slot, Flag::Empty)?;
        if let Some(newslot) = newslot {
            self.set_flag(newslot, Flag::Used)?;
        }
        Ok(newslot)
    }

    pub fn block(&self) -> &BlockId {
        &self.blk
    }

    fn set_flag(&mut self, slot: usize, flag: Flag) -> Result<(), TransactionError> {
        self.tx
            .lock()
            .unwrap()
            .set_int(&self.blk, self.offset(slot), flag as i32, true)
    }

    fn search_after(
        &mut self,
        slot: Option<usize>,
        flag: Flag,
    ) -> Result<Option<usize>, TransactionError> {
        let mut s = 0;
        if let Some(slot) = slot {
            s = slot + 1;
        }
        let f = flag as i32;
        while self.is_valid_slot(s) {
            if self.tx.lock().unwrap().get_int(&self.blk, self.offset(s))? == f {
                return Ok(Some(s));
            }
            s += 1;
        }
        Ok(None)
    }

    fn is_valid_slot(&self, slot: usize) -> bool {
        self.offset(slot + 1) <= self.tx.lock().unwrap().block_size()
    }

    fn offset(&self, slot: usize) -> usize {
        slot * self.layout.slot_size()
    }
}
