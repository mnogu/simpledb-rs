use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    file::blockid::BlockId,
    query::constant::Constant,
    record::{layout::Layout, rid::Rid, schema},
    tx::transaction::{Transaction, TransactionError},
};

pub struct BTPage {
    tx: Arc<Mutex<Transaction>>,
    currentblk: Option<BlockId>,
    layout: Layout,
}

impl BTPage {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        currentblk: BlockId,
        layout: Layout,
    ) -> Result<BTPage, AbortError> {
        tx.lock().unwrap().pin(&currentblk)?;
        Ok(BTPage {
            tx,
            currentblk: Some(currentblk),
            layout,
        })
    }

    pub fn find_slot_before(&self, searchkey: &Constant) -> Result<i32, TransactionError> {
        let mut slot = 0;
        while slot < self.get_num_recs()? && self.get_data_val(slot)? < *searchkey {
            slot += 1;
        }
        Ok(slot as i32 - 1)
    }

    pub fn close(&mut self) -> Result<(), AbortError> {
        if let Some(currentblk) = &self.currentblk {
            self.tx.lock().unwrap().unpin(currentblk)?;
        }
        self.currentblk = None;
        Ok(())
    }

    pub fn is_full(&self) -> Result<bool, TransactionError> {
        Ok(self.slotpos(self.get_num_recs()? + 1) >= self.tx.lock().unwrap().block_size())
    }

    pub fn split(&self, splitpos: usize, flag: i32) -> Result<BlockId, TransactionError> {
        let newblk = self.append_new(flag)?;
        let mut newpage = BTPage::new(self.tx.clone(), newblk.clone(), self.layout.clone())?;
        self.transfer_recs(splitpos, &newpage)?;
        newpage.set_flag(flag)?;
        newpage.close()?;
        Ok(newblk)
    }

    pub fn get_data_val(&self, slot: usize) -> Result<Constant, TransactionError> {
        self.get_val(slot, "dataval")
    }

    pub fn get_flag(&self) -> Result<i32, TransactionError> {
        if let Some(currentblk) = &self.currentblk {
            return self.tx.lock().unwrap().get_int(currentblk, 0);
        }
        Err(TransactionError::General)
    }

    pub fn set_flag(&self, val: i32) -> Result<(), TransactionError> {
        if let Some(currentblk) = &self.currentblk {
            self.tx.lock().unwrap().set_int(currentblk, 0, val, true)?;
            return Ok(());
        }
        Err(TransactionError::General)
    }

    pub fn append_new(&self, flag: i32) -> Result<BlockId, TransactionError> {
        if let Some(currentblk) = &self.currentblk {
            let mut tx = self.tx.lock().unwrap();
            let blk = tx.append(currentblk.file_name())?;
            tx.pin(&blk)?;
            self.format(&blk, flag)?;
            return Ok(blk);
        }
        Err(TransactionError::General)
    }

    pub fn format(&self, blk: &BlockId, flag: i32) -> Result<(), TransactionError> {
        self.tx.lock().unwrap().set_int(blk, 0, flag, false)?;
        let bytes = 4;
        self.tx.lock().unwrap().set_int(blk, bytes, 0, false)?;
        let recsize = self.layout.slot_size();
        let mut pos = 2 * bytes;
        while pos + recsize <= self.tx.lock().unwrap().block_size() {
            self.make_default_record(blk, pos)?;
            pos += recsize;
        }
        Ok(())
    }

    fn make_default_record(&self, blk: &BlockId, pos: usize) -> Result<(), TransactionError> {
        let mut tx = self.tx.lock().unwrap();
        for fldname in self.layout.schema().fields() {
            let offset = self.layout.offset(fldname);
            match self.layout.schema().type_(fldname) {
                schema::Type::Integer => tx.set_int(blk, pos + offset, 0, false)?,
                schema::Type::Varchar => tx.set_string(blk, pos + offset, "", false)?,
            };
        }
        Ok(())
    }

    pub fn get_child_num(&self, slot: usize) -> Result<i32, TransactionError> {
        self.get_int(slot, "block")
    }

    pub fn insert_dir(
        &self,
        slot: usize,
        val: Constant,
        blknum: i32,
    ) -> Result<(), TransactionError> {
        self.insert(slot)?;
        self.set_val(slot, "dataval", val)?;
        self.set_int(slot, "block", blknum)?;
        Ok(())
    }

    pub fn get_data_rid(&self, slot: usize) -> Result<Rid, TransactionError> {
        Ok(Rid::new(
            self.get_int(slot, "block")?,
            self.get_int(slot, "id")? as usize,
        ))
    }

    pub fn insert_leaf(
        &self,
        slot: usize,
        val: Constant,
        rid: &Rid,
    ) -> Result<(), TransactionError> {
        self.insert(slot)?;
        self.set_val(slot, "dataval", val)?;
        self.set_int(slot, "block", rid.block_number())?;
        self.set_int(slot, "id", rid.slot() as i32)?;
        Ok(())
    }

    pub fn delete(&self, slot: usize) -> Result<(), TransactionError> {
        for i in slot + 1..self.get_num_recs()? {
            self.copy_record(i, i - 1)?;
        }
        self.set_num_recs(self.get_num_recs()? - 1)?;
        Ok(())
    }

    pub fn get_num_recs(&self) -> Result<usize, TransactionError> {
        let bytes = 4;
        if let Some(currentblk) = &self.currentblk {
            return Ok(self.tx.lock().unwrap().get_int(currentblk, bytes)? as usize);
        }
        Err(TransactionError::General)
    }

    fn get_int(&self, slot: usize, fldname: &str) -> Result<i32, TransactionError> {
        let pos = self.fldpos(slot, fldname);
        if let Some(currentblk) = &self.currentblk {
            return self.tx.lock().unwrap().get_int(currentblk, pos);
        }
        Err(TransactionError::General)
    }

    fn get_string(&self, slot: usize, fldname: &str) -> Result<String, TransactionError> {
        let pos = self.fldpos(slot, fldname);
        if let Some(currentblk) = &self.currentblk {
            return self.tx.lock().unwrap().get_string(currentblk, pos);
        }
        Err(TransactionError::General)
    }

    fn get_val(&self, slot: usize, fldname: &str) -> Result<Constant, TransactionError> {
        let type_ = self.layout.schema().type_(fldname);
        match type_ {
            schema::Type::Integer => Ok(Constant::with_int(self.get_int(slot, fldname)?)),
            schema::Type::Varchar => Ok(Constant::with_string(&self.get_string(slot, fldname)?)),
        }
    }

    fn set_int(&self, slot: usize, fldname: &str, val: i32) -> Result<(), TransactionError> {
        let pos = self.fldpos(slot, fldname);
        if let Some(currentblk) = &self.currentblk {
            self.tx
                .lock()
                .unwrap()
                .set_int(currentblk, pos, val, true)?;
            return Ok(());
        }
        Err(TransactionError::General)
    }

    fn set_string(&self, slot: usize, fldname: &str, val: &str) -> Result<(), TransactionError> {
        let pos = self.fldpos(slot, fldname);
        if let Some(currentblk) = &self.currentblk {
            self.tx
                .lock()
                .unwrap()
                .set_string(currentblk, pos, val, true)?;
            return Ok(());
        }
        Err(TransactionError::General)
    }

    fn set_val(&self, slot: usize, fldname: &str, val: Constant) -> Result<(), TransactionError> {
        let type_ = self.layout.schema().type_(fldname);
        match type_ {
            schema::Type::Integer => {
                if let Some(val) = val.as_int() {
                    self.set_int(slot, fldname, val)
                } else {
                    Err(TransactionError::General)
                }
            }
            schema::Type::Varchar => {
                if let Some(val) = val.as_string() {
                    self.set_string(slot, fldname, &val)
                } else {
                    Err(TransactionError::General)
                }
            }
        }
    }

    fn set_num_recs(&self, n: usize) -> Result<(), TransactionError> {
        let bytes = 4;
        if let Some(currentblk) = &self.currentblk {
            self.tx
                .lock()
                .unwrap()
                .set_int(currentblk, bytes, n as i32, true)?;
            return Ok(());
        }
        Err(TransactionError::General)
    }

    fn insert(&self, slot: usize) -> Result<(), TransactionError> {
        for i in (slot + 1..=self.get_num_recs()?).rev() {
            self.copy_record(i - 1, i)?;
        }
        self.set_num_recs(self.get_num_recs()? + 1)?;
        Ok(())
    }

    fn copy_record(&self, from: usize, to: usize) -> Result<(), TransactionError> {
        let sch = self.layout.schema();
        for fldname in sch.fields() {
            self.set_val(to, fldname, self.get_val(from, fldname)?)?;
        }
        Ok(())
    }

    fn transfer_recs(&self, slot: usize, dest: &BTPage) -> Result<(), TransactionError> {
        let mut destslot = 0;
        while slot < self.get_num_recs()? {
            dest.insert(destslot)?;
            let sch = self.layout.schema();
            for fldname in sch.fields() {
                dest.set_val(destslot, fldname, self.get_val(slot, fldname)?)?;
            }
            self.delete(slot)?;
            destslot += 1;
        }
        Ok(())
    }

    fn fldpos(&self, slot: usize, fldname: &str) -> usize {
        let offset = self.layout.offset(fldname);
        self.slotpos(slot) + offset
    }

    fn slotpos(&self, slot: usize) -> usize {
        let slotsize = self.layout.slot_size();
        let bytes = 4;
        bytes + bytes + (slot * slotsize)
    }
}
