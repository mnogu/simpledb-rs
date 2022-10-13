use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    file::blockid::BlockId,
    query::{constant::Constant, scan::ScanControl},
    record::{layout::Layout, recordpage::RecordPage, schema::Type},
    tx::transaction::{Transaction, TransactionError},
};

pub struct ChunkScan {
    buffs: Vec<RecordPage>,
    tx: Arc<Mutex<Transaction>>,
    filename: String,
    layout: Arc<Layout>,
    startbnum: usize,
    endbnum: usize,
    currentbnum: usize,
    rpidx: usize,
    currentslot: Option<usize>,
}

impl ChunkScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        filename: &str,
        layout: Arc<Layout>,
        startbnum: usize,
        endbnum: usize,
    ) -> Result<ChunkScan, TransactionError> {
        let mut buffs = Vec::new();
        for i in startbnum..=endbnum {
            let blk = BlockId::new(filename, i as i32);
            buffs.push(RecordPage::new(tx.clone(), blk, layout.clone())?);
        }
        let mut s = ChunkScan {
            buffs,
            tx,
            filename: filename.to_string(),
            layout,
            startbnum,
            endbnum,
            currentbnum: 0,
            rpidx: usize::MAX,
            currentslot: None,
        };
        s.move_to_block(startbnum);
        Ok(s)
    }

    fn move_to_block(&mut self, blknum: usize) {
        self.currentbnum = blknum;
        self.rpidx = self.currentbnum - self.startbnum;
        self.currentslot = None;
    }
}

impl ScanControl for ChunkScan {
    fn close(&mut self) -> Result<(), AbortError> {
        for i in 0..self.buffs.len() {
            let blk = BlockId::new(&self.filename, (self.startbnum + i) as i32);
            self.tx.lock().unwrap().unpin(&blk)?;
        }
        Ok(())
    }

    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.move_to_block(self.startbnum);
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if let Some(rp) = self.buffs.get_mut(self.rpidx) {
            self.currentslot = rp.next_after(self.currentslot)?;
        }
        while self.currentslot.is_none() {
            if self.currentbnum == self.endbnum {
                return Ok(false);
            }
            let blknum = if let Some(rp) = self.buffs.get_mut(self.rpidx) {
                rp.block().number() as usize + 1
            } else {
                return Err(TransactionError::General);
            };
            self.move_to_block(blknum);
            if let Some(rp) = self.buffs.get_mut(self.rpidx) {
                self.currentslot = rp.next_after(self.currentslot)?;
            }
        }
        Ok(true)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if let Some(rp) = self.buffs.get_mut(self.rpidx) {
            if let Some(currentslot) = self.currentslot {
                return rp.get_int(currentslot, fldname);
            }
        }
        Err(TransactionError::General)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if let Some(rp) = self.buffs.get_mut(self.rpidx) {
            if let Some(currentslot) = self.currentslot {
                return rp.get_string(currentslot, fldname);
            }
        }
        Err(TransactionError::General)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        match self.layout.schema().type_(fldname) {
            Type::Integer => Ok(Constant::with_int(self.get_int(fldname)?)),
            Type::Varchar => Ok(Constant::with_string(&self.get_string(fldname)?)),
        }
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.layout.schema().has_field(fldname)
    }
}
