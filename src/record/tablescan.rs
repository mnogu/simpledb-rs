use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    file::blockid::BlockId,
    query::{contant::Constant, scan::Scan, updatescan::UpdateScan},
    tx::transaction::{Transaction, TransactionError},
};

use super::{layout::Layout, recordpage::RecordPage, rid::Rid};

pub struct TableScan {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    rp: Option<RecordPage>,
    filename: String,
    currentslot: Option<usize>,
}

impl Scan for TableScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        Ok(self.move_to_block(0)?)
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if let Some(rp) = &mut self.rp {
            self.currentslot = rp.next_after(self.currentslot)?;
        } else {
            return Err(TransactionError::General);
        }
        while self.currentslot.is_none() {
            if self.at_last_block()? {
                return Ok(false);
            }
            let mut blknum = None;
            if let Some(rp) = &self.rp {
                blknum = Some(rp.block().number() + 1);
            }
            if let Some(blknum) = blknum {
                self.move_to_block(blknum)?;
            } else {
                return Err(TransactionError::General);
            }
            if let Some(rp) = &mut self.rp {
                self.currentslot = rp.next_after(self.currentslot)?;
            } else {
                return Err(TransactionError::General);
            }
        }
        Ok(true)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if let Some(rp) = &mut self.rp {
            if let Some(currentslot) = self.currentslot {
                return Ok(rp.get_int(currentslot, fldname)?);
            }
        }
        Err(TransactionError::General)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if let Some(rp) = &mut self.rp {
            if let Some(currentslot) = self.currentslot {
                return Ok(rp.get_string(currentslot, fldname)?);
            }
        }
        Err(TransactionError::General)
    }

    fn get_val(&self, _fldname: &str) -> Constant {
        todo!()
    }

    fn has_field(&self, _fldname: &str) -> bool {
        todo!()
    }

    fn close(&mut self) -> Result<(), AbortError> {
        if let Some(rp) = &self.rp {
            self.tx.lock().unwrap().unpin(rp.block())?;
        }
        Ok(())
    }
}

impl UpdateScan for TableScan {
    fn set_val(&self, _fldname: &str, _val: Constant) {
        todo!()
    }

    fn set_int(&mut self, fldname: &str, val: i32) -> Result<(), TransactionError> {
        if let Some(rp) = &mut self.rp {
            if let Some(currentslot) = self.currentslot {
                rp.set_int(currentslot, fldname, val)?;
                return Ok(());
            }
        }
        Err(TransactionError::General)
    }

    fn set_string(&mut self, fldname: &str, val: &str) -> Result<(), TransactionError> {
        if let Some(rp) = &mut self.rp {
            if let Some(currentslot) = self.currentslot {
                rp.set_string(currentslot, fldname, val)?;
                return Ok(());
            }
        }
        Err(TransactionError::General)
    }

    fn insert(&mut self) -> Result<(), TransactionError> {
        if let Some(rp) = &mut self.rp {
            self.currentslot = rp.insert_after(self.currentslot)?;
        } else {
            return Err(TransactionError::General);
        }
        while self.currentslot.is_none() {
            if self.at_last_block()? {
                self.move_to_new_block()?;
            } else {
                let mut blknum = None;
                if let Some(rp) = &self.rp {
                    blknum = Some(rp.block().number() + 1);
                }
                if let Some(blknum) = blknum {
                    self.move_to_block(blknum)?;
                } else {
                    return Err(TransactionError::General);
                }
            }
            if let Some(rp) = &mut self.rp {
                self.currentslot = rp.insert_after(self.currentslot)?;
            } else {
                return Err(TransactionError::General);
            }
        }
        Ok(())
    }

    fn delete(&mut self) -> Result<(), TransactionError> {
        if let Some(rp) = &mut self.rp {
            if let Some(currentslot) = self.currentslot {
                rp.delete(currentslot)?;
                return Ok(());
            }
        }
        Err(TransactionError::General)
    }

    fn get_rid(&self) -> Option<Rid> {
        if let Some(rp) = &self.rp {
            if let Some(currentslot) = self.currentslot {
                return Some(Rid::new(rp.block().number(), currentslot));
            }
        }
        None
    }

    fn move_to_rid(&self, _rid: &Rid) {
        todo!()
    }
}

impl TableScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        tablname: &str,
        layout: Arc<Layout>,
    ) -> Result<TableScan, TransactionError> {
        let filename = format!("{}.tbl", tablname);
        let mut t = TableScan {
            tx: tx.clone(),
            layout,
            rp: None,
            filename: filename.clone(),
            currentslot: None,
        };
        if tx.lock().unwrap().size(&filename)? == 0 {
            t.move_to_new_block()?;
        } else {
            t.move_to_block(0)?;
        }
        Ok(t)
    }

    fn move_to_block(&mut self, blknum: i32) -> Result<(), TransactionError> {
        self.close()?;
        let blk = BlockId::new(&self.filename, blknum);
        self.rp = Some(RecordPage::new(self.tx.clone(), blk, self.layout.clone())?);
        self.currentslot = None;
        Ok(())
    }

    fn move_to_new_block(&mut self) -> Result<(), TransactionError> {
        self.close()?;
        let blk = self.tx.lock().unwrap().append(&self.filename)?;
        let mut rp = RecordPage::new(self.tx.clone(), blk, self.layout.clone())?;
        rp.format()?;
        self.rp = Some(rp);
        self.currentslot = None;
        Ok(())
    }

    fn at_last_block(&self) -> Result<bool, TransactionError> {
        if let Some(rp) = &self.rp {
            return Ok(
                rp.block().number() as usize == self.tx.lock().unwrap().size(&self.filename)? - 1
            );
        }
        Ok(false)
    }
}
