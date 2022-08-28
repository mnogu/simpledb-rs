use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    file::blockid::BlockId,
    query::contant::Constant,
    record::{layout::Layout, rid::Rid},
    tx::transaction::{Transaction, TransactionError},
};

use super::{btpage::BTPage, direntry::DirEntry};

pub struct BTreeLeaf {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    searchkey: Constant,
    contents: BTPage,
    currentslot: i32,
    filename: String,
}

impl BTreeLeaf {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        blk: BlockId,
        layout: Arc<Layout>,
        searchkey: Constant,
    ) -> Result<BTreeLeaf, TransactionError> {
        let contents = BTPage::new(tx.clone(), blk.clone(), layout.clone())?;
        let currentslot = contents.find_slot_before(&searchkey)?;
        let filename = blk.file_name().to_string();
        Ok(BTreeLeaf {
            tx,
            layout,
            searchkey,
            contents,
            currentslot,
            filename,
        })
    }

    pub fn close(&mut self) -> Result<(), AbortError> {
        self.contents.close()
    }

    pub fn next(&mut self) -> Result<bool, TransactionError> {
        self.currentslot += 1;
        if self.currentslot >= self.contents.get_num_recs()? as i32 {
            return self.try_overflow();
        }
        if self.contents.get_data_val(self.currentslot as usize)? == self.searchkey {
            return Ok(true);
        }
        self.try_overflow()
    }

    pub fn get_data_rid(&self) -> Result<Rid, TransactionError> {
        self.contents.get_data_rid(self.currentslot as usize)
    }

    pub fn delete(&mut self, dataid: Rid) -> Result<(), TransactionError> {
        while self.next()? {
            if self.get_data_rid()? == dataid {
                self.contents.delete(self.currentslot as usize)?;
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, datarid: Rid) -> Result<Option<DirEntry>, TransactionError> {
        if self.contents.get_flag()? >= 0 && self.contents.get_data_val(0)? > self.searchkey {
            let firstval = self.contents.get_data_val(0)?;
            let newblk = self.contents.split(0, self.contents.get_flag()?)?;
            self.currentslot = 0;
            self.contents.set_flag(-1)?;
            self.contents.insert_leaf(
                self.currentslot as usize,
                self.searchkey.clone(),
                &datarid,
            )?;
            return Ok(Some(DirEntry::new(firstval, newblk.number())));
        }

        self.currentslot += 1;
        self.contents
            .insert_leaf(self.currentslot as usize, self.searchkey.clone(), &datarid)?;
        if !self.contents.is_full()? {
            return Ok(None);
        }

        let firstkey = self.contents.get_data_val(0)?;
        let lastkey = self
            .contents
            .get_data_val(self.contents.get_num_recs()? - 1)?;
        if lastkey == firstkey {
            let newblk = self.contents.split(1, self.contents.get_flag()?)?;
            self.contents.set_flag(newblk.number())?;
            return Ok(None);
        }
        let mut splitpos = self.contents.get_num_recs()? / 2;
        let mut splitkey = self.contents.get_data_val(splitpos)?;
        if splitkey == firstkey {
            while self.contents.get_data_val(splitpos)? == splitkey {
                splitpos += 1;
            }
            splitkey = self.contents.get_data_val(splitpos)?;
        } else {
            while self.contents.get_data_val(splitpos - 1)? == splitkey {
                splitpos -= 1;
            }
        }
        let newblk = self.contents.split(splitpos, -1)?;
        Ok(Some(DirEntry::new(splitkey, newblk.number())))
    }

    fn try_overflow(&mut self) -> Result<bool, TransactionError> {
        let firstkey = self.contents.get_data_val(0)?;
        let flag = self.contents.get_flag()?;
        if self.searchkey != firstkey || flag < 0 {
            return Ok(false);
        }
        self.contents.close()?;
        let nextblk = BlockId::new(&self.filename, flag);
        self.contents = BTPage::new(self.tx.clone(), nextblk, self.layout.clone())?;
        self.currentslot = 0;
        Ok(true)
    }
}
