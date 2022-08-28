use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    file::blockid::BlockId,
    query::contant::Constant,
    record::layout::Layout,
    tx::transaction::{Transaction, TransactionError},
};

use super::{btpage::BTPage, direntry::DirEntry};

pub struct BTreeDir {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    contents: BTPage,
    filename: String,
}

impl BTreeDir {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        blk: BlockId,
        layout: Arc<Layout>,
    ) -> Result<BTreeDir, AbortError> {
        let contents = BTPage::new(tx.clone(), blk.clone(), layout.clone())?;
        let filename = blk.file_name().to_string();
        Ok(BTreeDir {
            tx,
            layout,
            contents,
            filename,
        })
    }

    pub fn close(&mut self) -> Result<(), AbortError> {
        self.contents.close()
    }

    pub fn search(&mut self, searchkey: &Constant) -> Result<i32, TransactionError> {
        let mut childblk = self.find_child_block(searchkey)?;
        while self.contents.get_flag()? > 0 {
            self.contents.close()?;
            self.contents = BTPage::new(self.tx.clone(), childblk, self.layout.clone())?;
            childblk = self.find_child_block(searchkey)?;
        }
        Ok(childblk.number())
    }

    pub fn make_new_root(&self, e: &DirEntry) -> Result<(), TransactionError> {
        let firstval = self.contents.get_data_val(0)?;
        let level = self.contents.get_flag()?;
        let newblk = self.contents.split(0, level)?;
        let oldroot = DirEntry::new(firstval, newblk.number());
        self.insert_entry(&oldroot)?;
        self.insert_entry(&e)?;
        self.contents.set_flag(level + 1)?;
        Ok(())
    }

    pub fn insert(&self, e: &DirEntry) -> Result<Option<DirEntry>, TransactionError> {
        if self.contents.get_flag()? == 0 {
            return Ok(self.insert_entry(e)?);
        }
        let childblk = self.find_child_block(&e.data_val())?;
        let mut child = BTreeDir::new(self.tx.clone(), childblk, self.layout.clone())?;
        let myentry = child.insert(e)?;
        child.close()?;
        if let Some(myentry) = &myentry {
            return Ok(self.insert_entry(myentry)?);
        }
        Ok(None)
    }

    fn insert_entry(&self, e: &DirEntry) -> Result<Option<DirEntry>, TransactionError> {
        let newslot = (1 + self.contents.find_slot_before(&e.data_val())?) as usize;
        self.contents
            .insert_dir(newslot, e.data_val(), e.block_number())?;
        if !self.contents.is_full()? {
            return Ok(None);
        }
        let level = self.contents.get_flag()?;
        let splitpos = self.contents.get_num_recs()? / 2;
        let splitval = self.contents.get_data_val(splitpos)?;
        let newblk = self.contents.split(splitpos, level)?;
        Ok(Some(DirEntry::new(splitval, newblk.number())))
    }

    fn find_child_block(&self, searchkey: &Constant) -> Result<BlockId, TransactionError> {
        let mut slot = self.contents.find_slot_before(searchkey)?;
        if self.contents.get_data_val((slot + 1) as usize)? == *searchkey {
            slot += 1;
        }
        let blknum = self.contents.get_child_num(slot as usize)?;
        Ok(BlockId::new(&self.filename, blknum))
    }
}
