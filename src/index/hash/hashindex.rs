use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

use crate::{
    buffer::buffermgr::AbortError,
    index::index::IndexControl,
    query::{constant::Constant, scan::ScanControl, updatescan::UpdateScanControl},
    record::{layout::Layout, rid::Rid, tablescan::TableScan},
    tx::transaction::{Transaction, TransactionError},
};

pub struct HashIndex {
    tx: Arc<Mutex<Transaction>>,
    idxname: String,
    layout: Layout,
    searchkey: Option<Constant>,
    ts: Option<TableScan>,
}

impl HashIndex {
    const NUM_BUCKETS: usize = 100;

    #[allow(dead_code)]
    pub fn new(tx: Arc<Mutex<Transaction>>, idxname: &str, layout: Layout) -> HashIndex {
        HashIndex {
            tx,
            idxname: idxname.to_string(),
            layout,
            searchkey: None,
            ts: None,
        }
    }

    #[allow(dead_code)]
    pub fn search_cost(numblocks: usize, _rpb: usize) -> usize {
        numblocks / HashIndex::NUM_BUCKETS
    }
}

impl IndexControl for HashIndex {
    fn before_first(&mut self, searchkey: Constant) -> Result<(), TransactionError> {
        self.close()?;
        self.searchkey = Some(searchkey.clone());
        let mut s = DefaultHasher::new();
        searchkey.hash(&mut s);
        let bucket = s.finish() as usize % HashIndex::NUM_BUCKETS;
        let tblname = format!("{}{}", self.idxname, bucket);
        self.ts = Some(TableScan::new(
            self.tx.clone(),
            &tblname,
            self.layout.clone(),
        )?);
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if let Some(ts) = &mut self.ts {
            if let Some(searchkey) = &self.searchkey {
                while ts.next()? {
                    if ts.get_val("dataval")? == searchkey.clone() {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn get_data_rid(&mut self) -> Result<Rid, TransactionError> {
        if let Some(ts) = &mut self.ts {
            let blknum = ts.get_int("block")?;
            let id = ts.get_int("id")?;
            return Ok(Rid::new(blknum, id as usize));
        }
        Err(TransactionError::General)
    }

    fn insert(&mut self, val: Constant, rid: &Rid) -> Result<(), TransactionError> {
        self.before_first(val.clone())?;
        if let Some(ts) = &mut self.ts {
            ts.insert()?;
            ts.set_int("block", rid.block_number())?;
            ts.set_int("id", rid.slot() as i32)?;
            ts.set_val("dataval", val)?;
            return Ok(());
        }
        Err(TransactionError::General)
    }

    fn delete(&mut self, val: Constant, rid: &Rid) -> Result<(), TransactionError> {
        self.before_first(val)?;
        while self.next()? {
            if self.get_data_rid()? == *rid {
                if let Some(ts) = &mut self.ts {
                    ts.delete()?;
                }
                return Ok(());
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), AbortError> {
        if let Some(ts) = &mut self.ts {
            ts.close()?;
        }
        Ok(())
    }
}
