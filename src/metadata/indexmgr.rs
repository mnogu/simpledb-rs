use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::{scan::Scan, updatescan::UpdateScan},
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    tx::transaction::{Transaction, TransactionError},
};

use super::{indexinfo::IndexInfo, statmgr::StatMgr, tablemgr::TableMgr};

pub struct IndexMgr {
    layout: Arc<Layout>,
    tblmgr: Arc<TableMgr>,
    statmgr: Arc<Mutex<StatMgr>>,
}

impl IndexMgr {
    pub fn new(
        isnew: bool,
        tblmgr: Arc<TableMgr>,
        statmgr: Arc<Mutex<StatMgr>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<IndexMgr, TransactionError> {
        if isnew {
            let mut sch = Schema::new();
            sch.add_string_field("indexname", TableMgr::MAX_NAME);
            sch.add_string_field("tablename", TableMgr::MAX_NAME);
            sch.add_string_field("fieldname", TableMgr::MAX_NAME);
            tblmgr.create_table("idxcat", Arc::new(sch), tx.clone())?;
        }
        let layout = Arc::new(tblmgr.get_layout("idxcat", tx)?);
        Ok(IndexMgr {
            layout,
            tblmgr,
            statmgr,
        })
    }

    pub fn create_index(
        &self,
        idxname: &str,
        tblname: &str,
        fldname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), TransactionError> {
        let mut ts = TableScan::new(tx, "idxcat", self.layout.clone())?;
        ts.insert()?;
        ts.set_string("indexname", idxname)?;
        ts.set_string("tablename", tblname)?;
        ts.set_string("fieldname", fldname)?;
        Ok(())
    }

    pub fn get_index_info(
        &self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>, TransactionError> {
        let mut result = HashMap::new();
        let mut ts = TableScan::new(tx.clone(), "idxcat", self.layout.clone())?;
        while ts.next()? {
            if ts.get_string("tablename")? == tblname {
                let idxname = ts.get_string("indexname")?;
                let fldname = ts.get_string("fieldname")?;
                let tbl_layout = Arc::new(self.tblmgr.get_layout(tblname, tx.clone())?);
                let tblsi = self.statmgr.lock().unwrap().get_stat_info(
                    tblname,
                    tbl_layout.clone(),
                    tx.clone(),
                )?;
                let ii = IndexInfo::new(&idxname, &fldname, tbl_layout.schema(), tx.clone(), tblsi);
                result.insert(fldname, ii);
            }
        }
        ts.close()?;
        Ok(result)
    }
}
