use std::sync::{Arc, Mutex};

use crate::{
    query::{scan::Scan, updatescan::UpdateScan},
    record::{schema::Schema, tablescan::TableScan},
    tx::transaction::{Transaction, TransactionError},
};

use super::tablemgr::TableMgr;

pub struct ViewMgr {
    tbl_mgr: Arc<TableMgr>,
}

impl ViewMgr {
    const MAX_VIEWDEF: usize = 100;

    pub fn new(
        is_new: bool,
        tbl_mgr: Arc<TableMgr>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<ViewMgr, TransactionError> {
        if is_new {
            let mut sch = Schema::new();
            sch.add_string_field("viewname", TableMgr::MAX_NAME);
            sch.add_string_field("viewdef", ViewMgr::MAX_VIEWDEF);
            tbl_mgr.create_table("viewcat", Arc::new(sch), tx)?;
        }
        Ok(ViewMgr { tbl_mgr })
    }

    pub fn create_view(
        &self,
        vname: &str,
        vdef: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), TransactionError> {
        let layout = self.tbl_mgr.get_layout("viewcat", tx.clone())?;
        let mut ts = TableScan::new(tx, "viewcat", Arc::new(layout))?;
        ts.insert()?;
        ts.set_string("viewname", vname)?;
        ts.set_string("viewdef", vdef)?;
        ts.close()?;
        Ok(())
    }

    pub fn get_view_def(
        &self,
        vname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<String, TransactionError> {
        let layout = self.tbl_mgr.get_layout("viewcat", tx.clone())?;
        let mut ts = TableScan::new(tx, "viewcat", Arc::new(layout))?;
        while ts.next()? {
            if ts.get_string("viewname")? == vname {
                let result = ts.get_string("viewdef")?;
                ts.close()?;
                return Ok(result);
            }
        }
        ts.close()?;
        Err(TransactionError::General)
    }
}
