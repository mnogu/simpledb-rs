use std::sync::{Arc, Mutex};

use crate::{
    metadata::{metadatamgr::MetadataMgr, statinfo::StatInfo},
    query::scan::Scan,
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    tx::transaction::{Transaction, TransactionError},
};

use super::plan::Plan;

pub struct TablePlan {
    tblname: String,
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    si: StatInfo,
}

impl TablePlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        tblname: &str,
        md: Arc<Mutex<MetadataMgr>>,
    ) -> Result<TablePlan, TransactionError> {
        let layout = Arc::new(md.lock().unwrap().get_layout(tblname, tx.clone())?);
        let si = md
            .lock()
            .unwrap()
            .get_stat_info(tblname, layout.clone(), tx.clone())?;
        Ok(TablePlan {
            tblname: tblname.to_string(),
            tx,
            layout,
            si,
        })
    }
}

impl Plan for TablePlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        Ok(TableScan::new(self.tx.clone(), &self.tblname, self.layout.clone())?.into())
    }

    fn schema(&self) -> Arc<Schema> {
        self.layout.schema()
    }
}
