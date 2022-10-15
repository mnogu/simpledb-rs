use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use crate::{
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    tx::transaction::{Transaction, TransactionError},
};

pub struct TempTable {
    tx: Arc<Mutex<Transaction>>,
    tblname: String,
    layout: Layout,
}

static NEXT_TABLE_NUM: AtomicUsize = AtomicUsize::new(0);

fn next_table_name() -> String {
    NEXT_TABLE_NUM.fetch_add(1, Ordering::SeqCst);
    format!("temp{}", NEXT_TABLE_NUM.load(Ordering::SeqCst))
}

impl TempTable {
    pub fn new(tx: Arc<Mutex<Transaction>>, sch: Arc<Schema>) -> TempTable {
        let tblname = next_table_name();
        let layout = Layout::new(sch);
        TempTable {
            tx,
            tblname,
            layout,
        }
    }

    pub fn open(&self) -> Result<TableScan, TransactionError> {
        TableScan::new(self.tx.clone(), &self.tblname, self.layout.clone())
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn get_layout(&self) -> Layout {
        self.layout.clone()
    }
}
