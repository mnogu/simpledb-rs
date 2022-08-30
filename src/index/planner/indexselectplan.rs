use std::sync::Arc;

use crate::{
    index::query::indexselectscan::IndexSelectScan,
    metadata::indexinfo::IndexInfo,
    plan::plan::Plan,
    query::{contant::Constant, scan::Scan},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

pub struct IndexSelectPlan<'a> {
    p: Box<dyn Plan>,
    ii: &'a IndexInfo,
    val: Constant,
}

impl<'a> IndexSelectPlan<'a> {
    pub fn new(p: Box<dyn Plan>, ii: &IndexInfo, val: Constant) -> IndexSelectPlan {
        IndexSelectPlan { p, ii, val }
    }
}

impl<'a> Plan for IndexSelectPlan<'a> {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p.open()?;
        if let Scan::Table(ts) = s {
            let idx = self.ii.open()?;
            return Ok(IndexSelectScan::new(ts, idx, self.val.clone())?.into());
        }
        Err(TransactionError::General)
    }

    fn schema(&self) -> Arc<Schema> {
        self.p.schema()
    }
}
