use std::sync::Arc;

use crate::{
    index::query::indexjoinscan::IndexJoinScan, metadata::indexinfo::IndexInfo, plan::plan::Plan,
    query::scan::Scan, record::schema::Schema, tx::transaction::TransactionError,
};

pub struct IndexJoinPlan<'a> {
    p1: Box<dyn Plan>,
    p2: Box<dyn Plan>,
    ii: &'a IndexInfo,
    joinfield: String,
    sch: Arc<Schema>,
}

impl<'a> IndexJoinPlan<'a> {
    pub fn new(
        p1: Box<dyn Plan>,
        p2: Box<dyn Plan>,
        ii: &'a IndexInfo,
        joinfield: &str,
    ) -> IndexJoinPlan<'a> {
        let mut sch = Schema::new();
        sch.add_all(&p1.schema());
        sch.add_all(&p2.schema());
        IndexJoinPlan {
            p1,
            p2,
            ii,
            joinfield: joinfield.to_string(),
            sch: Arc::new(sch),
        }
    }
}

impl<'a> Plan for IndexJoinPlan<'a> {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p1.open()?;
        if let Scan::Table(ts) = self.p2.open()? {
            let idx = self.ii.open()?;
            return Ok(IndexJoinScan::new(s, idx, &self.joinfield, ts)?.into());
        }
        Err(TransactionError::General)
    }

    fn schema(&self) -> Arc<Schema> {
        self.sch.clone()
    }
}
