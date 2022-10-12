use std::sync::Arc;

use crate::{
    index::query::indexselectscan::IndexSelectScan,
    metadata::indexinfo::IndexInfo,
    plan::plan::{Plan, PlanControl},
    query::{contant::Constant, scan::Scan},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

#[derive(Clone)]
pub struct IndexSelectPlan {
    p: Box<Plan>,
    ii: IndexInfo,
    val: Constant,
}

impl IndexSelectPlan {
    pub fn new(p: Plan, ii: IndexInfo, val: Constant) -> IndexSelectPlan {
        IndexSelectPlan {
            p: Box::new(p),
            ii,
            val,
        }
    }
}

impl PlanControl for IndexSelectPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p.open()?;
        if let Scan::Table(ts) = s {
            let idx = self.ii.open()?;
            return Ok(IndexSelectScan::new(ts, idx, self.val.clone())?.into());
        }
        Err(TransactionError::General)
    }

    fn records_output(&self) -> usize {
        self.ii.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        self.ii.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.p.schema()
    }
}
