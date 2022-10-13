use std::sync::Arc;

use crate::{
    index::query::indexjoinscan::IndexJoinScan,
    metadata::indexinfo::IndexInfo,
    plan::plan::{Plan, PlanControl},
    query::scan::Scan,
    record::schema::Schema,
    tx::transaction::TransactionError,
};

#[derive(Clone)]
pub struct IndexJoinPlan {
    p1: Box<Plan>,
    p2: Box<Plan>,
    ii: IndexInfo,
    joinfield: String,
    sch: Arc<Schema>,
}

impl IndexJoinPlan {
    pub fn new(p1: Plan, p2: Plan, ii: IndexInfo, joinfield: &str) -> IndexJoinPlan {
        let mut sch = Schema::new();
        sch.add_all(&p1.schema());
        sch.add_all(&p2.schema());
        IndexJoinPlan {
            p1: Box::new(p1),
            p2: Box::new(p2),
            ii,
            joinfield: joinfield.to_string(),
            sch: Arc::new(sch),
        }
    }
}

impl PlanControl for IndexJoinPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p1.open()?;
        if let Scan::Table(ts) = self.p2.open()? {
            let idx = self.ii.open()?;
            return Ok(IndexJoinScan::new(s, idx, &self.joinfield, ts)?.into());
        }
        Err(TransactionError::General)
    }

    fn blocks_accessed(&self) -> usize {
        self.p1.blocks_accessed()
            + (self.p1.records_output() * self.ii.blocks_accessed())
            + self.records_output()
    }

    fn records_output(&self) -> usize {
        self.p1.records_output() * self.ii.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        if self.p1.schema().has_field(fldname) {
            return self.p1.distinct_values(fldname);
        }
        self.p2.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.sch.clone()
    }
}
