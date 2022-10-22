use std::sync::{Arc, Mutex};

use crate::{
    plan::plan::{Plan, PlanControl},
    query::scan::Scan,
    record::schema::Schema,
    tx::transaction::{Transaction, TransactionError},
};

use super::{
    aggregationfn::{AggregationFn, AggregationFnControl},
    groupbyscan::GroupByScan,
    sortplan::SortPlan,
};

#[derive(Clone)]
pub struct GroupByPlan {
    p: Box<SortPlan>,
    groupfields: Vec<String>,
    aggfns: Vec<AggregationFn>,
    sch: Arc<Schema>,
}

impl PlanControl for GroupByPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p.open()?;
        Ok(GroupByScan::new(s, self.groupfields.clone(), self.aggfns.clone())?.into())
    }

    fn blocks_accessed(&self) -> usize {
        self.p.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        let mut numgroups = 1;
        for fldname in &self.groupfields {
            numgroups *= self.p.distinct_values(fldname);
        }
        numgroups
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        if self.p.schema().has_field(fldname) {
            return self.p.distinct_values(fldname);
        }
        self.records_output()
    }

    fn schema(&self) -> Arc<Schema> {
        self.sch.clone()
    }
}

impl GroupByPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p: Plan,
        groupfields: Vec<String>,
        aggfns: Vec<AggregationFn>,
    ) -> GroupByPlan {
        let p = SortPlan::new(tx, p, groupfields.clone());
        let mut sch = Schema::new();
        for fldname in &groupfields {
            sch.add(fldname, p.schema().as_ref());
        }
        for f in &aggfns {
            sch.add_int_field(&f.field_name());
        }
        let p = Box::new(p);
        let sch = Arc::new(sch);
        GroupByPlan {
            p,
            groupfields,
            aggfns,
            sch,
        }
    }
}
