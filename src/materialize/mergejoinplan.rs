use std::{
    cmp,
    sync::{Arc, Mutex},
};

use crate::{
    plan::plan::{Plan, PlanControl},
    query::scan::Scan,
    record::schema::Schema,
    tx::transaction::{Transaction, TransactionError},
};

use super::{mergejoinscan::MergeJoinScan, sortplan::SortPlan};

#[derive(Clone)]
pub struct MergeJoinPlan {
    p1: Box<Plan>,
    p2: Box<Plan>,
    fldname1: String,
    fldname2: String,
    sch: Arc<Schema>,
}

impl PlanControl for MergeJoinPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s1 = self.p1.open()?;
        if let Scan::Sort(s2) = self.p2.open()? {
            return Ok(MergeJoinScan::new(s1, s2, &self.fldname1, &self.fldname2).into());
        }
        Err(TransactionError::General)
    }

    fn blocks_accessed(&self) -> usize {
        self.p1.blocks_accessed() + self.p2.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        let maxvals = cmp::max(
            self.p1.distinct_values(&self.fldname1),
            self.p2.distinct_values(&self.fldname2),
        );
        (self.p1.records_output() * self.p2.records_output()) / maxvals
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

impl MergeJoinPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p1: Plan,
        p2: Plan,
        fldname1: &str,
        fldname2: &str,
    ) -> MergeJoinPlan {
        let fldname1 = fldname1.to_string();
        let sortlist1 = vec![fldname1.clone()];
        let p1: Box<Plan> = Box::new(SortPlan::new(tx.clone(), p1, sortlist1).into());

        let fldname2 = fldname2.to_string();
        let sortlist2 = vec![fldname2.clone()];
        let p2: Box<Plan> = Box::new(SortPlan::new(tx, p2, sortlist2).into());

        let mut sch = Schema::new();
        sch.add_all(&p1.schema());
        sch.add_all(&p2.schema());
        let sch = Arc::new(sch);

        MergeJoinPlan {
            p1,
            p2,
            fldname1,
            fldname2,
            sch,
        }
    }
}
