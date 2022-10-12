use std::{cmp, sync::Arc};

use crate::{
    query::{predicate::Predicate, scan::Scan, selectscan::SelectScan},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::plan::{Plan, PlanControl};

#[derive(Clone)]
pub struct SelectPlan {
    p: Box<Plan>,
    pred: Arc<Predicate>,
}

impl SelectPlan {
    pub fn new(p: Plan, pred: Arc<Predicate>) -> SelectPlan {
        SelectPlan {
            p: Box::new(p),
            pred,
        }
    }
}

impl PlanControl for SelectPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p.open()?;
        Ok(SelectScan::new(s, self.pred.clone()).into())
    }

    fn records_output(&self) -> usize {
        self.p.records_output() / self.pred.reduction_factor(&self.p)
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        if self.pred.equates_with_constant(fldname).is_some() {
            return 1;
        }
        let fldname2 = self.pred.equates_with_field(fldname);
        if let Some(fldname2) = fldname2 {
            return cmp::min(
                self.p.distinct_values(fldname),
                self.p.distinct_values(&fldname2),
            );
        }
        self.p.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.p.schema()
    }
}
