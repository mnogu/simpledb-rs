use std::sync::Arc;

use crate::{
    query::{predicate::Predicate, scan::Scan, selectscan::SelectScan},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::plan::Plan;

pub struct SelectPlan {
    p: Box<dyn Plan>,
    pred: Arc<Predicate>,
}

impl SelectPlan {
    pub fn new(p: Box<dyn Plan>, pred: Arc<Predicate>) -> SelectPlan {
        SelectPlan { p, pred }
    }
}

impl Plan for SelectPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p.open()?;
        Ok(SelectScan::new(s, self.pred.clone()).into())
    }

    fn schema(&self) -> Arc<Schema> {
        self.p.schema()
    }
}
