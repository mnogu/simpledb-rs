use std::sync::Arc;

use crate::{query::scan::Scan, record::schema::Schema, tx::transaction::TransactionError};

use super::{
    plan::{Plan, PlanControl},
    productplan::ProductPlan,
};

#[derive(Clone)]
pub struct OptimizedProductPlan {
    bestplan: Box<Plan>,
}

impl OptimizedProductPlan {
    #[allow(dead_code)]
    pub fn new(p1: Plan, p2: Plan) -> OptimizedProductPlan {
        let prod1 = ProductPlan::new(p1.clone(), p2.clone());
        let prod2 = ProductPlan::new(p2, p1);
        let b1 = prod1.blocks_accessed();
        let b2 = prod2.blocks_accessed();
        let bestplan = Box::new(if b1 < b2 { prod1 } else { prod2 }.into());
        OptimizedProductPlan { bestplan }
    }
}

impl PlanControl for OptimizedProductPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        self.bestplan.open()
    }

    fn blocks_accessed(&self) -> usize {
        self.bestplan.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.bestplan.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        self.bestplan.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.bestplan.schema()
    }
}
