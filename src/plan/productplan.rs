use std::sync::{Arc, Mutex};

use crate::{
    query::{productscan::ProductScan, scan::Scan},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::plan::{Plan, PlanControl};

#[derive(Clone)]
pub struct ProductPlan {
    p1: Box<Plan>,
    p2: Box<Plan>,
    schema: Arc<Schema>,
}

impl ProductPlan {
    pub fn new(p1: Plan, p2: Plan) -> ProductPlan {
        let mut schema = Schema::new();
        schema.add_all(&p1.schema());
        schema.add_all(&p2.schema());
        ProductPlan {
            p1: Box::new(p1),
            p2: Box::new(p2),
            schema: Arc::new(schema),
        }
    }
}

impl PlanControl for ProductPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s1 = self.p1.open()?;
        let s2 = self.p2.open()?;
        Ok(ProductScan::new(Arc::new(Mutex::new(s1)), Arc::new(Mutex::new(s2)))?.into())
    }

    fn blocks_accessed(&self) -> usize {
        self.p1.blocks_accessed() + (self.p1.records_output() * self.p2.blocks_accessed())
    }

    fn records_output(&self) -> usize {
        self.p1.records_output() * self.p2.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        if self.p1.schema().has_field(fldname) {
            return self.p1.distinct_values(fldname);
        }
        self.p2.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
