use std::sync::Arc;

use crate::{
    query::{productscan::ProductScan, scan::Scan},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::plan::Plan;

pub struct ProductPlan {
    p1: Box<dyn Plan>,
    p2: Box<dyn Plan>,
    schema: Arc<Schema>,
}

impl ProductPlan {
    pub fn new(p1: Box<dyn Plan>, p2: Box<dyn Plan>) -> ProductPlan {
        let mut schema = Schema::new();
        schema.add_all(&p1.schema());
        schema.add_all(&p2.schema());
        ProductPlan {
            p1,
            p2,
            schema: Arc::new(schema),
        }
    }
}

impl Plan for ProductPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s1 = self.p1.open()?;
        let s2 = self.p2.open()?;
        Ok(ProductScan::new(s1, s2)?.into())
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
