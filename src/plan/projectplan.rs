use std::sync::Arc;

use crate::{
    query::{projectscan::ProjectScan, scan::Scan},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::plan::Plan;

pub struct ProjectPlan {
    p: Box<dyn Plan>,
    schema: Arc<Schema>,
}

impl ProjectPlan {
    pub fn new(p: Box<dyn Plan>, field_list: Vec<String>) -> ProjectPlan {
        let mut schema = Schema::new();
        for fldname in field_list {
            schema.add(&fldname, &p.schema());
        }
        ProjectPlan {
            p,
            schema: Arc::new(schema),
        }
    }
}

impl Plan for ProjectPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p.open()?;
        Ok(ProjectScan::new(s, self.schema.fields().clone()).into())
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
