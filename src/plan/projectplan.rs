use std::sync::Arc;

use crate::{
    query::{projectscan::ProjectScan, scan::Scan},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::plan::{Plan, PlanControl};

#[derive(Clone)]
pub struct ProjectPlan {
    p: Box<Plan>,
    schema: Arc<Schema>,
}

impl ProjectPlan {
    pub fn new(p: Plan, field_list: Vec<String>) -> ProjectPlan {
        let mut schema = Schema::new();
        for fldname in field_list {
            schema.add(&fldname, &p.schema());
        }
        ProjectPlan {
            p: Box::new(p),
            schema: Arc::new(schema),
        }
    }
}

impl PlanControl for ProjectPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let s = self.p.open()?;
        Ok(ProjectScan::new(s, self.schema.fields().clone()).into())
    }

    fn blocks_accessed(&self) -> usize {
        self.p.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.p.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        self.p.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
