use std::sync::{Arc, Mutex};

use crate::{parse::querydata::QueryData, tx::transaction::Transaction};

use super::plan::{Plan, PlanError};

pub trait QueryPlanner {
    fn create_plan(
        &self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Box<dyn Plan>, PlanError>;
}
