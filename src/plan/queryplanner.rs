use std::sync::{Arc, Mutex};

use crate::{parse::querydata::QueryData, tx::transaction::Transaction};

use super::{
    basicqueryplanner::BasicQueryPlanner,
    plan::{Plan, PlanError},
};

pub trait QueryPlannerControl {
    fn create_plan(
        &self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Box<dyn Plan>, PlanError>;
}

pub enum QueryPlanner {
    Basic(BasicQueryPlanner),
}

impl From<BasicQueryPlanner> for QueryPlanner {
    fn from(p: BasicQueryPlanner) -> Self {
        QueryPlanner::Basic(p)
    }
}

impl QueryPlannerControl for QueryPlanner {
    fn create_plan(
        &self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Box<dyn Plan>, PlanError> {
        match self {
            QueryPlanner::Basic(planner) => planner.create_plan(data, tx),
        }
    }
}
