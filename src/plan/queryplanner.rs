use std::sync::{Arc, Mutex};

use crate::{
    opt::heuristicqueryplanner::HeuristicQueryPlanner, parse::querydata::QueryData,
    tx::transaction::Transaction,
};

use super::{
    basicqueryplanner::BasicQueryPlanner,
    plan::{Plan, PlanError},
};

pub trait QueryPlannerControl {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Plan, PlanError>;
}

pub enum QueryPlanner {
    Basic(BasicQueryPlanner),
    Heuristic(HeuristicQueryPlanner),
}

impl From<BasicQueryPlanner> for QueryPlanner {
    fn from(p: BasicQueryPlanner) -> Self {
        QueryPlanner::Basic(p)
    }
}

impl From<HeuristicQueryPlanner> for QueryPlanner {
    fn from(p: HeuristicQueryPlanner) -> Self {
        QueryPlanner::Heuristic(p)
    }
}

impl QueryPlannerControl for QueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Plan, PlanError> {
        match self {
            QueryPlanner::Basic(planner) => planner.create_plan(data, tx),
            QueryPlanner::Heuristic(planner) => planner.create_plan(data, tx),
        }
    }
}
