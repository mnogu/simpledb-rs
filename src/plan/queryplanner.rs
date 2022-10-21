use std::sync::{Arc, Mutex};

use enum_dispatch::enum_dispatch;

use crate::{
    opt::heuristicqueryplanner::HeuristicQueryPlanner, parse::querydata::QueryData,
    tx::transaction::Transaction,
};

use super::{
    basicqueryplanner::BasicQueryPlanner,
    betterqueryplanner::BetterQueryPlanner,
    plan::{Plan, PlanError},
};

#[enum_dispatch(QueryPlanner)]
pub trait QueryPlannerControl {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Plan, PlanError>;
}

#[enum_dispatch]
pub enum QueryPlanner {
    Basic(BasicQueryPlanner),
    Better(BetterQueryPlanner),
    Heuristic(HeuristicQueryPlanner),
}
