use std::sync::{Arc, Mutex};

use crate::{
    metadata::metadatamgr::MetadataMgr,
    parse::{parser::Parser, querydata::QueryData},
    tx::transaction::Transaction,
};

use super::{
    plan::{Plan, PlanControl, PlanError},
    productplan::ProductPlan,
    projectplan::ProjectPlan,
    queryplanner::QueryPlannerControl,
    selectplan::SelectPlan,
    tableplan::TablePlan,
};

pub struct BetterQueryPlanner {
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl BetterQueryPlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> BetterQueryPlanner {
        BetterQueryPlanner { mdm }
    }
}

impl QueryPlannerControl for BetterQueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Plan, PlanError> {
        let mut plans = Vec::new();
        for tblname in data.tables() {
            let viewdef = self
                .mdm
                .lock()
                .unwrap()
                .get_view_def(&tblname, tx.clone())?;
            if let Some(viewdef) = viewdef {
                let mut parser = Parser::new(&viewdef);
                let viewdata = parser.query()?;
                plans.push(self.create_plan(viewdata, tx.clone())?);
            } else {
                plans.push(TablePlan::new(tx.clone(), &tblname, self.mdm.clone())?.into());
            }
        }

        let mut p = plans.remove(0);
        for nextplan in plans {
            let choice1: Plan = ProductPlan::new(nextplan.clone(), p.clone()).into();
            let choice2: Plan = ProductPlan::new(p.clone(), nextplan).into();
            if choice1.blocks_accessed() < choice2.blocks_accessed() {
                p = choice1;
            } else {
                p = choice2;
            }
        }

        p = SelectPlan::new(p, data.pred()).into();

        Ok(ProjectPlan::new(p, data.fields()).into())
    }
}
