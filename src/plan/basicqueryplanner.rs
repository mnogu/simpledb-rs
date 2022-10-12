use std::sync::{Arc, Mutex};

use crate::{
    metadata::metadatamgr::MetadataMgr,
    parse::{parser::Parser, querydata::QueryData},
    tx::transaction::Transaction,
};

use super::{
    plan::{Plan, PlanError},
    productplan::ProductPlan,
    projectplan::ProjectPlan,
    queryplanner::QueryPlannerControl,
    selectplan::SelectPlan,
    tableplan::TablePlan,
};

pub struct BasicQueryPlanner {
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl BasicQueryPlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> BasicQueryPlanner {
        BasicQueryPlanner { mdm }
    }
}

impl QueryPlannerControl for BasicQueryPlanner {
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
            p = ProductPlan::new(p, nextplan).into();
        }

        p = SelectPlan::new(p, data.pred()).into();

        Ok(ProjectPlan::new(p, data.fields()).into())
    }
}
