use std::sync::{Arc, Mutex};

use crate::{
    metadata::metadatamgr::MetadataMgr,
    parse::querydata::QueryData,
    plan::{
        plan::{Plan, PlanControl, PlanError},
        projectplan::ProjectPlan,
        queryplanner::QueryPlannerControl,
    },
    tx::transaction::Transaction,
};

use super::tableplanner::TablePlanner;

pub struct HeuristicQueryPlanner {
    tableplanners: Vec<TablePlanner>,
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl HeuristicQueryPlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> HeuristicQueryPlanner {
        HeuristicQueryPlanner {
            tableplanners: Vec::new(),
            mdm,
        }
    }

    fn get_lowest_select_plan(&mut self) -> Option<Plan> {
        let mut bestidx = None;
        let mut bestplan: Option<Plan> = None;
        for (idx, tp) in self.tableplanners.iter().enumerate() {
            let plan = tp.make_select_plan();
            if let Some(bestplan) = &bestplan {
                if plan.records_output() >= bestplan.records_output() {
                    continue;
                }
            }
            bestidx = Some(idx);
            bestplan = Some(plan);
        }
        if let Some(bestidx) = bestidx {
            self.tableplanners.remove(bestidx);
        }
        bestplan
    }

    fn get_lowest_join_plan(&mut self, current: &Plan) -> Option<Plan> {
        let mut bestidx = None;
        let mut bestplan: Option<Plan> = None;
        for (idx, tp) in self.tableplanners.iter().enumerate() {
            let plan = tp.make_join_plan(current);
            if let Some(plan) = plan {
                if let Some(bestplan) = &bestplan {
                    if plan.records_output() >= bestplan.records_output() {
                        continue;
                    }
                }
                bestidx = Some(idx);
                bestplan = Some(plan);
            }
        }
        if let Some(bestidx) = bestidx {
            self.tableplanners.remove(bestidx);
        }
        bestplan
    }

    fn get_lowest_product_plan(&mut self, current: &Plan) -> Option<Plan> {
        let mut bestidx = None;
        let mut bestplan: Option<Plan> = None;
        for (idx, tp) in self.tableplanners.iter().enumerate() {
            let plan = tp.make_product_plan(current);
            if let Some(bestplan) = &bestplan {
                if plan.records_output() >= bestplan.records_output() {
                    continue;
                }
            }
            bestidx = Some(idx);
            bestplan = Some(plan);
        }
        if let Some(bestidx) = bestidx {
            self.tableplanners.remove(bestidx);
        }
        bestplan
    }
}

impl QueryPlannerControl for HeuristicQueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Plan, PlanError> {
        for tblname in data.tables() {
            let tp = TablePlanner::new(&tblname, data.pred(), tx.clone(), self.mdm.clone())?;
            self.tableplanners.push(tp);
        }

        let mut currentplan = self.get_lowest_select_plan();

        while !self.tableplanners.is_empty() {
            if let Some(cp) = currentplan {
                let p = self.get_lowest_join_plan(&cp);
                if p.is_some() {
                    currentplan = p;
                } else {
                    currentplan = self.get_lowest_product_plan(&cp);
                }
            } else {
                return Err(PlanError::General);
            }
        }

        if let Some(currentplan) = currentplan {
            return Ok(ProjectPlan::new(currentplan, data.fields()).into());
        }
        Err(PlanError::General)
    }
}
