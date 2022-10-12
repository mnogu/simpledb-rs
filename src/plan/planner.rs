use std::sync::{Arc, Mutex};

use crate::{
    parse::{
        parser::{Object, Parser},
        querydata::QueryData,
    },
    tx::transaction::Transaction,
};

use super::{
    plan::{Plan, PlanError},
    queryplanner::{QueryPlanner, QueryPlannerControl},
    updateplanner::{UpdatePlanner, UpdatePlannerControl},
};

pub struct Planner {
    qplanner: QueryPlanner,
    uplanner: UpdatePlanner,
}

impl Planner {
    pub fn new(qplanner: QueryPlanner, uplanner: UpdatePlanner) -> Planner {
        Planner { qplanner, uplanner }
    }

    pub fn create_query_plan(
        &mut self,
        qry: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Plan, PlanError> {
        let mut parser = Parser::new(qry);
        let data = parser.query()?;
        self.verify_query(&data);
        self.qplanner.create_plan(data, tx)
    }

    pub fn execute_update(
        &self,
        cmd: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, PlanError> {
        let mut parser = Parser::new(cmd);
        let data = parser.update_cmd()?;
        self.verify_update(&data);
        Ok(match data {
            Object::Insert(object) => self.uplanner.execute_insert(&object, tx)?,
            Object::Delete(object) => self.uplanner.execute_delete(&object, tx)?,
            Object::Modify(object) => self.uplanner.execute_modify(&object, tx)?,
            Object::CreateTable(object) => self.uplanner.execute_create_table(&object, tx)?,
            Object::CreateView(object) => self.uplanner.execute_create_view(&object, tx)?,
            Object::CreateIndex(object) => self.uplanner.execute_create_index(&object, tx)?,
        })
    }

    fn verify_query(&self, _: &QueryData) {}

    fn verify_update(&self, _: &Object) {}
}
