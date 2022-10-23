use std::sync::{Arc, Mutex};

use crate::{
    metadata::metadatamgr::MetadataMgr,
    parse::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    query::{
        scan::{Scan, ScanControl},
        updatescan::UpdateScanControl,
    },
    tx::transaction::{Transaction, TransactionError},
};

use super::{
    plan::{Plan, PlanControl},
    selectplan::SelectPlan,
    tableplan::TablePlan,
    updateplanner::UpdatePlannerControl,
};

pub struct BasicUpdatePlanner {
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl BasicUpdatePlanner {
    #[allow(dead_code)]
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> BasicUpdatePlanner {
        BasicUpdatePlanner { mdm }
    }
}

impl UpdatePlannerControl for BasicUpdatePlanner {
    fn execute_delete(
        &self,
        data: &DeleteData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        let mut p: Plan = TablePlan::new(tx, &data.table_name(), self.mdm.clone())?.into();
        p = SelectPlan::new(p, data.pred()).into();
        let us = p.open()?;
        if let Scan::Select(mut us) = us {
            let mut count = 0;
            while us.next()? {
                us.delete()?;
                count += 1;
            }
            us.close()?;
            return Ok(count);
        }
        Err(TransactionError::General)
    }

    fn execute_modify(
        &self,
        data: &ModifyData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        let mut p: Plan = TablePlan::new(tx, &data.table_name(), self.mdm.clone())?.into();
        p = SelectPlan::new(p, data.pred()).into();
        let us = p.open()?;
        if let Scan::Select(mut us) = us {
            let mut count = 0;
            while us.next()? {
                let val = data.new_value().evaluate(&mut us)?;
                us.set_val(&data.target_field(), val)?;
                count += 1;
            }
            us.close()?;
            return Ok(count);
        }
        Err(TransactionError::General)
    }

    fn execute_insert(
        &self,
        data: &InsertData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        let p = TablePlan::new(tx, &data.table_name(), self.mdm.clone())?;
        let us = p.open()?;
        if let Scan::Table(mut us) = us {
            us.insert()?;
            let vals = data.vals();
            let mut iter = vals.iter();
            for fldname in data.fields() {
                let val = iter.next();
                if let Some(val) = val {
                    us.set_val(&fldname, val.clone())?;
                }
            }
            us.close()?;
            return Ok(1);
        }
        Err(TransactionError::General)
    }

    fn execute_create_table(
        &self,
        data: &CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        self.mdm
            .lock()
            .unwrap()
            .create_table(&data.table_name(), data.new_schema(), tx)?;
        Ok(0)
    }

    fn execute_create_view(
        &self,
        data: &CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        self.mdm
            .lock()
            .unwrap()
            .create_view(&data.view_name(), &data.view_def(), tx)?;
        Ok(0)
    }

    fn execute_create_index(
        &self,
        data: &CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        self.mdm.lock().unwrap().create_index(
            &data.index_name(),
            &data.table_name(),
            &data.field_name(),
            tx,
        )?;
        Ok(0)
    }
}
