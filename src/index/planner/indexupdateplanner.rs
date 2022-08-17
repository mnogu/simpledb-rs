use std::{
    iter::zip,
    sync::{Arc, Mutex},
};

use crate::{
    index::index::IndexControl,
    metadata::metadatamgr::MetadataMgr,
    parse::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    plan::{
        plan::Plan, selectplan::SelectPlan, tableplan::TablePlan,
        updateplanner::UpdatePlannerControl,
    },
    query::{
        scan::{Scan, ScanControl},
        updatescan::UpdateScanControl,
    },
    tx::transaction::{Transaction, TransactionError},
};

pub struct IndexUpdatePlanner {
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl IndexUpdatePlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> IndexUpdatePlanner {
        IndexUpdatePlanner { mdm }
    }
}

impl UpdatePlannerControl for IndexUpdatePlanner {
    fn execute_insert(
        &self,
        data: &InsertData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        let tblname = data.table_name();
        let p = TablePlan::new(tx.clone(), &tblname, self.mdm.clone())?;

        let s = p.open()?;
        if let Scan::Table(mut s) = s {
            s.insert()?;
            let rid = s.get_rid();

            if let Some(rid) = rid {
                let indexes = self.mdm.lock().unwrap().get_index_info(&tblname, tx)?;
                for (fldname, val) in zip(data.fields(), data.vals()) {
                    s.set_val(&fldname, val.clone())?;

                    let ii = indexes.get(&fldname);
                    if let Some(ii) = ii {
                        let mut idx = ii.open();
                        idx.insert(val, &rid)?;
                        idx.close()?;
                    }
                }
            }
            s.close()?;
            return Ok(1);
        }
        Err(TransactionError::General)
    }

    fn execute_delete(
        &self,
        data: &DeleteData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        let tblname = data.table_name();
        let p = TablePlan::new(tx.clone(), &tblname, self.mdm.clone())?;
        let indexes = self
            .mdm
            .lock()
            .unwrap()
            .get_index_info(&tblname, tx.clone())?;

        let s = p.open()?;
        if let Scan::Select(mut s) = s {
            let mut count = 0;
            while s.next()? {
                let rid = s.get_rid();
                if let Some(rid) = rid {
                    for (fldname, ii) in &indexes {
                        let val = s.get_val(&fldname)?;
                        let mut idx = ii.open();
                        idx.delete(val, &rid)?;
                        idx.close()?;
                    }
                    s.delete()?;
                    count += 1;
                }
            }
            s.close()?;
            return Ok(count);
        }
        Err(TransactionError::General)
    }

    fn execute_modify(
        &self,
        data: &ModifyData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        let tblname = data.table_name();
        let fldname = data.target_field();
        let mut p: Box<dyn Plan> =
            Box::new(TablePlan::new(tx.clone(), &tblname, self.mdm.clone())?);
        p = Box::new(SelectPlan::new(p, data.pred()));

        let m = self.mdm.lock().unwrap().get_index_info(&tblname, tx)?;
        let ii = m.get(&fldname);
        let mut idx = match ii {
            Some(ii) => Some(ii.open()),
            None => None,
        };

        let s = p.open()?;
        if let Scan::Select(mut s) = s {
            let mut count = 0;
            while s.next()? {
                let newval = data.new_value().evaluate(&mut s)?;
                let oldval = s.get_val(&fldname)?;
                s.set_val(&data.target_field(), newval.clone())?;

                if let Some(idx) = &mut idx {
                    let rid = s.get_rid();
                    if let Some(rid) = rid {
                        idx.delete(oldval, &rid)?;
                        idx.insert(newval, &rid)?;
                    }
                }
                count += 1;
            }
            return Ok(count);
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
