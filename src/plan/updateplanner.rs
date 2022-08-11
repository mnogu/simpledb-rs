use std::sync::{Arc, Mutex};

use crate::{
    parse::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    tx::transaction::{Transaction, TransactionError},
};

use super::basicupdateplanner::BasicUpdatePlanner;

pub trait UpdatePlannerControl {
    fn execute_insert(
        &self,
        data: &InsertData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError>;

    fn execute_delete(
        &self,
        data: &DeleteData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError>;

    fn execute_modify(
        &self,
        data: &ModifyData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError>;

    fn execute_create_table(
        &self,
        data: &CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError>;

    fn execute_create_view(
        &self,
        data: &CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError>;

    fn execute_create_index(
        &self,
        data: &CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError>;
}

pub enum UpdatePlanner {
    Basic(BasicUpdatePlanner),
}

impl From<BasicUpdatePlanner> for UpdatePlanner {
    fn from(p: BasicUpdatePlanner) -> Self {
        UpdatePlanner::Basic(p)
    }
}

impl UpdatePlannerControl for UpdatePlanner {
    fn execute_insert(
        &self,
        data: &InsertData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        match self {
            UpdatePlanner::Basic(planner) => planner.execute_insert(data, tx),
        }
    }

    fn execute_delete(
        &self,
        data: &DeleteData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        match self {
            UpdatePlanner::Basic(planner) => planner.execute_delete(data, tx),
        }
    }

    fn execute_modify(
        &self,
        data: &ModifyData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        match self {
            UpdatePlanner::Basic(planner) => planner.execute_modify(data, tx),
        }
    }

    fn execute_create_table(
        &self,
        data: &CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        match self {
            UpdatePlanner::Basic(planner) => planner.execute_create_table(data, tx),
        }
    }

    fn execute_create_view(
        &self,
        data: &CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        match self {
            UpdatePlanner::Basic(planner) => planner.execute_create_view(data, tx),
        }
    }

    fn execute_create_index(
        &self,
        data: &CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<usize, TransactionError> {
        match self {
            UpdatePlanner::Basic(planner) => planner.execute_create_index(data, tx),
        }
    }
}
