use std::sync::{Arc, Mutex};

use enum_dispatch::enum_dispatch;

use crate::{
    index::planner::indexupdateplanner::IndexUpdatePlanner,
    parse::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    tx::transaction::{Transaction, TransactionError},
};

use super::basicupdateplanner::BasicUpdatePlanner;

#[enum_dispatch(UpdatePlanner)]
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

#[enum_dispatch]
pub enum UpdatePlanner {
    Basic(BasicUpdatePlanner),
    Index(IndexUpdatePlanner),
}
