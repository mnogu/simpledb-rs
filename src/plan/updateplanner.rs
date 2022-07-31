use std::sync::{Arc, Mutex};

use crate::{
    parse::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    tx::transaction::{Transaction, TransactionError},
};

pub trait UpdatePlanner {
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
