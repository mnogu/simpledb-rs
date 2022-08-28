use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    record::{layout::Layout, schema::Schema},
    tx::transaction::{Transaction, TransactionError},
};

use super::{
    indexinfo::IndexInfo, indexmgr::IndexMgr, statinfo::StatInfo, statmgr::StatMgr,
    tablemgr::TableMgr, viewmgr::ViewMgr,
};

pub struct MetadataMgr {
    tblmgr: Arc<TableMgr>,
    viewmgr: ViewMgr,
    statmgr: Arc<Mutex<StatMgr>>,
    idxmgr: IndexMgr,
}

impl MetadataMgr {
    pub fn new(isnew: bool, tx: Arc<Mutex<Transaction>>) -> Result<MetadataMgr, TransactionError> {
        let tblmgr = Arc::new(TableMgr::new(isnew, tx.clone())?);
        let viewmgr = ViewMgr::new(isnew, tblmgr.clone(), tx.clone())?;
        let statmgr = Arc::new(Mutex::new(StatMgr::new(tblmgr.clone(), tx.clone())?));
        let idxmgr = IndexMgr::new(isnew, tblmgr.clone(), statmgr.clone(), tx)?;
        Ok(MetadataMgr {
            tblmgr,
            viewmgr,
            statmgr,
            idxmgr,
        })
    }

    pub fn create_table(
        &self,
        tblname: &str,
        sch: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), TransactionError> {
        self.tblmgr.create_table(tblname, sch, tx)
    }

    pub fn get_layout(
        &self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Layout, TransactionError> {
        self.tblmgr.get_layout(tblname, tx)
    }

    pub fn create_view(
        &self,
        viewname: &str,
        viewdef: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), TransactionError> {
        self.viewmgr.create_view(viewname, viewdef, tx)
    }

    pub fn get_view_def(
        &self,
        viewname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Option<String>, TransactionError> {
        self.viewmgr.get_view_def(viewname, tx)
    }

    pub fn create_index(
        &self,
        idxname: &str,
        tblname: &str,
        fldname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), TransactionError> {
        self.idxmgr.create_index(idxname, tblname, fldname, tx)
    }

    pub fn get_index_info(
        &self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>, TransactionError> {
        self.idxmgr.get_index_info(tblname, tx)
    }

    pub fn get_stat_info(
        &mut self,
        tblname: &str,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo, TransactionError> {
        self.statmgr
            .lock()
            .unwrap()
            .get_stat_info(tblname, layout, tx)
    }
}
