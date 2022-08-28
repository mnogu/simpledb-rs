use std::sync::{Arc, Mutex};

use crate::{
    api::{
        connection::ConnectionControl, driver::SQLError, metadata::MetaData,
        resultset::ResultSetControl,
    },
    plan::plan::Plan,
    query::scan::{Scan, ScanControl},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::{embeddedconnection::EmbeddedConnection, embeddedmetadata::EmbeddedMetaData};

pub struct EmbeddedResultSet {
    s: Scan,
    sch: Arc<Schema>,
    conn: Arc<Mutex<EmbeddedConnection>>,
}

impl EmbeddedResultSet {
    pub fn new(
        plan: Box<dyn Plan>,
        conn: Arc<Mutex<EmbeddedConnection>>,
    ) -> Result<EmbeddedResultSet, TransactionError> {
        Ok(EmbeddedResultSet {
            s: plan.open()?,
            sch: plan.schema(),
            conn,
        })
    }
}

impl ResultSetControl for EmbeddedResultSet {
    fn next(&mut self) -> Result<bool, SQLError> {
        if let Ok(r) = self.s.next() {
            return Ok(r);
        }
        self.conn.lock().unwrap().rollback()?;
        Err(SQLError::General)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, SQLError> {
        let fldname = fldname.to_lowercase();
        if let Ok(v) = self.s.get_int(&fldname) {
            return Ok(v);
        }
        self.conn.lock().unwrap().rollback()?;
        Err(SQLError::General)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, SQLError> {
        let fldname = fldname.to_lowercase();
        if let Ok(v) = self.s.get_string(&fldname) {
            return Ok(v);
        }
        self.conn.lock().unwrap().rollback()?;
        Err(SQLError::General)
    }

    fn get_meta_data(&self) -> MetaData {
        EmbeddedMetaData::new(self.sch.clone()).into()
    }

    fn close(&mut self) -> Result<(), SQLError> {
        self.s.close()?;
        self.conn.lock().unwrap().close()
    }
}
