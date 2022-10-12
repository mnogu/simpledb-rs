use std::sync::{Arc, Mutex};

use crate::{
    api::{connection::ConnectionControl, driver::SQLError},
    plan::planner::Planner,
    server::simpledb::SimpleDB,
    tx::transaction::Transaction,
};

pub struct EmbeddedConnection {
    db: SimpleDB,
    current_tx: Arc<Mutex<Transaction>>,
    planner: Option<Arc<Mutex<Planner>>>,
}

impl EmbeddedConnection {
    pub fn new(db: SimpleDB) -> Result<EmbeddedConnection, SQLError> {
        let current_tx = Arc::new(Mutex::new(db.new_tx()?));
        let planner = db.planner();
        Ok(EmbeddedConnection {
            db,
            current_tx,
            planner,
        })
    }

    pub(in crate::api) fn get_transaction(&self) -> Arc<Mutex<Transaction>> {
        self.current_tx.clone()
    }

    pub(in crate::api) fn planner(&self) -> Option<Arc<Mutex<Planner>>> {
        self.planner.clone()
    }
}

impl ConnectionControl for EmbeddedConnection {
    fn close(&mut self) -> Result<(), SQLError> {
        Ok(self.current_tx.lock().unwrap().commit()?)
    }

    fn commit(&mut self) -> Result<(), SQLError> {
        self.current_tx.lock().unwrap().commit()?;
        self.current_tx = Arc::new(Mutex::new(self.db.new_tx()?));
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), SQLError> {
        self.current_tx.lock().unwrap().rollback()?;
        self.current_tx = Arc::new(Mutex::new(self.db.new_tx()?));
        Ok(())
    }
}
