use std::sync::{Arc, Mutex};

use crate::api::{
    connection::ConnectionControl, driver::SQLError, resultset::ResultSet,
    statement::StatementControl,
};

use super::{embeddedconnection::EmbeddedConnection, embeddedresultset::EmbeddedResultSet};

pub struct EmbeddedStatement {
    conn: Arc<Mutex<EmbeddedConnection>>,
}

impl EmbeddedStatement {
    pub fn new(conn: Arc<Mutex<EmbeddedConnection>>) -> EmbeddedStatement {
        EmbeddedStatement { conn }
    }
}

impl StatementControl for EmbeddedStatement {
    fn execute_query(&mut self, qry: &str) -> Result<ResultSet, SQLError> {
        let tx = self.conn.lock().unwrap().get_transaction();
        let planner = self.conn.lock().unwrap().planner();
        if let Some(planner) = planner {
            let pln = planner.lock().unwrap().create_query_plan(qry, tx);
            if let Ok(pln) = pln {
                if let Ok(s) = EmbeddedResultSet::new(pln, self.conn.clone()) {
                    return Ok(s.into());
                }
            }
        }
        self.conn.lock().unwrap().rollback()?;
        Err(SQLError::General)
    }

    fn execute_update(&mut self, cmd: &str) -> Result<usize, SQLError> {
        let tx = self.conn.lock().unwrap().get_transaction();
        let planner = self.conn.lock().unwrap().planner();
        if let Some(planner) = planner {
            let result = planner.lock().unwrap().execute_update(cmd, tx);
            if let Ok(result) = result {
                if self.conn.lock().unwrap().commit().is_ok() {
                    return Ok(result);
                }
            }
        }
        self.conn.lock().unwrap().rollback()?;
        Err(SQLError::General)
    }
}
