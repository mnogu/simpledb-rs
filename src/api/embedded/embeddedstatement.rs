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
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.get_transaction();
        if let Some(planner) = conn.planner() {
            if let Ok(pln) = planner.create_query_plan(qry, tx) {
                if let Ok(s) = EmbeddedResultSet::new(pln, self.conn.clone()) {
                    return Ok(s.into());
                }
            }
        }
        conn.rollback()?;
        Err(SQLError::General)
    }

    fn execute_update(&mut self, cmd: &str) -> Result<usize, SQLError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.get_transaction();
        if let Some(planner) = conn.planner() {
            if let Ok(result) = planner.execute_update(cmd, tx) {
                if conn.commit().is_ok() {
                    return Ok(result);
                }
            }
        }
        conn.rollback()?;
        Err(SQLError::General)
    }
}
