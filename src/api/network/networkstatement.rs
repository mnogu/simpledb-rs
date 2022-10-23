use std::sync::{Arc, Mutex};

use tonic::transport::Channel;

use crate::api::{driver::SQLError, resultset::ResultSet, statement::StatementControl};

use super::{
    networkconnection::NetworkConnection,
    networkresultset::NetworkResultSet,
    simpledb::{statement_client::StatementClient, ExecuteQueryRequest, ExecuteUpdateRequest},
};

pub struct NetworkStatement {
    conn: Arc<Mutex<NetworkConnection>>,
    client: StatementClient<Channel>,
}

impl NetworkStatement {
    #[allow(dead_code)]
    pub fn new(conn: Arc<Mutex<NetworkConnection>>) -> NetworkStatement {
        let client = StatementClient::new(conn.lock().unwrap().channel());
        NetworkStatement { conn, client }
    }
}

impl StatementControl for NetworkStatement {
    fn execute_query(&mut self, qry: &str) -> Result<ResultSet, SQLError> {
        let request = tonic::Request::new(ExecuteQueryRequest {
            query: qry.to_string(),
        });
        let response = self
            .conn
            .lock()
            .unwrap()
            .run(self.client.execute_query(request))?;
        Ok(NetworkResultSet::new(self.conn.clone(), response.into_inner().id).into())
    }

    fn execute_update(&mut self, cmd: &str) -> Result<usize, SQLError> {
        let request = tonic::Request::new(ExecuteUpdateRequest {
            command: cmd.to_string(),
        });
        let response = self
            .conn
            .lock()
            .unwrap()
            .run(self.client.execute_update(request))?;
        Ok(response.into_inner().count as usize)
    }
}
