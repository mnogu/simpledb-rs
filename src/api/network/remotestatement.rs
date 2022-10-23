use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tonic::{Request, Response, Status};

use crate::api::{
    embedded::{
        embeddedconnection::EmbeddedConnection, embeddedresultset::EmbeddedResultSet,
        embeddedstatement::EmbeddedStatement,
    },
    resultset::ResultSet,
    statement::StatementControl,
};

use super::simpledb::{
    statement_server::Statement, ExecuteQueryReply, ExecuteQueryRequest, ExecuteUpdateReply,
    ExecuteUpdateRequest,
};

pub struct RemoteStatement {
    stmt: Arc<Mutex<EmbeddedStatement>>,
    rss: Arc<Mutex<HashMap<u64, EmbeddedResultSet>>>,
}

impl RemoteStatement {
    pub fn new(conn: Arc<Mutex<EmbeddedConnection>>) -> RemoteStatement {
        let stmt = Arc::new(Mutex::new(EmbeddedStatement::new(conn)));
        let rss = Arc::new(Mutex::new(HashMap::new()));
        RemoteStatement { stmt, rss }
    }

    #[allow(dead_code)]
    pub fn result_sets(&self) -> Arc<Mutex<HashMap<u64, EmbeddedResultSet>>> {
        self.rss.clone()
    }
}

#[tonic::async_trait]
impl Statement for RemoteStatement {
    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<ExecuteQueryReply>, Status> {
        let rs = self
            .stmt
            .lock()
            .unwrap()
            .execute_query(&request.into_inner().query);
        if let Ok(ResultSet::Embedded(rs)) = rs {
            let id = self.rss.lock().unwrap().len() as u64;
            self.rss.lock().unwrap().insert(id, rs);
            let reply = ExecuteQueryReply { id };
            return Ok(Response::new(reply));
        }
        Err(Status::internal("failed to execute the query"))
    }

    async fn execute_update(
        &self,
        request: Request<ExecuteUpdateRequest>,
    ) -> Result<Response<ExecuteUpdateReply>, Status> {
        let count = self
            .stmt
            .lock()
            .unwrap()
            .execute_update(&request.into_inner().command);
        if let Ok(count) = count {
            let reply = ExecuteUpdateReply {
                count: count as u64,
            };
            return Ok(Response::new(reply));
        }
        Err(Status::internal("failed to execute the update"))
    }
}
