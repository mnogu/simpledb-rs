use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use crate::api::connection::ConnectionControl;
use crate::api::driver::SQLError;
use crate::api::embedded::embeddedconnection::EmbeddedConnection;
use crate::server::simpledb::SimpleDB;

use super::remotestatement::RemoteStatement;
use super::simpledb::connection_server::Connection;
use super::simpledb::{
    CloseConnectionReply, CloseConnectionRequest, CommitReply, CommitRequest, RollbackReply,
    RollbackRequest,
};

pub struct RemoteConnection {
    conn: Arc<Mutex<EmbeddedConnection>>,
}

impl RemoteConnection {
    pub fn new(db: SimpleDB) -> Result<RemoteConnection, SQLError> {
        let conn = Arc::new(Mutex::new(EmbeddedConnection::new(db)?));
        Ok(RemoteConnection { conn })
    }

    pub fn create_statement(&self) -> RemoteStatement {
        RemoteStatement::new(self.conn.clone())
    }
}

#[tonic::async_trait]
impl Connection for RemoteConnection {
    async fn close(
        &self,
        _: Request<CloseConnectionRequest>,
    ) -> Result<Response<CloseConnectionReply>, Status> {
        if self.conn.lock().unwrap().close().is_err() {
            return Err(Status::internal("failed to close"));
        }
        let reply = CloseConnectionReply {};
        Ok(Response::new(reply))
    }

    async fn commit(&self, _: Request<CommitRequest>) -> Result<Response<CommitReply>, Status> {
        if self.conn.lock().unwrap().commit().is_err() {
            return Err(Status::internal("failed to commit"));
        }
        let reply = CommitReply {};
        Ok(Response::new(reply))
    }

    async fn rollback(
        &self,
        _: Request<RollbackRequest>,
    ) -> Result<Response<RollbackReply>, Status> {
        if self.conn.lock().unwrap().rollback().is_err() {
            return Err(Status::internal("failed to rollback"));
        }
        let reply = RollbackReply {};
        Ok(Response::new(reply))
    }
}
