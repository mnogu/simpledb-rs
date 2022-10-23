use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tonic::{Request, Response, Status};

use crate::api::{embedded::embeddedresultset::EmbeddedResultSet, resultset::ResultSetControl};

use super::simpledb::{
    result_set_server::ResultSet, CloseResultSetReply, CloseResultSetRequest, GetIntReply,
    GetIntRequest, GetStringReply, GetStringRequest, NextReply, NextRequest,
};

pub struct RemoteResultSet {
    rss: Arc<Mutex<HashMap<u64, EmbeddedResultSet>>>,
}

impl RemoteResultSet {
    #[allow(dead_code)]
    pub fn new(rss: Arc<Mutex<HashMap<u64, EmbeddedResultSet>>>) -> RemoteResultSet {
        RemoteResultSet { rss }
    }
}

#[tonic::async_trait]
impl ResultSet for RemoteResultSet {
    async fn next(&self, request: Request<NextRequest>) -> Result<Response<NextReply>, Status> {
        let request = request.into_inner();
        let mut rss = self.rss.lock().unwrap();
        let rs = rss.get_mut(&request.id);
        if let Some(rs) = rs {
            if let Ok(has_next) = rs.next() {
                let reply = NextReply { has_next };
                return Ok(Response::new(reply));
            }
        }
        Err(Status::internal("failed to be next"))
    }

    async fn get_int(
        &self,
        request: Request<GetIntRequest>,
    ) -> Result<Response<GetIntReply>, Status> {
        let request = request.into_inner();
        let mut rss = self.rss.lock().unwrap();
        let rs = rss.get_mut(&request.id);
        if let Some(rs) = rs {
            if let Ok(value) = rs.get_int(&request.name) {
                let reply = GetIntReply { value };
                return Ok(Response::new(reply));
            }
        }
        Err(Status::internal("failed to get the integer"))
    }

    async fn get_string(
        &self,
        request: Request<GetStringRequest>,
    ) -> Result<Response<GetStringReply>, Status> {
        let request = request.into_inner();
        let mut rss = self.rss.lock().unwrap();
        let rs = rss.get_mut(&request.id);
        if let Some(rs) = rs {
            if let Ok(value) = rs.get_string(&request.name) {
                let reply = GetStringReply { value };
                return Ok(Response::new(reply));
            }
        }
        Err(Status::internal("failed to get the string"))
    }

    async fn close(
        &self,
        request: Request<CloseResultSetRequest>,
    ) -> Result<Response<CloseResultSetReply>, Status> {
        let request = request.into_inner();
        let mut rss = self.rss.lock().unwrap();
        let rs = rss.get_mut(&request.id);
        if let Some(rs) = rs {
            if rs.close().is_ok() {
                rss.remove(&request.id);
                let reply = CloseResultSetReply {};
                return Ok(Response::new(reply));
            }
        }
        Err(Status::internal("failed to close"))
    }
}
