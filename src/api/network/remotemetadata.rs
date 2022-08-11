use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tonic::{Request, Response, Status};

use crate::{
    api::{
        embedded::embeddedresultset::EmbeddedResultSet, metadata::MetaDataControl,
        resultset::ResultSetControl,
    },
    record::schema::Type,
};

use super::simpledb::{
    meta_data_server::MetaData, GetColumnCountReply, GetColumnCountRequest,
    GetColumnDisplaySizeReply, GetColumnDisplaySizeRequest, GetColumnNameReply,
    GetColumnNameRequest, GetColumnTypeReply, GetColumnTypeRequest,
};

pub struct RemoteMetaData {
    rss: Arc<Mutex<HashMap<u64, EmbeddedResultSet>>>,
}

impl RemoteMetaData {
    pub fn new(rss: Arc<Mutex<HashMap<u64, EmbeddedResultSet>>>) -> RemoteMetaData {
        RemoteMetaData { rss }
    }
}

#[tonic::async_trait]
impl MetaData for RemoteMetaData {
    async fn get_column_count(
        &self,
        request: Request<GetColumnCountRequest>,
    ) -> Result<Response<GetColumnCountReply>, Status> {
        let request = request.into_inner();
        let mut rss = self.rss.lock().unwrap();
        let rs = rss.get_mut(&request.id);
        if let Some(rs) = rs {
            let count = rs.get_meta_data().get_column_count();
            if let Ok(count) = count {
                let reply = GetColumnCountReply {
                    count: count as u64,
                };
                return Ok(Response::new(reply));
            }
        }
        Err(Status::internal("failed to get the column count"))
    }

    async fn get_column_name(
        &self,
        request: Request<GetColumnNameRequest>,
    ) -> Result<Response<GetColumnNameReply>, Status> {
        let request = request.into_inner();
        let mut rss = self.rss.lock().unwrap();
        let rs = rss.get_mut(&request.id);
        if let Some(rs) = rs {
            let name = rs.get_meta_data().get_column_name(request.index as usize);
            if let Ok(name) = name {
                let reply = GetColumnNameReply { name };
                return Ok(Response::new(reply));
            }
        }
        Err(Status::internal("failed to get the column name"))
    }

    async fn get_column_type(
        &self,
        request: Request<GetColumnTypeRequest>,
    ) -> Result<Response<GetColumnTypeReply>, Status> {
        let request = request.into_inner();
        let mut rss = self.rss.lock().unwrap();
        let rs = rss.get_mut(&request.id);
        if let Some(rs) = rs {
            let type_ = rs.get_meta_data().get_column_type(request.index as usize);
            if let Ok(type_) = type_ {
                let type_ = match type_ {
                    Type::Integer => 4,
                    Type::Varchar => 12,
                };
                let reply = GetColumnTypeReply { r#type: type_ };
                return Ok(Response::new(reply));
            }
        }
        Err(Status::internal("failed to get the column type"))
    }

    async fn get_column_display_size(
        &self,
        request: Request<GetColumnDisplaySizeRequest>,
    ) -> Result<Response<GetColumnDisplaySizeReply>, Status> {
        let request = request.into_inner();
        let mut rss = self.rss.lock().unwrap();
        let rs = rss.get_mut(&request.id);
        if let Some(rs) = rs {
            let size = rs
                .get_meta_data()
                .get_column_display_size(request.index as usize);
            if let Ok(size) = size {
                let reply = GetColumnDisplaySizeReply { size: size as u64 };
                return Ok(Response::new(reply));
            }
        }
        Err(Status::internal("failed to get the column display size"))
    }
}
