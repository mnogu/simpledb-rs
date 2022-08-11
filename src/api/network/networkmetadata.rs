use std::sync::{Arc, Mutex};

use tonic::transport::Channel;

use crate::{
    api::{driver::SQLError, metadata::MetaDataControl},
    record::schema::Type,
};

use super::{
    networkconnection::NetworkConnection,
    simpledb::{
        meta_data_client::MetaDataClient, GetColumnCountRequest, GetColumnDisplaySizeRequest,
        GetColumnNameRequest, GetColumnTypeRequest,
    },
};

pub struct NetworkMetaData {
    conn: Arc<Mutex<NetworkConnection>>,
    client: MetaDataClient<Channel>,
    id: u64,
}

impl NetworkMetaData {
    pub fn new(conn: Arc<Mutex<NetworkConnection>>, id: u64) -> NetworkMetaData {
        let client = MetaDataClient::new(conn.lock().unwrap().channel());
        NetworkMetaData { conn, client, id }
    }
}

impl MetaDataControl for NetworkMetaData {
    fn get_column_count(&mut self) -> Result<usize, SQLError> {
        let request = tonic::Request::new(GetColumnCountRequest { id: self.id });
        let response = self
            .conn
            .lock()
            .unwrap()
            .run(self.client.get_column_count(request))?;
        Ok(response.into_inner().count as usize)
    }

    fn get_column_name(&mut self, column: usize) -> Result<String, SQLError> {
        let request = tonic::Request::new(GetColumnNameRequest {
            id: self.id,
            index: column as u64,
        });
        let response = self
            .conn
            .lock()
            .unwrap()
            .run(self.client.get_column_name(request))?;
        Ok(response.into_inner().name)
    }

    fn get_column_type(&mut self, column: usize) -> Result<Type, SQLError> {
        let request = tonic::Request::new(GetColumnTypeRequest {
            id: self.id,
            index: column as u64,
        });
        let response = self
            .conn
            .lock()
            .unwrap()
            .run(self.client.get_column_type(request))?;
        match response.into_inner().r#type {
            4 => Ok(Type::Integer),
            12 => Ok(Type::Varchar),
            _ => Err(SQLError::General),
        }
    }

    fn get_column_display_size(&mut self, column: usize) -> Result<usize, SQLError> {
        let request = tonic::Request::new(GetColumnDisplaySizeRequest {
            id: self.id,
            index: column as u64,
        });
        let response = self
            .conn
            .lock()
            .unwrap()
            .run(self.client.get_column_display_size(request))?;
        Ok(response.into_inner().size as usize)
    }
}
