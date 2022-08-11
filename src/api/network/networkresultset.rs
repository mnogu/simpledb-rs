use std::sync::{Arc, Mutex};

use tonic::transport::Channel;

use crate::api::{driver::SQLError, metadata::MetaData, resultset::ResultSetControl};

use super::{
    networkconnection::NetworkConnection,
    networkmetadata::NetworkMetaData,
    simpledb::{
        result_set_client::ResultSetClient, CloseResultSetRequest, GetIntRequest, GetStringRequest,
        NextRequest,
    },
};

pub struct NetworkResultSet {
    conn: Arc<Mutex<NetworkConnection>>,
    client: ResultSetClient<Channel>,
    id: u64,
}

impl NetworkResultSet {
    pub fn new(conn: Arc<Mutex<NetworkConnection>>, id: u64) -> NetworkResultSet {
        let client = ResultSetClient::new(conn.lock().unwrap().channel());
        NetworkResultSet { conn, client, id }
    }
}

impl ResultSetControl for NetworkResultSet {
    fn next(&mut self) -> Result<bool, SQLError> {
        let request = tonic::Request::new(NextRequest { id: self.id });
        let response = self.conn.lock().unwrap().run(self.client.next(request))?;
        Ok(response.into_inner().has_next)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, SQLError> {
        let request = tonic::Request::new(GetIntRequest {
            id: self.id,
            name: fldname.to_string(),
        });
        let response = self
            .conn
            .lock()
            .unwrap()
            .run(self.client.get_int(request))?;
        Ok(response.into_inner().value)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, SQLError> {
        let request = tonic::Request::new(GetStringRequest {
            id: self.id,
            name: fldname.to_string(),
        });
        let response = self
            .conn
            .lock()
            .unwrap()
            .run(self.client.get_string(request))?;
        Ok(response.into_inner().value)
    }

    fn get_meta_data(&self) -> MetaData {
        NetworkMetaData::new(self.conn.clone(), self.id).into()
    }

    fn close(&mut self) -> Result<(), SQLError> {
        let request = tonic::Request::new(CloseResultSetRequest { id: self.id });
        self.conn.lock().unwrap().run(self.client.close(request))?;
        Ok(())
    }
}
