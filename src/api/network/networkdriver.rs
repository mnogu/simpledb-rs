use std::sync::{Arc, Mutex};

use tonic::transport::Endpoint;

use crate::api::{
    connection::Connection,
    driver::{DriverControl, SQLError},
};

use super::networkconnection::NetworkConnection;

pub struct NetworkDriver {}

impl NetworkDriver {
    pub fn new() -> NetworkDriver {
        NetworkDriver {}
    }
}

impl DriverControl for NetworkDriver {
    fn connect(&self, url: &str) -> Result<Connection, SQLError> {
        let url = url.to_string();
        let v: Vec<&str> = url.splitn(2, "//").collect();
        let host = v[v.len() - 1];
        let endpoint = Endpoint::from_shared(format!("http://{}:1099", host))?;
        Ok(Arc::new(Mutex::new(NetworkConnection::new(endpoint)?)).into())
    }
}
