use std::sync::{Arc, Mutex};

use crate::{
    api::{
        connection::Connection,
        driver::{DriverControl, SQLError},
    },
    server::simpledb::SimpleDB,
};

use super::embeddedconnection::EmbeddedConnection;

pub struct EmbeddedDriver {}

impl EmbeddedDriver {
    pub fn new() -> EmbeddedDriver {
        EmbeddedDriver {}
    }
}

impl DriverControl for EmbeddedDriver {
    fn connect(&self, url: &str) -> Result<Connection, SQLError> {
        let dbname = url;
        let db = SimpleDB::new(dbname)?;
        Ok(Arc::new(Mutex::new(EmbeddedConnection::new(db)?)).into())
    }
}
