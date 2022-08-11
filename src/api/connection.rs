use std::sync::{Arc, Mutex};

use super::{
    driver::SQLError,
    embedded::{embeddedconnection::EmbeddedConnection, embeddedstatement::EmbeddedStatement},
    network::{networkconnection::NetworkConnection, networkstatement::NetworkStatement},
    statement::Statement,
};

pub trait ConnectionControl {
    fn close(&mut self) -> Result<(), SQLError>;
    fn commit(&mut self) -> Result<(), SQLError>;
    fn rollback(&mut self) -> Result<(), SQLError>;
}

pub enum Connection {
    Embedded(Arc<Mutex<EmbeddedConnection>>),
    Network(Arc<Mutex<NetworkConnection>>),
}

impl From<Arc<Mutex<EmbeddedConnection>>> for Connection {
    fn from(c: Arc<Mutex<EmbeddedConnection>>) -> Self {
        Connection::Embedded(c)
    }
}

impl From<Arc<Mutex<NetworkConnection>>> for Connection {
    fn from(c: Arc<Mutex<NetworkConnection>>) -> Self {
        Connection::Network(c)
    }
}

impl ConnectionControl for Connection {
    fn close(&mut self) -> Result<(), SQLError> {
        match self {
            Connection::Embedded(conn) => conn.lock().unwrap().close(),
            Connection::Network(conn) => conn.lock().unwrap().close(),
        }
    }

    fn commit(&mut self) -> Result<(), SQLError> {
        match self {
            Connection::Embedded(conn) => conn.lock().unwrap().commit(),
            Connection::Network(conn) => conn.lock().unwrap().commit(),
        }
    }

    fn rollback(&mut self) -> Result<(), SQLError> {
        match self {
            Connection::Embedded(conn) => conn.lock().unwrap().rollback(),
            Connection::Network(conn) => conn.lock().unwrap().rollback(),
        }
    }
}

impl Connection {
    pub fn create_statement(&self) -> Statement {
        match self {
            Connection::Embedded(conn) => Statement::Embedded(EmbeddedStatement::new(conn.clone())),
            Connection::Network(conn) => Statement::Network(NetworkStatement::new(conn.clone())),
        }
    }
}
