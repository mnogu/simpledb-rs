use enum_dispatch::enum_dispatch;
use tonic::Status;

use crate::{
    buffer::buffermgr::AbortError, plan::plan::PlanError, tx::transaction::TransactionError,
};

use super::{
    connection::Connection, embedded::embeddeddriver::EmbeddedDriver,
    network::networkdriver::NetworkDriver,
};

#[derive(Debug)]
pub enum SQLError {
    Abort(AbortError),
    IO(std::io::Error),
    Plan(PlanError),
    Status(Status),
    Transaction(TransactionError),
    Transport(tonic::transport::Error),
    General,
}

impl From<AbortError> for SQLError {
    fn from(e: AbortError) -> Self {
        SQLError::Abort(e)
    }
}

impl From<std::io::Error> for SQLError {
    fn from(e: std::io::Error) -> Self {
        SQLError::IO(e)
    }
}

impl From<PlanError> for SQLError {
    fn from(e: PlanError) -> Self {
        SQLError::Plan(e)
    }
}

impl From<Status> for SQLError {
    fn from(e: Status) -> Self {
        SQLError::Status(e)
    }
}

impl From<TransactionError> for SQLError {
    fn from(e: TransactionError) -> Self {
        SQLError::Transaction(e)
    }
}

impl From<tonic::transport::Error> for SQLError {
    fn from(e: tonic::transport::Error) -> Self {
        SQLError::Transport(e)
    }
}

#[enum_dispatch(Driver)]
pub trait DriverControl {
    fn connect(&self, url: &str) -> Result<Connection, SQLError>;
}

#[enum_dispatch]
pub enum Driver {
    Embedded(EmbeddedDriver),
    Network(NetworkDriver),
}
