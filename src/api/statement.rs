use enum_dispatch::enum_dispatch;

use super::{
    driver::SQLError, embedded::embeddedstatement::EmbeddedStatement,
    network::networkstatement::NetworkStatement, resultset::ResultSet,
};

#[enum_dispatch(Statement)]
pub trait StatementControl {
    fn execute_query(&mut self, qry: &str) -> Result<ResultSet, SQLError>;
    fn execute_update(&mut self, cmd: &str) -> Result<usize, SQLError>;
}

#[enum_dispatch]
pub enum Statement {
    Embedded(EmbeddedStatement),
    Network(NetworkStatement),
}
