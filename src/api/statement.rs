use super::{
    driver::SQLError, embedded::embeddedstatement::EmbeddedStatement,
    network::networkstatement::NetworkStatement, resultset::ResultSet,
};

pub trait StatementControl {
    fn execute_query(&mut self, qry: &str) -> Result<ResultSet, SQLError>;
    fn execute_update(&mut self, cmd: &str) -> Result<usize, SQLError>;
}

pub enum Statement {
    Embedded(EmbeddedStatement),
    Network(NetworkStatement),
}

impl StatementControl for Statement {
    fn execute_query(&mut self, qry: &str) -> Result<ResultSet, SQLError> {
        match self {
            Statement::Embedded(stmt) => stmt.execute_query(qry),
            Statement::Network(stmt) => stmt.execute_query(qry),
        }
    }

    fn execute_update(&mut self, cmd: &str) -> Result<usize, SQLError> {
        match self {
            Statement::Embedded(stmt) => stmt.execute_update(cmd),
            Statement::Network(stmt) => stmt.execute_update(cmd),
        }
    }
}
