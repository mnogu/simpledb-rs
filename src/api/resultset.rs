use enum_dispatch::enum_dispatch;

use super::{
    driver::SQLError, embedded::embeddedresultset::EmbeddedResultSet, metadata::MetaData,
    network::networkresultset::NetworkResultSet,
};

#[enum_dispatch(ResultSet)]
pub trait ResultSetControl {
    fn next(&mut self) -> Result<bool, SQLError>;
    fn get_int(&mut self, fldname: &str) -> Result<i32, SQLError>;
    fn get_string(&mut self, fldname: &str) -> Result<String, SQLError>;
    fn get_meta_data(&self) -> MetaData;
    fn close(&mut self) -> Result<(), SQLError>;
}

#[enum_dispatch]
pub enum ResultSet {
    Embedded(EmbeddedResultSet),
    Network(NetworkResultSet),
}
