use enum_dispatch::enum_dispatch;

use crate::record::schema::Type;

use super::{
    driver::SQLError, embedded::embeddedmetadata::EmbeddedMetaData,
    network::networkmetadata::NetworkMetaData,
};

#[enum_dispatch(MetaData)]
pub trait MetaDataControl {
    fn get_column_count(&mut self) -> Result<usize, SQLError>;
    fn get_column_name(&mut self, column: usize) -> Result<String, SQLError>;
    fn get_column_type(&mut self, column: usize) -> Result<Type, SQLError>;
    fn get_column_display_size(&mut self, column: usize) -> Result<usize, SQLError>;
}

#[enum_dispatch]
pub enum MetaData {
    Embedded(EmbeddedMetaData),
    Network(NetworkMetaData),
}
