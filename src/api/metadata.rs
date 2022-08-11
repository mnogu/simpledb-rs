use crate::record::schema::Type;

use super::{
    driver::SQLError, embedded::embeddedmetadata::EmbeddedMetaData,
    network::networkmetadata::NetworkMetaData,
};

pub trait MetaDataControl {
    fn get_column_count(&mut self) -> Result<usize, SQLError>;
    fn get_column_name(&mut self, column: usize) -> Result<String, SQLError>;
    fn get_column_type(&mut self, column: usize) -> Result<Type, SQLError>;
    fn get_column_display_size(&mut self, column: usize) -> Result<usize, SQLError>;
}

pub enum MetaData {
    Embedded(EmbeddedMetaData),
    Network(NetworkMetaData),
}

impl From<EmbeddedMetaData> for MetaData {
    fn from(md: EmbeddedMetaData) -> Self {
        MetaData::Embedded(md)
    }
}

impl From<NetworkMetaData> for MetaData {
    fn from(md: NetworkMetaData) -> Self {
        MetaData::Network(md)
    }
}

impl MetaDataControl for MetaData {
    fn get_column_count(&mut self) -> Result<usize, SQLError> {
        match self {
            MetaData::Embedded(md) => md.get_column_count(),
            MetaData::Network(md) => md.get_column_count(),
        }
    }

    fn get_column_name(&mut self, column: usize) -> Result<String, SQLError> {
        match self {
            MetaData::Embedded(md) => md.get_column_name(column),
            MetaData::Network(md) => md.get_column_name(column),
        }
    }

    fn get_column_type(&mut self, column: usize) -> Result<Type, SQLError> {
        match self {
            MetaData::Embedded(md) => md.get_column_type(column),
            MetaData::Network(md) => md.get_column_type(column),
        }
    }

    fn get_column_display_size(&mut self, column: usize) -> Result<usize, SQLError> {
        match self {
            MetaData::Embedded(md) => md.get_column_display_size(column),
            MetaData::Network(md) => md.get_column_display_size(column),
        }
    }
}
