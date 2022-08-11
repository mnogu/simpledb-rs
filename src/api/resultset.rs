use super::{
    driver::SQLError, embedded::embeddedresultset::EmbeddedResultSet, metadata::MetaData,
    network::networkresultset::NetworkResultSet,
};

pub trait ResultSetControl {
    fn next(&mut self) -> Result<bool, SQLError>;
    fn get_int(&mut self, fldname: &str) -> Result<i32, SQLError>;
    fn get_string(&mut self, fldname: &str) -> Result<String, SQLError>;
    fn get_meta_data(&self) -> MetaData;
    fn close(&mut self) -> Result<(), SQLError>;
}

pub enum ResultSet {
    Embedded(EmbeddedResultSet),
    Network(NetworkResultSet),
}

impl From<EmbeddedResultSet> for ResultSet {
    fn from(rs: EmbeddedResultSet) -> Self {
        ResultSet::Embedded(rs)
    }
}

impl From<NetworkResultSet> for ResultSet {
    fn from(rs: NetworkResultSet) -> Self {
        ResultSet::Network(rs)
    }
}

impl ResultSetControl for ResultSet {
    fn next(&mut self) -> Result<bool, SQLError> {
        match self {
            ResultSet::Embedded(rs) => rs.next(),
            ResultSet::Network(rs) => rs.next(),
        }
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, SQLError> {
        match self {
            ResultSet::Embedded(rs) => rs.get_int(fldname),
            ResultSet::Network(rs) => rs.get_int(fldname),
        }
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, SQLError> {
        match self {
            ResultSet::Embedded(rs) => rs.get_string(fldname),
            ResultSet::Network(rs) => rs.get_string(fldname),
        }
    }

    fn get_meta_data(&self) -> MetaData {
        match self {
            ResultSet::Embedded(rs) => rs.get_meta_data(),
            ResultSet::Network(rs) => rs.get_meta_data(),
        }
    }

    fn close(&mut self) -> Result<(), SQLError> {
        match self {
            ResultSet::Embedded(rs) => rs.close(),
            ResultSet::Network(rs) => rs.close(),
        }
    }
}
