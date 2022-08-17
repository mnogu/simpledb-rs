use crate::{
    buffer::buffermgr::AbortError, query::contant::Constant, record::rid::Rid,
    tx::transaction::TransactionError,
};

use super::hash::hashindex::HashIndex;

pub trait IndexControl {
    fn before_first(&mut self, searchkey: Constant) -> Result<(), TransactionError>;
    fn next(&mut self) -> Result<bool, TransactionError>;
    fn get_data_rid(&mut self) -> Result<Rid, TransactionError>;
    fn insert(&mut self, val: Constant, rid: &Rid) -> Result<(), TransactionError>;
    fn delete(&mut self, val: Constant, rid: &Rid) -> Result<(), TransactionError>;
    fn close(&mut self) -> Result<(), AbortError>;
}

pub enum Index {
    Hash(HashIndex),
}

impl From<HashIndex> for Index {
    fn from(i: HashIndex) -> Self {
        Index::Hash(i)
    }
}

impl IndexControl for Index {
    fn before_first(&mut self, searchkey: Constant) -> Result<(), TransactionError> {
        match self {
            Index::Hash(i) => i.before_first(searchkey),
        }
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        match self {
            Index::Hash(i) => i.next(),
        }
    }

    fn get_data_rid(&mut self) -> Result<Rid, TransactionError> {
        match self {
            Index::Hash(i) => i.get_data_rid(),
        }
    }

    fn insert(&mut self, val: Constant, rid: &Rid) -> Result<(), TransactionError> {
        match self {
            Index::Hash(i) => i.insert(val, rid),
        }
    }

    fn delete(&mut self, val: Constant, rid: &Rid) -> Result<(), TransactionError> {
        match self {
            Index::Hash(i) => i.delete(val, rid),
        }
    }

    fn close(&mut self) -> Result<(), AbortError> {
        match self {
            Index::Hash(i) => i.close(),
        }
    }
}