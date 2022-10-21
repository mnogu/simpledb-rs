use enum_dispatch::enum_dispatch;

use crate::{
    buffer::buffermgr::AbortError, query::constant::Constant, record::rid::Rid,
    tx::transaction::TransactionError,
};

use super::{btree::btreeindex::BTreeIndex, hash::hashindex::HashIndex};

#[enum_dispatch(Index)]
pub trait IndexControl {
    fn before_first(&mut self, searchkey: Constant) -> Result<(), TransactionError>;
    fn next(&mut self) -> Result<bool, TransactionError>;
    fn get_data_rid(&mut self) -> Result<Rid, TransactionError>;
    fn insert(&mut self, val: Constant, rid: &Rid) -> Result<(), TransactionError>;
    fn delete(&mut self, val: Constant, rid: &Rid) -> Result<(), TransactionError>;
    fn close(&mut self) -> Result<(), AbortError>;
}

#[enum_dispatch]
pub enum Index {
    Hash(HashIndex),
    BTree(BTreeIndex),
}
