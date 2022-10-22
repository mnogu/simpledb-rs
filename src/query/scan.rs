use enum_dispatch::enum_dispatch;

use crate::{
    buffer::buffermgr::AbortError,
    index::query::{indexjoinscan::IndexJoinScan, indexselectscan::IndexSelectScan},
    materialize::{groupbyscan::GroupByScan, mergejoinscan::MergeJoinScan, sortscan::SortScan},
    multibuffer::{chunkscan::ChunkScan, multibufferproductscan::MultibufferProductScan},
    record::tablescan::TableScan,
    tx::transaction::TransactionError,
};

use super::{
    constant::Constant, productscan::ProductScan, projectscan::ProjectScan, selectscan::SelectScan,
};

#[enum_dispatch(Scan)]
pub trait ScanControl {
    fn before_first(&mut self) -> Result<(), TransactionError>;
    fn next(&mut self) -> Result<bool, TransactionError>;
    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError>;
    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError>;
    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError>;
    fn has_field(&self, fldname: &str) -> bool;
    fn close(&mut self) -> Result<(), AbortError>;
}

#[enum_dispatch]
pub enum Scan {
    Product(ProductScan),
    Project(ProjectScan),
    Select(SelectScan),
    Table(TableScan),
    IndexSelect(IndexSelectScan),
    IndexJoin(IndexJoinScan),
    Chunk(ChunkScan),
    MultibufferProduct(MultibufferProductScan),
    Sort(SortScan),
    GroupBy(GroupByScan),
    MergeJoin(MergeJoinScan),
}
