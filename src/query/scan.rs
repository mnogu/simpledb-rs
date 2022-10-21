use crate::{
    buffer::buffermgr::AbortError,
    index::query::{indexjoinscan::IndexJoinScan, indexselectscan::IndexSelectScan},
    materialize::sortscan::SortScan,
    multibuffer::{chunkscan::ChunkScan, multibufferproductscan::MultibufferProductScan},
    record::tablescan::TableScan,
    tx::transaction::TransactionError,
};

use super::{
    constant::Constant, productscan::ProductScan, projectscan::ProjectScan, selectscan::SelectScan,
};

pub trait ScanControl {
    fn before_first(&mut self) -> Result<(), TransactionError>;
    fn next(&mut self) -> Result<bool, TransactionError>;
    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError>;
    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError>;
    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError>;
    fn has_field(&self, fldname: &str) -> bool;
    fn close(&mut self) -> Result<(), AbortError>;
}

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
}

impl ScanControl for Scan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        match self {
            Scan::Product(scan) => scan.before_first(),
            Scan::Project(scan) => scan.before_first(),
            Scan::Select(scan) => scan.before_first(),
            Scan::Table(scan) => scan.before_first(),
            Scan::IndexSelect(scan) => scan.before_first(),
            Scan::IndexJoin(scan) => scan.before_first(),
            Scan::Chunk(scan) => scan.before_first(),
            Scan::MultibufferProduct(scan) => scan.before_first(),
            Scan::Sort(scan) => scan.before_first(),
        }
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        match self {
            Scan::Product(scan) => scan.next(),
            Scan::Project(scan) => scan.next(),
            Scan::Select(scan) => scan.next(),
            Scan::Table(scan) => scan.next(),
            Scan::IndexSelect(scan) => scan.next(),
            Scan::IndexJoin(scan) => scan.next(),
            Scan::Chunk(scan) => scan.next(),
            Scan::MultibufferProduct(scan) => scan.next(),
            Scan::Sort(scan) => scan.next(),
        }
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        match self {
            Scan::Product(scan) => scan.get_int(fldname),
            Scan::Project(scan) => scan.get_int(fldname),
            Scan::Select(scan) => scan.get_int(fldname),
            Scan::Table(scan) => scan.get_int(fldname),
            Scan::IndexSelect(scan) => scan.get_int(fldname),
            Scan::IndexJoin(scan) => scan.get_int(fldname),
            Scan::Chunk(scan) => scan.get_int(fldname),
            Scan::MultibufferProduct(scan) => scan.get_int(fldname),
            Scan::Sort(scan) => scan.get_int(fldname),
        }
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        match self {
            Scan::Product(scan) => scan.get_string(fldname),
            Scan::Project(scan) => scan.get_string(fldname),
            Scan::Select(scan) => scan.get_string(fldname),
            Scan::Table(scan) => scan.get_string(fldname),
            Scan::IndexSelect(scan) => scan.get_string(fldname),
            Scan::IndexJoin(scan) => scan.get_string(fldname),
            Scan::Chunk(scan) => scan.get_string(fldname),
            Scan::MultibufferProduct(scan) => scan.get_string(fldname),
            Scan::Sort(scan) => scan.get_string(fldname),
        }
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        match self {
            Scan::Product(scan) => scan.get_val(fldname),
            Scan::Project(scan) => scan.get_val(fldname),
            Scan::Select(scan) => scan.get_val(fldname),
            Scan::Table(scan) => scan.get_val(fldname),
            Scan::IndexSelect(scan) => scan.get_val(fldname),
            Scan::IndexJoin(scan) => scan.get_val(fldname),
            Scan::Chunk(scan) => scan.get_val(fldname),
            Scan::MultibufferProduct(scan) => scan.get_val(fldname),
            Scan::Sort(scan) => scan.get_val(fldname),
        }
    }

    fn has_field(&self, fldname: &str) -> bool {
        match self {
            Scan::Product(scan) => scan.has_field(fldname),
            Scan::Project(scan) => scan.has_field(fldname),
            Scan::Select(scan) => scan.has_field(fldname),
            Scan::Table(scan) => scan.has_field(fldname),
            Scan::IndexSelect(scan) => scan.has_field(fldname),
            Scan::IndexJoin(scan) => scan.has_field(fldname),
            Scan::Chunk(scan) => scan.has_field(fldname),
            Scan::MultibufferProduct(scan) => scan.has_field(fldname),
            Scan::Sort(scan) => scan.has_field(fldname),
        }
    }

    fn close(&mut self) -> Result<(), AbortError> {
        match self {
            Scan::Product(scan) => scan.close(),
            Scan::Project(scan) => scan.close(),
            Scan::Select(scan) => scan.close(),
            Scan::Table(scan) => scan.close(),
            Scan::IndexSelect(scan) => scan.close(),
            Scan::IndexJoin(scan) => scan.close(),
            Scan::Chunk(scan) => scan.close(),
            Scan::MultibufferProduct(scan) => scan.close(),
            Scan::Sort(scan) => scan.close(),
        }
    }
}

impl From<ProductScan> for Scan {
    fn from(s: ProductScan) -> Self {
        Scan::Product(s)
    }
}

impl From<ProjectScan> for Scan {
    fn from(s: ProjectScan) -> Self {
        Scan::Project(s)
    }
}

impl From<SelectScan> for Scan {
    fn from(s: SelectScan) -> Self {
        Scan::Select(s)
    }
}

impl From<TableScan> for Scan {
    fn from(s: TableScan) -> Self {
        Scan::Table(s)
    }
}

impl From<IndexSelectScan> for Scan {
    fn from(s: IndexSelectScan) -> Self {
        Scan::IndexSelect(s)
    }
}

impl From<IndexJoinScan> for Scan {
    fn from(s: IndexJoinScan) -> Self {
        Scan::IndexJoin(s)
    }
}

impl From<ChunkScan> for Scan {
    fn from(s: ChunkScan) -> Self {
        Scan::Chunk(s)
    }
}

impl From<MultibufferProductScan> for Scan {
    fn from(s: MultibufferProductScan) -> Self {
        Scan::MultibufferProduct(s)
    }
}

impl From<SortScan> for Scan {
    fn from(s: SortScan) -> Self {
        Scan::Sort(s)
    }
}
