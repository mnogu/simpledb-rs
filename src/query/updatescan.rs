use crate::{
    record::{rid::Rid, tablescan::TableScan},
    tx::transaction::TransactionError,
};

use super::{contant::Constant, selectscan::SelectScan};

pub trait UpdateScanControl {
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<(), TransactionError>;

    fn set_int(&mut self, fldname: &str, val: i32) -> Result<(), TransactionError>;

    fn set_string(&mut self, fldname: &str, val: &str) -> Result<(), TransactionError>;

    fn insert(&mut self) -> Result<(), TransactionError>;

    fn delete(&mut self) -> Result<(), TransactionError>;

    fn get_rid(&self) -> Option<Rid>;

    fn move_to_rid(&mut self, rid: &Rid) -> Result<(), TransactionError>;
}

pub enum UpdateScan {
    Select(SelectScan),
    Table(TableScan),
}

impl UpdateScanControl for UpdateScan {
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<(), TransactionError> {
        match self {
            UpdateScan::Select(scan) => scan.set_val(fldname, val),
            UpdateScan::Table(scan) => scan.set_val(fldname, val),
        }
    }

    fn set_int(&mut self, fldname: &str, val: i32) -> Result<(), TransactionError> {
        match self {
            UpdateScan::Select(scan) => scan.set_int(fldname, val),
            UpdateScan::Table(scan) => scan.set_int(fldname, val),
        }
    }

    fn set_string(&mut self, fldname: &str, val: &str) -> Result<(), TransactionError> {
        match self {
            UpdateScan::Select(scan) => scan.set_string(fldname, val),
            UpdateScan::Table(scan) => scan.set_string(fldname, val),
        }
    }

    fn insert(&mut self) -> Result<(), TransactionError> {
        match self {
            UpdateScan::Select(scan) => scan.insert(),
            UpdateScan::Table(scan) => scan.insert(),
        }
    }

    fn delete(&mut self) -> Result<(), TransactionError> {
        match self {
            UpdateScan::Select(scan) => scan.delete(),
            UpdateScan::Table(scan) => scan.delete(),
        }
    }

    fn get_rid(&self) -> Option<Rid> {
        match self {
            UpdateScan::Select(scan) => scan.get_rid(),
            UpdateScan::Table(scan) => scan.get_rid(),
        }
    }

    fn move_to_rid(&mut self, rid: &Rid) -> Result<(), TransactionError> {
        match self {
            UpdateScan::Select(scan) => scan.move_to_rid(rid),
            UpdateScan::Table(scan) => scan.move_to_rid(rid),
        }
    }
}
