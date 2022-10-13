use std::sync::Arc;

use crate::{buffer::buffermgr::AbortError, record::rid::Rid, tx::transaction::TransactionError};

use super::{
    constant::Constant,
    predicate::Predicate,
    scan::{Scan, ScanControl},
    updatescan::UpdateScanControl,
};

pub struct SelectScan {
    s: Box<Scan>,
    pred: Arc<Predicate>,
}

impl ScanControl for SelectScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s.before_first()
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        while self.s.next()? {
            if self.pred.is_satisfied(&mut self.s)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        self.s.get_int(fldname)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        self.s.get_string(fldname)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        self.s.get_val(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.s.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.s.close()
    }
}

impl UpdateScanControl for SelectScan {
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<(), TransactionError> {
        match &mut *self.s {
            Scan::Select(scan) => scan.set_val(fldname, val),
            Scan::Table(scan) => scan.set_val(fldname, val),
            _ => Ok(()),
        }
    }

    fn set_int(&mut self, fldname: &str, val: i32) -> Result<(), TransactionError> {
        match &mut *self.s {
            Scan::Select(scan) => scan.set_int(fldname, val),
            Scan::Table(scan) => scan.set_int(fldname, val),
            _ => Ok(()),
        }
    }

    fn set_string(&mut self, fldname: &str, val: &str) -> Result<(), TransactionError> {
        match &mut *self.s {
            Scan::Select(scan) => scan.set_string(fldname, val),
            Scan::Table(scan) => scan.set_string(fldname, val),
            _ => Ok(()),
        }
    }

    fn insert(&mut self) -> Result<(), TransactionError> {
        match &mut *self.s {
            Scan::Select(scan) => scan.insert(),
            Scan::Table(scan) => scan.insert(),
            _ => Ok(()),
        }
    }

    fn delete(&mut self) -> Result<(), TransactionError> {
        match &mut *self.s {
            Scan::Select(scan) => scan.delete(),
            Scan::Table(scan) => scan.delete(),
            _ => Ok(()),
        }
    }

    fn get_rid(&self) -> Option<Rid> {
        match &*self.s {
            Scan::Select(scan) => scan.get_rid(),
            Scan::Table(scan) => scan.get_rid(),
            _ => None,
        }
    }

    fn move_to_rid(&mut self, rid: &Rid) -> Result<(), TransactionError> {
        match &mut *self.s {
            Scan::Select(scan) => scan.move_to_rid(rid),
            Scan::Table(scan) => scan.move_to_rid(rid),
            _ => Ok(()),
        }
    }
}

impl SelectScan {
    pub fn new(s: Scan, pred: Arc<Predicate>) -> SelectScan {
        SelectScan {
            s: Box::new(s),
            pred,
        }
    }
}
