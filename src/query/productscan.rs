use std::sync::{Arc, Mutex};

use crate::{buffer::buffermgr::AbortError, tx::transaction::TransactionError};

use super::{
    contant::Constant,
    scan::{Scan, ScanControl},
};

pub struct ProductScan {
    s1: Arc<Mutex<Scan>>,
    s2: Arc<Mutex<Scan>>,
}

impl ScanControl for ProductScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s1.lock().unwrap().before_first()?;
        self.s1.lock().unwrap().next()?;
        self.s2.lock().unwrap().before_first()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if self.s2.lock().unwrap().next()? {
            return Ok(true);
        }
        self.s2.lock().unwrap().before_first()?;
        Ok(self.s2.lock().unwrap().next()? && self.s1.lock().unwrap().next()?)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if self.s1.lock().unwrap().has_field(fldname) {
            return self.s1.lock().unwrap().get_int(fldname);
        }
        self.s2.lock().unwrap().get_int(fldname)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if self.s1.lock().unwrap().has_field(fldname) {
            return self.s1.lock().unwrap().get_string(fldname);
        }
        self.s2.lock().unwrap().get_string(fldname)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        if self.s1.lock().unwrap().has_field(fldname) {
            return self.s1.lock().unwrap().get_val(fldname);
        }
        self.s2.lock().unwrap().get_val(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.s1.lock().unwrap().has_field(fldname) || self.s2.lock().unwrap().has_field(fldname)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.s1.lock().unwrap().close()?;
        self.s2.lock().unwrap().close()?;
        Ok(())
    }
}

impl ProductScan {
    pub fn new(
        s1: Arc<Mutex<Scan>>,
        s2: Arc<Mutex<Scan>>,
    ) -> Result<ProductScan, TransactionError> {
        let mut ps = ProductScan { s1, s2 };
        ps.before_first()?;
        Ok(ps)
    }
}
