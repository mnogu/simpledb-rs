use crate::{buffer::buffermgr::AbortError, tx::transaction::TransactionError};

use super::{
    contant::Constant,
    scan::{Scan, ScanControl},
};

pub struct ProductScan {
    s1: Box<Scan>,
    s2: Box<Scan>,
}

impl ScanControl for ProductScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s1.before_first()?;
        self.s1.next()?;
        self.s2.before_first()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if self.s2.next()? {
            return Ok(true);
        }
        self.s2.before_first()?;
        Ok(self.s2.next()? && self.s1.next()?)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if self.s1.has_field(fldname) {
            return self.s1.get_int(fldname);
        }
        self.s2.get_int(fldname)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if self.s1.has_field(fldname) {
            return self.s1.get_string(fldname);
        }
        self.s2.get_string(fldname)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        if self.s1.has_field(fldname) {
            return self.s1.get_val(fldname);
        }
        self.s2.get_val(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.s1.has_field(fldname) || self.s2.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.s1.close()?;
        self.s2.close()?;
        Ok(())
    }
}

impl ProductScan {
    pub fn new(s1: Scan, s2: Scan) -> Result<ProductScan, TransactionError> {
        let mut ps = ProductScan {
            s1: Box::new(s1),
            s2: Box::new(s2),
        };
        ps.before_first()?;
        Ok(ps)
    }
}
