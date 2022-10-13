use crate::tx::transaction::TransactionError;

use super::{
    constant::Constant,
    scan::{Scan, ScanControl},
};

pub struct ProjectScan {
    s: Box<Scan>,
    fieldlist: Vec<String>,
}

impl ScanControl for ProjectScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s.before_first()
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        self.s.next()
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if self.has_field(fldname) {
            return self.s.get_int(fldname);
        }
        Err(TransactionError::General)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if self.has_field(fldname) {
            return self.s.get_string(fldname);
        }
        Err(TransactionError::General)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        if self.has_field(fldname) {
            return self.s.get_val(fldname);
        }
        Err(TransactionError::General)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.fieldlist.contains(&fldname.to_string())
    }

    fn close(&mut self) -> Result<(), crate::buffer::buffermgr::AbortError> {
        self.s.close()
    }
}

impl ProjectScan {
    pub fn new(s: Scan, fieldlist: Vec<String>) -> ProjectScan {
        ProjectScan {
            s: Box::new(s),
            fieldlist,
        }
    }
}
