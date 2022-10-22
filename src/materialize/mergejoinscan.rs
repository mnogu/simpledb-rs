use crate::{
    buffer::buffermgr::AbortError,
    query::{
        constant::Constant,
        scan::{Scan, ScanControl},
    },
    tx::transaction::TransactionError,
};

use super::sortscan::SortScan;

pub struct MergeJoinScan {
    s1: Box<Scan>,
    s2: SortScan,
    fldname1: String,
    fldname2: String,
    joinval: Option<Constant>,
}

impl ScanControl for MergeJoinScan {
    fn close(&mut self) -> Result<(), AbortError> {
        self.s1.close()?;
        self.s2.close()?;
        Ok(())
    }

    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s1.before_first()?;
        self.s2.before_first()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        let mut hasmore2 = self.s2.next()?;
        if let Some(joinval) = self.joinval.clone() {
            if hasmore2 && self.s2.get_val(&self.fldname2)? == joinval {
                return Ok(true);
            }
        }
        let mut hasmore1 = self.s1.next()?;
        if let Some(joinval) = self.joinval.clone() {
            if hasmore1 && self.s1.get_val(&self.fldname1)? == joinval {
                self.s2.restore_position()?;
                return Ok(true);
            }
        }

        while hasmore1 && hasmore2 {
            let v1 = self.s1.get_val(&self.fldname1)?;
            let v2 = self.s2.get_val(&self.fldname2)?;
            if v1 < v2 {
                hasmore1 = self.s1.next()?;
            } else if v1 > v2 {
                hasmore2 = self.s2.next()?;
            } else {
                self.s2.save_position();
                self.joinval = Some(self.s2.get_val(&self.fldname2)?);
                return Ok(true);
            }
        }
        Ok(false)
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
}

impl MergeJoinScan {
    pub fn new(s1: Scan, s2: SortScan, fldname1: &str, fldname2: &str) -> MergeJoinScan {
        MergeJoinScan {
            s1: Box::new(s1),
            s2,
            fldname1: fldname1.to_string(),
            fldname2: fldname2.to_string(),
            joinval: None,
        }
    }
}
