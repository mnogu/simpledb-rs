use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    query::{
        constant::Constant,
        scan::{Scan, ScanControl},
    },
    tx::transaction::TransactionError,
};

use super::{
    aggregationfn::{AggregationFn, AggregationFnControl},
    groupvalue::GroupValue,
};

pub struct GroupByScan {
    s: Arc<Mutex<Scan>>,
    groupfields: Vec<String>,
    aggfns: Vec<AggregationFn>,
    groupval: Option<GroupValue>,
    moregroups: bool,
}

impl ScanControl for GroupByScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s.lock().unwrap().before_first()?;
        self.moregroups = self.s.lock().unwrap().next()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if !self.moregroups {
            return Ok(false);
        }
        for f in &mut self.aggfns {
            f.process_first(self.s.clone())?;
        }
        self.groupval = Some(GroupValue::new(self.s.clone(), self.groupfields.clone())?);
        while self.s.lock().unwrap().next()? {
            self.moregroups = true;
            let gv = GroupValue::new(self.s.clone(), self.groupfields.clone())?;
            if let Some(groupval) = self.groupval.clone() {
                if groupval != gv {
                    return Ok(true);
                }
            }
            for f in &mut self.aggfns {
                f.process_next(self.s.clone())?;
            }
        }
        self.moregroups = false;
        Ok(true)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.s.lock().unwrap().close()?;
        Ok(())
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        if self.groupfields.contains(&fldname.to_string()) {
            if let Some(groupval) = &self.groupval {
                if let Some(v) = groupval.get_val(fldname) {
                    return Ok(v.clone());
                }
            }
            return Err(TransactionError::General);
        }
        for f in &self.aggfns {
            if f.field_name() == *fldname {
                if let Some(v) = f.value() {
                    return Ok(v);
                } else {
                    return Err(TransactionError::General);
                }
            }
        }
        Err(TransactionError::General)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if let Ok(v) = self.get_val(fldname) {
            if let Some(i) = v.as_int() {
                return Ok(i);
            }
        }
        Err(TransactionError::General)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if let Ok(v) = self.get_val(fldname) {
            if let Some(s) = v.as_string() {
                return Ok(s);
            }
        }
        Err(TransactionError::General)
    }

    fn has_field(&self, fldname: &str) -> bool {
        if self.groupfields.contains(&fldname.to_string()) {
            return true;
        }
        for f in &self.aggfns {
            if f.field_name() == *fldname {
                return true;
            }
        }
        false
    }
}

impl GroupByScan {
    pub fn new(
        s: Scan,
        groupfields: Vec<String>,
        aggfns: Vec<AggregationFn>,
    ) -> Result<GroupByScan, TransactionError> {
        let s = Arc::new(Mutex::new(s));
        let mut gbs = GroupByScan {
            s,
            groupfields,
            aggfns,
            groupval: None,
            moregroups: false,
        };
        gbs.before_first()?;
        Ok(gbs)
    }
}
