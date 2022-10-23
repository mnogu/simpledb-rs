use std::sync::{Arc, Mutex};

use crate::{
    query::{
        constant::Constant,
        scan::{Scan, ScanControl},
    },
    tx::transaction::TransactionError,
};

use super::aggregationfn::AggregationFnControl;

#[derive(Clone)]
pub struct MaxFn {
    fldname: String,
    val: Option<Constant>,
}

impl AggregationFnControl for MaxFn {
    fn process_first(&mut self, s: Arc<Mutex<Scan>>) -> Result<(), TransactionError> {
        self.val = Some(s.lock().unwrap().get_val(&self.fldname)?);
        Ok(())
    }

    fn process_next(&mut self, s: Arc<Mutex<Scan>>) -> Result<(), TransactionError> {
        let newval = s.lock().unwrap().get_val(&self.fldname)?;
        if let Some(val) = &self.val {
            if newval > *val {
                self.val = Some(newval);
            }
        }
        Ok(())
    }
    fn field_name(&self) -> String {
        format!("maxof{}", self.fldname)
    }

    fn value(&self) -> Option<Constant> {
        self.val.clone()
    }
}

impl MaxFn {
    #[allow(dead_code)]
    pub fn new(fldname: &str) -> MaxFn {
        MaxFn {
            fldname: fldname.to_string(),
            val: None,
        }
    }
}
