use std::sync::{Arc, Mutex};

use crate::{
    query::{constant::Constant, scan::Scan},
    tx::transaction::TransactionError,
};

use super::aggregationfn::AggregationFnControl;

#[derive(Clone)]
pub struct CountFn {
    fldname: String,
    count: usize,
}

impl AggregationFnControl for CountFn {
    fn process_first(&mut self, _: Arc<Mutex<Scan>>) -> Result<(), TransactionError> {
        self.count = 1;
        Ok(())
    }

    fn process_next(&mut self, _: Arc<Mutex<Scan>>) -> Result<(), TransactionError> {
        self.count += 1;
        Ok(())
    }

    fn field_name(&self) -> String {
        format!("countof{}", self.fldname)
    }

    fn value(&self) -> Option<Constant> {
        Some(Constant::with_int(self.count as i32))
    }
}

impl CountFn {
    pub fn new(fldname: &str) -> CountFn {
        CountFn {
            fldname: fldname.to_string(),
            count: 0,
        }
    }
}
