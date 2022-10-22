use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::{
        constant::Constant,
        scan::{Scan, ScanControl},
    },
    tx::transaction::TransactionError,
};

#[derive(Clone, PartialEq)]
pub struct GroupValue {
    vals: HashMap<String, Constant>,
}

impl GroupValue {
    pub fn new(s: Arc<Mutex<Scan>>, fields: Vec<String>) -> Result<GroupValue, TransactionError> {
        let mut vals = HashMap::new();
        for fldname in &fields {
            vals.insert(fldname.clone(), s.lock().unwrap().get_val(fldname)?);
        }
        Ok(GroupValue { vals })
    }

    pub fn get_val(&self, fldname: &str) -> Option<&Constant> {
        self.vals.get(fldname)
    }
}
