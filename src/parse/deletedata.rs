use std::sync::Arc;

use crate::query::predicate::Predicate;

use super::parser::ObjectControl;

pub struct DeleteData {
    tblname: String,
    pred: Arc<Predicate>,
}

impl DeleteData {
    pub fn new(tblname: &str, pred: Predicate) -> DeleteData {
        DeleteData {
            tblname: tblname.to_string(),
            pred: Arc::new(pred),
        }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn pred(&self) -> Arc<Predicate> {
        self.pred.clone()
    }
}

impl ObjectControl for DeleteData {}
