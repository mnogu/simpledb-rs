use crate::query::predicate::Predicate;

use super::parser::ObjectControl;

pub struct DeleteData {
    tblname: String,
    pred: Predicate,
}

impl DeleteData {
    pub fn new(tblname: &str, pred: Predicate) -> DeleteData {
        DeleteData {
            tblname: tblname.to_string(),
            pred,
        }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn pred(&self) -> Predicate {
        self.pred.clone()
    }
}

impl ObjectControl for DeleteData {}
