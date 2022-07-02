use crate::query::predicate::Predicate;

use super::parser::Object;

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
}

impl Object for DeleteData {}
