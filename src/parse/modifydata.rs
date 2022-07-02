use crate::query::{expression::Expression, predicate::Predicate};

use super::parser::Object;

pub struct ModifyData {
    tblname: String,
    fldname: String,
    newval: Expression,
    pred: Predicate,
}

impl ModifyData {
    pub fn new(tblname: &str, fldname: &str, newval: Expression, pred: Predicate) -> ModifyData {
        ModifyData {
            tblname: tblname.to_string(),
            fldname: fldname.to_string(),
            newval,
            pred,
        }
    }
}

impl Object for ModifyData {}
