use crate::query::{expression::Expression, predicate::Predicate};

use super::parser::ObjectControl;

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

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn target_field(&self) -> String {
        self.fldname.clone()
    }

    pub fn new_value(&self) -> Expression {
        self.newval.clone()
    }

    pub fn pred(&self) -> Predicate {
        self.pred.clone()
    }
}

impl ObjectControl for ModifyData {}
