use std::sync::Arc;

use crate::query::{expression::Expression, predicate::Predicate};

use super::parser::ObjectControl;

pub struct ModifyData {
    tblname: String,
    fldname: String,
    newval: Arc<Expression>,
    pred: Arc<Predicate>,
}

impl ModifyData {
    pub fn new(tblname: &str, fldname: &str, newval: Expression, pred: Predicate) -> ModifyData {
        ModifyData {
            tblname: tblname.to_string(),
            fldname: fldname.to_string(),
            newval: Arc::new(newval),
            pred: Arc::new(pred),
        }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn target_field(&self) -> String {
        self.fldname.clone()
    }

    pub fn new_value(&self) -> Arc<Expression> {
        self.newval.clone()
    }

    pub fn pred(&self) -> Arc<Predicate> {
        self.pred.clone()
    }
}

impl ObjectControl for ModifyData {}
