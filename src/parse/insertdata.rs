use crate::query::constant::Constant;

use super::parser::ObjectControl;

pub struct InsertData {
    tblname: String,
    flds: Vec<String>,
    vals: Vec<Constant>,
}

impl InsertData {
    pub fn new(tblname: &str, flds: Vec<String>, vals: Vec<Constant>) -> InsertData {
        InsertData {
            tblname: tblname.to_string(),
            flds,
            vals,
        }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn fields(&self) -> Vec<String> {
        self.flds.clone()
    }

    pub fn vals(&self) -> Vec<Constant> {
        self.vals.clone()
    }
}

impl ObjectControl for InsertData {}
