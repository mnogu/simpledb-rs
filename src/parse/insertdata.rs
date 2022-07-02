use crate::query::contant::Constant;

use super::parser::Object;

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
}

impl Object for InsertData {}
