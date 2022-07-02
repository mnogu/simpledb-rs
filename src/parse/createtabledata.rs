use crate::record::schema::Schema;

use super::parser::Object;

pub struct CreateTableData {
    tblname: String,
    sch: Schema,
}

impl CreateTableData {
    pub fn new(tblname: &str, sch: Schema) -> CreateTableData {
        CreateTableData {
            tblname: tblname.to_string(),
            sch,
        }
    }
}

impl Object for CreateTableData {}
