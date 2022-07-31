use std::sync::Arc;

use crate::record::schema::Schema;

use super::parser::ObjectControl;

pub struct CreateTableData {
    tblname: String,
    sch: Arc<Schema>,
}

impl CreateTableData {
    pub fn new(tblname: &str, sch: Schema) -> CreateTableData {
        CreateTableData {
            tblname: tblname.to_string(),
            sch: Arc::new(sch),
        }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn new_schema(&self) -> Arc<Schema> {
        self.sch.clone()
    }
}

impl ObjectControl for CreateTableData {}
