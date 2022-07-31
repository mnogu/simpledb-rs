use super::parser::ObjectControl;

pub struct CreateIndexData {
    idxname: String,
    tblname: String,
    fldname: String,
}

impl CreateIndexData {
    pub fn new(idxname: &str, tblname: &str, fldname: &str) -> CreateIndexData {
        CreateIndexData {
            idxname: idxname.to_string(),
            tblname: tblname.to_string(),
            fldname: fldname.to_string(),
        }
    }

    pub fn index_name(&self) -> String {
        self.idxname.clone()
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn field_name(&self) -> String {
        self.fldname.clone()
    }
}

impl ObjectControl for CreateIndexData {}
