use super::parser::Object;

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
}

impl Object for CreateIndexData {}
