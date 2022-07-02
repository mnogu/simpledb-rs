use super::{parser::Object, querydata::QueryData};

pub struct CreateViewData {
    viewname: String,
    qry_data: QueryData,
}

impl CreateViewData {
    pub fn new(viewname: &str, qry_data: QueryData) -> CreateViewData {
        CreateViewData {
            viewname: viewname.to_string(),
            qry_data,
        }
    }
}

impl Object for CreateViewData {}
