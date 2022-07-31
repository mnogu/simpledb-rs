use super::{parser::ObjectControl, querydata::QueryData};

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

    pub fn view_name(&self) -> String {
        self.viewname.clone()
    }

    pub fn view_def(&self) -> String {
        format!("{}", self.qry_data)
    }
}

impl ObjectControl for CreateViewData {}
