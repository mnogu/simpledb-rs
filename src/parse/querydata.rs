use crate::query::predicate::Predicate;

pub struct QueryData {
    fields: Vec<String>,
    tables: Vec<String>,
    pred: Predicate,
}

impl QueryData {
    pub fn new(fields: Vec<String>, tables: Vec<String>, pred: Predicate) -> QueryData {
        QueryData {
            fields,
            tables,
            pred,
        }
    }
}
