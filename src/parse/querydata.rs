use std::{fmt, sync::Arc};

use crate::query::predicate::Predicate;

pub struct QueryData {
    fields: Vec<String>,
    tables: Vec<String>,
    pred: Arc<Predicate>,
}

impl fmt::Display for QueryData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut predstring = format!("{}", self.pred);
        if !predstring.is_empty() {
            predstring = format!("where {}", predstring);
        }

        write!(
            f,
            "select {} from {}{}",
            self.fields.join(", "),
            self.tables.join(", "),
            predstring,
        )
    }
}

impl QueryData {
    pub fn new(fields: Vec<String>, tables: Vec<String>, pred: Predicate) -> QueryData {
        QueryData {
            fields,
            tables,
            pred: Arc::new(pred),
        }
    }

    pub fn fields(&self) -> Vec<String> {
        self.fields.clone()
    }

    pub fn tables(&self) -> Vec<String> {
        self.tables.clone()
    }

    pub fn pred(&self) -> Arc<Predicate> {
        self.pred.clone()
    }
}
