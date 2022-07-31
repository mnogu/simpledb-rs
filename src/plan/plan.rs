use std::sync::Arc;

use crate::{
    parse::badsyntaxerror::BadSyntaxError, query::scan::Scan, record::schema::Schema,
    tx::transaction::TransactionError,
};

#[derive(Debug)]
pub enum PlanError {
    Transaction(TransactionError),
    Syntax(BadSyntaxError),
    General,
}

impl From<TransactionError> for PlanError {
    fn from(e: TransactionError) -> Self {
        PlanError::Transaction(e)
    }
}

impl From<BadSyntaxError> for PlanError {
    fn from(e: BadSyntaxError) -> Self {
        PlanError::Syntax(e)
    }
}

pub trait Plan {
    fn open(&self) -> Result<Scan, TransactionError>;
    fn schema(&self) -> Arc<Schema>;
}
