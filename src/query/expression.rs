use std::fmt;

use crate::tx::transaction::TransactionError;

use super::{contant::Constant, scan::ScanControl};

pub struct Expression {
    val: Option<Constant>,
    fldname: Option<String>,
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(val) = &self.val {
            return write!(f, "{}", val);
        }
        if let Some(fldname) = &self.fldname {
            return write!(f, "{}", fldname);
        }
        return write!(f, "");
    }
}

impl Expression {
    pub fn with_constant(val: Constant) -> Expression {
        Expression {
            val: Some(val),
            fldname: None,
        }
    }

    pub fn with_string(fldname: &str) -> Expression {
        Expression {
            val: None,
            fldname: Some(fldname.to_string()),
        }
    }

    pub fn evaluate<T: ScanControl>(&self, s: &mut T) -> Result<Constant, TransactionError> {
        if let Some(val) = &self.val {
            return Ok(val.clone());
        }
        if let Some(fldname) = &self.fldname {
            return Ok(s.get_val(fldname)?);
        }
        Err(TransactionError::General)
    }
}
