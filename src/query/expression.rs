use std::fmt;

use crate::{record::schema::Schema, tx::transaction::TransactionError};

use super::{constant::Constant, scan::ScanControl};

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
        write!(f, "")
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
            return s.get_val(fldname);
        }
        Err(TransactionError::General)
    }

    pub fn as_constant(&self) -> Option<Constant> {
        self.val.clone()
    }

    pub fn as_field_name(&self) -> Option<String> {
        self.fldname.clone()
    }

    pub fn applies_to(&self, sch: &Schema) -> bool {
        if self.val.is_some() {
            return true;
        }
        if let Some(fldname) = self.fldname.clone() {
            return sch.has_field(&fldname);
        }
        false
    }
}
