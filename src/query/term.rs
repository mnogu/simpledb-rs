use std::fmt;

use crate::tx::transaction::TransactionError;

use super::{expression::Expression, scan::Scan};

pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.lhs, self.rhs)
    }
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Term {
        Term { lhs, rhs }
    }

    pub fn is_satisfied(&self, s: &mut Scan) -> Result<bool, TransactionError> {
        let lhsval = self.lhs.evaluate(s)?;
        let rhsval = self.rhs.evaluate(s)?;
        Ok(lhsval == rhsval)
    }
}
