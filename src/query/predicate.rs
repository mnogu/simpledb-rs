use std::fmt;

use crate::tx::transaction::TransactionError;

use super::{scan::Scan, term::Term};

pub struct Predicate {
    terms: Vec<Term>,
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.terms
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(" and ")
        )
    }
}

impl Predicate {
    pub fn new() -> Predicate {
        Predicate { terms: Vec::new() }
    }

    pub fn with_term(t: Term) -> Predicate {
        let terms = vec![t];
        Predicate { terms }
    }

    pub fn conjoin_with(&mut self, pred: Predicate) {
        self.terms.extend(pred.terms)
    }

    pub fn is_satisfied<A: Scan>(&self, s: &mut A) -> Result<bool, TransactionError> {
        for t in &self.terms {
            if !t.is_satisfied(s)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
