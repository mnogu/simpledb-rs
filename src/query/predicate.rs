use std::{fmt, sync::Arc};

use crate::{plan::plan::Plan, record::schema::Schema, tx::transaction::TransactionError};

use super::{constant::Constant, scan::Scan, term::Term};

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

    pub fn is_satisfied(&self, s: &mut Scan) -> Result<bool, TransactionError> {
        for t in &self.terms {
            if !t.is_satisfied(s)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn reduction_factor(&self, p: &Plan) -> usize {
        let mut factor = 1;
        for t in self.terms.iter() {
            factor *= t.reduction_factor(p);
        }
        factor
    }

    pub fn select_sub_pred(&self, sch: Arc<Schema>) -> Option<Predicate> {
        let mut result = Predicate::new();
        for t in self.terms.iter() {
            if t.applies_to(&sch) {
                result.terms.push(t.clone());
            }
        }
        if result.terms.is_empty() {
            return None;
        }
        Some(result)
    }

    pub fn join_sub_pred(&self, sch1: Arc<Schema>, sch2: Arc<Schema>) -> Option<Predicate> {
        let mut result = Predicate::new();
        let mut newsch = Schema::new();
        newsch.add_all(&sch1);
        newsch.add_all(&sch2);
        for t in self.terms.iter() {
            if !t.applies_to(&sch1) && !t.applies_to(&sch2) && t.applies_to(&newsch) {
                result.terms.push(t.clone());
            }
        }
        if result.terms.is_empty() {
            return None;
        }
        Some(result)
    }

    pub fn equates_with_constant(&self, fldname: &str) -> Option<Constant> {
        for t in self.terms.iter() {
            let c = t.equates_with_constant(fldname);
            if c.is_some() {
                return c;
            }
        }
        None
    }

    pub fn equates_with_field(&self, fldname: &str) -> Option<String> {
        for t in self.terms.iter() {
            let s = t.equates_with_field(fldname);
            if s.is_some() {
                return s;
            }
        }
        None
    }
}
