use std::{cmp, fmt};

use crate::{
    plan::plan::{Plan, PlanControl},
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::{constant::Constant, expression::Expression, scan::Scan};

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

    pub fn reduction_factor(&self, p: &Plan) -> usize {
        let lhs_name = self.lhs.as_field_name();
        let rhs_name = self.rhs.as_field_name();
        if let Some(lhs_name) = lhs_name.clone() {
            if let Some(rhs_name) = rhs_name {
                return cmp::max(p.distinct_values(&lhs_name), p.distinct_values(&rhs_name));
            }
        }
        if let Some(lhs_name) = lhs_name {
            return p.distinct_values(&lhs_name);
        }
        if let Some(rhs_name) = rhs_name {
            return p.distinct_values(&rhs_name);
        }
        if self.lhs.as_constant() == self.rhs.as_constant() {
            return 1;
        }
        usize::MAX
    }

    pub fn equates_with_constant(&self, fldname: &str) -> Option<Constant> {
        if let Some(lhs_name) = self.lhs.as_field_name() {
            if lhs_name != fldname {
                return None;
            }

            if let Some(rhs_name) = self.rhs.as_constant() {
                return Some(rhs_name);
            }
        }
        if let Some(rhs_name) = self.rhs.as_field_name() {
            if rhs_name != fldname {
                return None;
            }

            if let Some(lhs_name) = self.lhs.as_constant() {
                return Some(lhs_name);
            }
        }
        None
    }

    pub fn equates_with_field(&self, fldname: &str) -> Option<String> {
        if let Some(lhs_name) = self.lhs.as_field_name() {
            if let Some(rhs_name) = self.rhs.as_field_name() {
                if lhs_name == fldname {
                    return Some(rhs_name);
                }
                if rhs_name == fldname {
                    return Some(lhs_name);
                }
            }
        }
        None
    }

    pub fn applies_to(&self, sch: &Schema) -> bool {
        self.lhs.applies_to(sch) && self.rhs.applies_to(sch)
    }
}
