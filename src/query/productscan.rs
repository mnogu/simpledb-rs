use crate::{buffer::buffermgr::AbortError, tx::transaction::TransactionError};

use super::{contant::Constant, scan::Scan};

pub struct ProductScan<A, B>
where
    A: Scan,
    B: Scan,
{
    s1: A,
    s2: B,
}

impl<A, B> Scan for ProductScan<A, B>
where
    A: Scan,
    B: Scan,
{
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s1.before_first()?;
        self.s1.next()?;
        self.s2.before_first()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if self.s2.next()? {
            return Ok(true);
        }
        self.s2.before_first()?;
        Ok(self.s2.next()? && self.s1.next()?)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if self.s1.has_field(fldname) {
            return Ok(self.s1.get_int(fldname)?);
        }
        Ok(self.s2.get_int(fldname)?)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if self.s1.has_field(fldname) {
            return Ok(self.s1.get_string(fldname)?);
        }
        Ok(self.s2.get_string(fldname)?)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        if self.s1.has_field(fldname) {
            return Ok(self.s1.get_val(fldname)?);
        }
        Ok(self.s2.get_val(fldname)?)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.s1.has_field(fldname) || self.s2.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.s1.close()?;
        self.s2.close()?;
        Ok(())
    }
}

impl<A, B> ProductScan<A, B>
where
    A: Scan,
    B: Scan,
{
    pub fn new(s1: A, s2: B) -> Result<ProductScan<A, B>, TransactionError> {
        let mut ps = ProductScan { s1, s2 };
        ps.before_first()?;
        Ok(ps)
    }
}
