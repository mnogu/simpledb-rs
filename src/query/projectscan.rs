use crate::tx::transaction::TransactionError;

use super::{contant::Constant, scan::Scan};

pub struct ProjectScan<A>
where
    A: Scan,
{
    s: A,
    fieldlist: Vec<String>,
}

impl<A> Scan for ProjectScan<A>
where
    A: Scan,
{
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s.before_first()
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        self.s.next()
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if self.has_field(fldname) {
            return self.s.get_int(fldname);
        }
        Err(TransactionError::General)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if self.has_field(fldname) {
            return self.s.get_string(fldname);
        }
        Err(TransactionError::General)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        if self.has_field(fldname) {
            return self.s.get_val(fldname);
        }
        Err(TransactionError::General)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.fieldlist.contains(&fldname.to_string())
    }

    fn close(&mut self) -> Result<(), crate::buffer::buffermgr::AbortError> {
        self.s.close()
    }
}

impl<A> ProjectScan<A>
where
    A: Scan,
{
    pub fn new(s: A, fieldlist: Vec<String>) -> ProjectScan<A> {
        ProjectScan { s, fieldlist }
    }
}
