use crate::{buffer::buffermgr::AbortError, record::rid::Rid, tx::transaction::TransactionError};

use super::{contant::Constant, predicate::Predicate, scan::Scan, updatescan::UpdateScan};
pub struct SelectScan<A>
where
    A: Scan,
{
    s: A,
    pred: Predicate,
}

impl<A> Scan for SelectScan<A>
where
    A: Scan,
{
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.s.before_first()
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        while self.s.next()? {
            if self.pred.is_satisfied(&mut self.s)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        self.s.get_int(fldname)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        self.s.get_string(fldname)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        self.s.get_val(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.s.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.s.close()
    }
}

impl<A> UpdateScan for SelectScan<A>
where
    A: UpdateScan,
{
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<(), TransactionError> {
        self.s.set_val(fldname, val)
    }

    fn set_int(&mut self, fldname: &str, val: i32) -> Result<(), TransactionError> {
        self.s.set_int(fldname, val)
    }

    fn set_string(&mut self, fldname: &str, val: &str) -> Result<(), TransactionError> {
        self.s.set_string(fldname, val)
    }

    fn insert(&mut self) -> Result<(), TransactionError> {
        self.s.insert()
    }

    fn delete(&mut self) -> Result<(), TransactionError> {
        self.s.delete()
    }

    fn get_rid(&self) -> Option<Rid> {
        self.s.get_rid()
    }

    fn move_to_rid(&mut self, rid: &Rid) -> Result<(), TransactionError> {
        self.s.move_to_rid(rid)
    }
}

impl<A> SelectScan<A>
where
    A: Scan,
{
    pub fn new(s: A, pred: Predicate) -> SelectScan<A> {
        SelectScan { s, pred }
    }
}
