use crate::{record::rid::Rid, tx::transaction::TransactionError};

use super::{contant::Constant, scan::Scan};

pub trait UpdateScan: Scan {
    fn set_val(&self, fldname: &str, val: Constant);

    fn set_int(&mut self, fldname: &str, val: i32) -> Result<(), TransactionError>;

    fn set_string(&mut self, fldname: &str, val: &str) -> Result<(), TransactionError>;

    fn insert(&mut self) -> Result<(), TransactionError>;

    fn delete(&mut self) -> Result<(), TransactionError>;

    fn get_rid(&self) -> Option<Rid>;

    fn move_to_rid(&self, rid: &Rid);
}
