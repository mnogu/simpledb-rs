use crate::{buffer::buffermgr::AbortError, tx::transaction::TransactionError};

use super::contant::Constant;

pub trait Scan {
    fn before_first(&mut self) -> Result<(), TransactionError>;

    fn next(&mut self) -> Result<bool, TransactionError>;

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError>;

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError>;

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError>;

    fn has_field(&self, fldname: &str) -> bool;

    fn close(&mut self) -> Result<(), AbortError>;
}
