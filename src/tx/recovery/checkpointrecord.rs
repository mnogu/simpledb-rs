use crate::tx::transaction::{Transaction, TransactionError};

use super::logrecord::{LogRecord, Op};

pub struct CheckPointRecord {}

impl LogRecord for CheckPointRecord {
    fn op(&self) -> Op {
        Op::CheckPoint
    }

    fn tx_number(&self) -> Option<usize> {
        None
    }

    fn undo(&self, _: &mut Transaction) -> Result<(), TransactionError> {
        Ok(())
    }
}

impl CheckPointRecord {
    pub fn new() -> CheckPointRecord {
        CheckPointRecord {}
    }
}
