use std::io::Error;

use crate::{
    file::page::Page,
    log::logmgr::LogMgr,
    tx::transaction::{Transaction, TransactionError},
};

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

    pub fn write_to_log(lm: &mut LogMgr) -> Result<usize, Error> {
        let bytes = 4;
        let mut rec = Vec::with_capacity(bytes);
        rec.resize(rec.capacity(), 0);
        let mut p = Page::with_vec(rec);
        p.set_int(0, Op::CheckPoint as i32);
        lm.append(p.contents())
    }
}
