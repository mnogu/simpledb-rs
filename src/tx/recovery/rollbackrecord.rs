use std::io::Error;

use crate::{
    file::page::Page,
    log::logmgr::LogMgr,
    tx::transaction::{Transaction, TransactionError},
};

use super::logrecord::{LogRecord, Op};

pub struct RollbackRecord {
    txnum: usize,
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> Op {
        Op::Rollback
    }

    fn tx_number(&self) -> Option<usize> {
        Some(self.txnum)
    }

    fn undo(&self, _: &mut Transaction) -> Result<(), TransactionError> {
        Ok(())
    }
}

impl RollbackRecord {
    pub fn new(p: Page) -> RollbackRecord {
        let bytes = 4;
        let tpos = bytes;
        RollbackRecord {
            txnum: p.get_int(tpos) as usize,
        }
    }

    pub fn write_to_log(lm: &mut LogMgr, txnum: usize) -> Result<usize, Error> {
        let bytes = 4;
        let mut rec = Vec::with_capacity(2 * bytes);
        rec.resize(rec.capacity(), 0);
        let mut p = Page::with_vec(rec);
        p.set_int(0, Op::Rollback as i32);
        p.set_int(bytes, txnum as i32);
        lm.append(p.contents())
    }
}
