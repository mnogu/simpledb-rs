use std::{
    io::Error,
    sync::{Arc, Mutex},
};

use crate::{
    file::page::Page,
    log::logmgr::LogMgr,
    tx::transaction::{Transaction, TransactionError},
};

use super::logrecord::{LogRecord, Op};

pub struct StartRecord {
    txnum: usize,
}

impl LogRecord for StartRecord {
    fn op(&self) -> Op {
        Op::Start
    }

    fn tx_number(&self) -> Option<usize> {
        Some(self.txnum)
    }

    fn undo(&self, _: &mut Transaction) -> Result<(), TransactionError> {
        Ok(())
    }
}

impl StartRecord {
    pub fn new(p: Page) -> StartRecord {
        let bytes = 4;
        let tpos = bytes;
        StartRecord {
            txnum: p.get_int(tpos) as usize,
        }
    }

    pub fn write_to_log(lm: &Arc<Mutex<LogMgr>>, txnum: usize) -> Result<usize, Error> {
        let bytes = 4;
        let mut rec = Vec::with_capacity(2 * bytes);
        rec.resize(rec.capacity(), 0);
        let mut p = Page::with_vec(rec);
        p.set_int(0, Op::Start as i32);
        p.set_int(bytes, txnum as i32);
        lm.lock().unwrap().append(p.contents())
    }
}
