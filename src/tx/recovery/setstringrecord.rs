use std::{io::Error, string::FromUtf8Error};

use crate::{
    file::{blockid::BlockId, page::Page},
    log::logmgr::LogMgr,
    tx::transaction::{Transaction, TransactionError},
};

use super::logrecord::{LogRecord, Op};

pub struct SetStringRecord {
    txnum: usize,
    offset: usize,
    val: String,
    blk: BlockId,
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> Op {
        Op::SetString
    }

    fn tx_number(&self) -> Option<usize> {
        Some(self.txnum)
    }

    fn undo(&self, tx: &mut Transaction) -> Result<(), TransactionError> {
        tx.pin(&self.blk)?;
        tx.set_string(&self.blk, self.offset, &self.val, false)?;
        tx.unpin(&self.blk)?;
        Ok(())
    }
}

impl SetStringRecord {
    pub fn new(p: Page) -> Result<SetStringRecord, FromUtf8Error> {
        let bytes = 4;
        let tpos = bytes;
        let txnum = p.get_int(tpos) as usize;
        let fpos = tpos + bytes;
        let filename = p.get_string(fpos)?;
        let bpos = fpos + Page::max_length(filename.len());
        let blknum = p.get_int(bpos);
        let blk = BlockId::new(&filename, blknum);
        let opos = bpos + bytes;
        let offset = p.get_int(opos) as usize;
        let vpos = opos + bytes;
        let val = p.get_string(vpos)?;
        Ok(SetStringRecord {
            txnum,
            offset,
            val,
            blk,
        })
    }

    pub fn write_to_log(
        lm: &mut LogMgr,
        txnum: usize,
        blk: BlockId,
        offset: usize,
        val: &str,
    ) -> Result<usize, Error> {
        let bytes = 4;
        let tpos = bytes;
        let fpos = tpos + bytes;
        let bpos = fpos + Page::max_length(blk.file_name().len());
        let opos = bpos + bytes;
        let vpos = opos + bytes;
        let reclen = vpos + Page::max_length(val.len());
        let mut rec = Vec::with_capacity(reclen);
        rec.resize(rec.capacity(), 0);
        let mut p = Page::new_with_vec(rec);
        p.set_int(0, Op::SetString as i32);
        p.set_int(tpos, txnum as i32);
        p.set_string(fpos, blk.file_name());
        p.set_int(bpos, blk.number());
        p.set_int(opos, offset as i32);
        p.set_string(vpos, val);
        lm.append(p.contents())
    }
}
