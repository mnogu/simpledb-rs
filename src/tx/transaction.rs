use std::{
    io::Error,
    string::FromUtf8Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    buffer::buffermgr::{AbortError, BufferMgr},
    file::{blockid::BlockId, filemgr::FileMgr},
    log::logmgr::LogMgr,
    tx::recovery::rollbackrecord::RollbackRecord,
};

use super::{
    bufferlist::BufferList,
    concurrency::concurrencymgr::ConcurrencyMgr,
    recovery::{
        logrecord::{create_log_record, Op},
        recoverymgr::{RecoveryError, RecoveryMgr},
    },
};

static NEXT_TX_NUM: AtomicUsize = AtomicUsize::new(0);

fn next_tx_num() -> usize {
    NEXT_TX_NUM.fetch_add(1, Ordering::SeqCst);
    NEXT_TX_NUM.load(Ordering::SeqCst)
}

#[derive(Debug)]
pub enum TransactionError {
    Abort(AbortError),
    Recovery(RecoveryError),
    Utf8(FromUtf8Error),
    IO(Error),
    General,
}

impl From<AbortError> for TransactionError {
    fn from(e: AbortError) -> Self {
        TransactionError::Abort(e)
    }
}

impl From<RecoveryError> for TransactionError {
    fn from(e: RecoveryError) -> Self {
        TransactionError::Recovery(e)
    }
}

impl From<FromUtf8Error> for TransactionError {
    fn from(e: FromUtf8Error) -> Self {
        TransactionError::Utf8(e)
    }
}

impl From<Error> for TransactionError {
    fn from(e: Error) -> Self {
        TransactionError::IO(e)
    }
}

pub struct Transaction {
    recovery_mgr: RecoveryMgr,
    concur_mgr: ConcurrencyMgr,
    bm: Arc<Mutex<BufferMgr>>,
    fm: Arc<FileMgr>,
    txnum: usize,
    mybuffers: BufferList,
    lm: Arc<Mutex<LogMgr>>,
}

impl Transaction {
    const END_OF_FILE: i32 = -1;

    pub fn new(
        fm: Arc<FileMgr>,
        lm: Arc<Mutex<LogMgr>>,
        bm: Arc<Mutex<BufferMgr>>,
    ) -> Result<Transaction, Error> {
        let txnum = next_tx_num();
        let recovery_mgr = RecoveryMgr::new(txnum, lm.clone(), bm.clone())?;
        let concur_mgr = ConcurrencyMgr::new();
        let mybuffers = BufferList::new(bm.clone());
        Ok(Transaction {
            recovery_mgr,
            concur_mgr,
            bm,
            fm,
            txnum,
            mybuffers,
            lm,
        })
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        self.recovery_mgr.commit()?;
        println!("transaction {} committed", self.txnum);
        self.concur_mgr.release();
        self.mybuffers.unpin_all();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        self.do_rollback()?;
        self.bm.lock().unwrap().flush_all(self.txnum)?;
        let mut lm = self.lm.lock().unwrap();
        let lsn = RollbackRecord::write_to_log(&mut lm, self.txnum)?;
        lm.flush(lsn)?;
        println!("transaction {} rolled back", self.txnum);
        self.concur_mgr.release();
        self.mybuffers.unpin_all();
        Ok(())
    }

    pub fn pin(&mut self, blk: &BlockId) -> Result<(), AbortError> {
        self.mybuffers.pin(blk)
    }

    pub fn unpin(&mut self, blk: &BlockId) -> Result<(), AbortError> {
        self.mybuffers.unpin(blk)
    }

    pub fn get_int(&mut self, blk: &BlockId, offset: usize) -> Result<i32, TransactionError> {
        self.concur_mgr.s_lock(blk)?;
        let idx = self.mybuffers.get_index(blk);
        if let Some(idx) = idx {
            let mut bm = self.bm.lock().unwrap();
            let buff = bm.buffer(idx);
            return Ok(buff.contents().get_int(offset));
        }
        Err(TransactionError::General)
    }

    pub fn get_string(&mut self, blk: &BlockId, offset: usize) -> Result<String, TransactionError> {
        self.concur_mgr.s_lock(blk)?;
        let idx = self.mybuffers.get_index(blk);
        if let Some(idx) = idx {
            let mut bm = self.bm.lock().unwrap();
            let buff = bm.buffer(idx);
            let s = buff.contents().get_string(offset)?;
            return Ok(s);
        }
        Err(TransactionError::General)
    }

    pub fn set_int(
        &mut self,
        blk: &BlockId,
        offset: usize,
        val: i32,
        ok_to_log: bool,
    ) -> Result<(), TransactionError> {
        self.concur_mgr.x_lock(blk)?;
        let idx = self.mybuffers.get_index(blk);
        if let Some(idx) = idx {
            let mut bm = self.bm.lock().unwrap();
            let buff = bm.buffer(idx);
            let mut lsn = None;
            if ok_to_log {
                lsn = Some(self.recovery_mgr.set_int(buff, offset, val)?);
            }
            let p = buff.contents();
            p.set_int(offset, val);
            if let Some(lsn) = lsn {
                buff.set_modified(self.txnum, lsn as i32);
            }
            return Ok(());
        }
        Err(TransactionError::General)
    }

    pub fn set_string(
        &mut self,
        blk: &BlockId,
        offset: usize,
        val: &str,
        ok_to_log: bool,
    ) -> Result<(), TransactionError> {
        self.concur_mgr.x_lock(blk)?;
        let idx = self.mybuffers.get_index(blk);
        if let Some(idx) = idx {
            let mut bm = self.bm.lock().unwrap();
            let buff = bm.buffer(idx);
            let mut lsn = None;
            if ok_to_log {
                lsn = Some(self.recovery_mgr.set_string(buff, offset, val)?);
            }
            let p = buff.contents();
            p.set_string(offset, val);
            if let Some(lsn) = lsn {
                buff.set_modified(self.txnum, lsn as i32);
            }
            return Ok(());
        }
        Err(TransactionError::General)
    }

    fn do_rollback(&mut self) -> Result<(), TransactionError> {
        let mut recs = Vec::new();
        for bytes in self.lm.lock().unwrap().iterator()? {
            let rec = create_log_record(bytes)?;
            if let Some(txnum) = rec.tx_number() {
                if txnum == self.txnum {
                    if rec.op() == Op::Start {
                        break;
                    }
                    recs.push(rec);
                }
            }
        }

        for rec in recs.iter() {
            rec.undo(self)?;
        }
        Ok(())
    }

    pub fn size(&mut self, filename: &str) -> Result<usize, TransactionError> {
        let dummyblk = BlockId::new(filename, Transaction::END_OF_FILE);
        self.concur_mgr.s_lock(&dummyblk)?;
        Ok(self.fm.length(filename)?)
    }

    pub fn append(&mut self, filename: &str) -> Result<BlockId, TransactionError> {
        let dummyblk = BlockId::new(filename, Transaction::END_OF_FILE);
        self.concur_mgr.x_lock(&dummyblk)?;
        Ok(self.fm.append(filename)?)
    }

    pub fn block_size(&self) -> usize {
        self.fm.block_size()
    }
}
