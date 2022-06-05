use std::{
    io::Error,
    string::FromUtf8Error,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::{buffer::Buffer, buffermgr::BufferMgr},
    log::logmgr::LogMgr,
};

use super::{
    commitrecord::CommitRecord, setintrecord::SetIntRecord, setstringrecord::SetStringRecord,
    startrecord::StartRecord,
};

#[derive(Debug)]
pub enum RecoveryError {
    IO(Error),
    Utf8(FromUtf8Error),
    General,
}

impl From<Error> for RecoveryError {
    fn from(e: Error) -> Self {
        RecoveryError::IO(e)
    }
}

impl From<FromUtf8Error> for RecoveryError {
    fn from(e: FromUtf8Error) -> Self {
        RecoveryError::Utf8(e)
    }
}

pub struct RecoveryMgr {
    lm: Arc<Mutex<LogMgr>>,
    bm: Arc<Mutex<BufferMgr>>,
    txnum: usize,
}

impl RecoveryMgr {
    pub fn new(
        txnum: usize,
        lm: Arc<Mutex<LogMgr>>,
        bm: Arc<Mutex<BufferMgr>>,
    ) -> Result<RecoveryMgr, Error> {
        StartRecord::write_to_log(&mut lm.lock().unwrap(), txnum)?;
        Ok(RecoveryMgr { lm, bm, txnum })
    }

    pub fn commit(&self) -> Result<(), Error> {
        self.bm.lock().unwrap().flush_all(self.txnum)?;
        let mut lm = self.lm.lock().unwrap();
        let lsn = CommitRecord::write_to_log(&mut lm, self.txnum)?;
        lm.flush(lsn)?;
        Ok(())
    }

    pub fn set_int(
        &mut self,
        buff: &mut Buffer,
        offset: usize,
        _: i32,
    ) -> Result<usize, RecoveryError> {
        let oldval = buff.contents().get_int(offset);
        let blk = buff.block();
        if let Some(blk) = blk {
            let lsn = SetIntRecord::write_to_log(
                &mut self.lm.lock().unwrap(),
                self.txnum,
                blk.clone(),
                offset,
                oldval,
            )?;
            return Ok(lsn);
        }
        Err(RecoveryError::General)
    }

    pub fn set_string(
        &mut self,
        buff: &mut Buffer,
        offset: usize,
        _: &str,
    ) -> Result<usize, RecoveryError> {
        let oldval = buff.contents().get_string(offset)?;
        let blk = buff.block();
        if let Some(blk) = blk {
            let lsn = SetStringRecord::write_to_log(
                &mut self.lm.lock().unwrap(),
                self.txnum,
                blk.clone(),
                offset,
                &oldval,
            )?;
            return Ok(lsn);
        }
        Err(RecoveryError::General)
    }
}
