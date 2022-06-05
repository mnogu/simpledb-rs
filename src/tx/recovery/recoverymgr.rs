use std::{cell::RefCell, io::Error, rc::Rc, string::FromUtf8Error};

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
    lm: Rc<RefCell<LogMgr>>,
    bm: Rc<RefCell<BufferMgr>>,
    txnum: usize,
}

impl RecoveryMgr {
    pub fn new(
        txnum: usize,
        lm: Rc<RefCell<LogMgr>>,
        bm: Rc<RefCell<BufferMgr>>,
    ) -> Result<RecoveryMgr, Error> {
        StartRecord::write_to_log(&mut lm.borrow_mut(), txnum)?;
        Ok(RecoveryMgr { lm, bm, txnum })
    }

    pub fn commit(&self) -> Result<(), Error> {
        self.bm.borrow_mut().flush_all(self.txnum)?;
        let mut lm = self.lm.borrow_mut();
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
                &mut self.lm.borrow_mut(),
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
                &mut self.lm.borrow_mut(),
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
