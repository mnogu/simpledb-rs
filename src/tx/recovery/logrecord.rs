use crate::{
    file::page::Page,
    tx::transaction::{Transaction, TransactionError},
};

use super::{
    checkpointrecord::CheckPointRecord, commitrecord::CommitRecord, rollbackrecord::RollbackRecord,
    setintrecord::SetIntRecord, setstringrecord::SetStringRecord, startrecord::StartRecord,
};

#[derive(Eq, PartialEq)]
pub enum Op {
    CheckPoint = 0,
    Start = 1,
    Commit = 2,
    Rollback = 3,
    SetInt = 4,
    SetString = 5,
}

pub trait LogRecord {
    fn op(&self) -> Op;

    fn tx_number(&self) -> Option<usize>;

    fn undo(&self, tx: &mut Transaction) -> Result<(), TransactionError>;
}

pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<dyn LogRecord>, TransactionError> {
    let p = Page::with_vec(bytes);
    match p.get_int(0) {
        x if x == Op::CheckPoint as i32 => Ok(Box::new(CheckPointRecord::new())),
        x if x == Op::Start as i32 => Ok(Box::new(StartRecord::new(p))),
        x if x == Op::Commit as i32 => Ok(Box::new(CommitRecord::new(p))),
        x if x == Op::Rollback as i32 => Ok(Box::new(RollbackRecord::new(p))),
        x if x == Op::SetInt as i32 => Ok(Box::new(SetIntRecord::new(p)?)),
        x if x == Op::SetString as i32 => Ok(Box::new(SetStringRecord::new(p)?)),
        _ => Err(TransactionError::General),
    }
}
