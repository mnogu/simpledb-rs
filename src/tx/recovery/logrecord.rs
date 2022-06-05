use crate::{
    file::page::Page,
    tx::transaction::{Transaction, TransactionError},
};

use super::{
    checkpointrecord::CheckPointRecord, commitrecord::CommitRecord, rollbackrecord::RollbackRecord,
    setintrecord::SetIntRecord, setstringrecord::SetStringRecord, startrecord::StartRecord,
};

#[derive(PartialEq)]
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

impl TryFrom<i32> for Op {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Op::CheckPoint),
            1 => Ok(Op::Start),
            2 => Ok(Op::Commit),
            3 => Ok(Op::Rollback),
            4 => Ok(Op::SetInt),
            5 => Ok(Op::SetString),
            _ => Err(()),
        }
    }
}

pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<dyn LogRecord>, TransactionError> {
    let p = Page::new_with_vec(bytes);
    match (p.get_int(0)).try_into() {
        Ok(Op::CheckPoint) => Ok(Box::new(CheckPointRecord::new())),
        Ok(Op::Start) => Ok(Box::new(StartRecord::new(p))),
        Ok(Op::Commit) => Ok(Box::new(CommitRecord::new(p))),
        Ok(Op::Rollback) => Ok(Box::new(RollbackRecord::new(p))),
        Ok(Op::SetInt) => Ok(Box::new(SetIntRecord::new(p)?)),
        Ok(Op::SetString) => Ok(Box::new(SetStringRecord::new(p)?)),
        Err(_) => Err(TransactionError::General),
    }
}
