use std::sync::{Arc, Mutex};

use enum_dispatch::enum_dispatch;

use crate::{
    query::{constant::Constant, scan::Scan},
    tx::transaction::TransactionError,
};

use super::{countfn::CountFn, maxfn::MaxFn};

#[enum_dispatch(AggregationFn)]
pub trait AggregationFnControl {
    fn process_first(&mut self, s: Arc<Mutex<Scan>>) -> Result<(), TransactionError>;
    fn process_next(&mut self, s: Arc<Mutex<Scan>>) -> Result<(), TransactionError>;
    fn field_name(&self) -> String;
    fn value(&self) -> Option<Constant>;
}

#[derive(Clone)]
#[enum_dispatch]
pub enum AggregationFn {
    Max(MaxFn),
    Count(CountFn),
}
