use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::buffermgr::AbortError,
    query::{constant::Constant, scan::ScanControl},
    record::tablescan::TableScan,
    tx::transaction::TransactionError,
};

use super::{recordcomparator::RecordComparator, temptable::TempTable};

pub struct SortScan {
    s1: Arc<Mutex<TableScan>>,
    s2: Option<Arc<Mutex<TableScan>>>,
    comp: RecordComparator,
    currentidx: Option<usize>,
    hasmore1: bool,
    hasmore2: bool,
}

impl SortScan {
    pub fn new(
        runs: Vec<Arc<TempTable>>,
        comp: RecordComparator,
    ) -> Result<SortScan, TransactionError> {
        let t1 = runs.get(0);
        let mut s1 = if let Some(t1) = t1 {
            t1.open()?
        } else {
            return Err(TransactionError::General);
        };
        let hasmore1 = s1.next()?;
        let t2 = runs.get(1);
        let (s2, hasmore2) = if let Some(t2) = t2 {
            let mut s2 = t2.open()?;
            let hasmore2 = s2.next()?;
            (Some(Arc::new(Mutex::new(s2))), hasmore2)
        } else {
            (None, false)
        };
        let s1 = Arc::new(Mutex::new(s1));
        Ok(SortScan {
            s1,
            s2,
            currentidx: None,
            comp,
            hasmore1,
            hasmore2,
        })
    }

    fn current_scan(&mut self) -> Result<Arc<Mutex<TableScan>>, TransactionError> {
        match self.currentidx {
            Some(1) => Ok(self.s1.clone()),
            Some(2) => {
                let s2 = self.s2.clone();
                match s2 {
                    Some(s2) => Ok(s2),
                    _ => Err(TransactionError::General),
                }
            }
            _ => Err(TransactionError::General),
        }
    }
}

impl ScanControl for SortScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.currentidx = None;
        self.s1.lock().unwrap().before_first()?;
        self.hasmore1 = self.s1.lock().unwrap().next()?;
        let s2 = self.s2.clone();
        if let Some(s2) = s2 {
            s2.lock().unwrap().before_first()?;
            self.hasmore2 = s2.lock().unwrap().next()?;
        }
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if let Some(currentscan) = self.currentidx {
            if currentscan == 1 {
                self.hasmore1 = self.s1.lock().unwrap().next()?;
            } else {
                let s2 = self.s2.clone();
                if let Some(s2) = s2 {
                    if currentscan == 2 {
                        self.hasmore2 = s2.lock().unwrap().next()?;
                    }
                }
            }
        }

        if !self.hasmore1 && !self.hasmore2 {
            return Ok(false);
        } else if self.hasmore1 && self.hasmore2 {
            let s2 = self.s2.clone();
            if let Some(s2) = s2 {
                if self.comp.partial_cmp(self.s1.clone(), s2) == Some(Ordering::Less) {
                    self.currentidx = Some(1);
                } else {
                    self.currentidx = Some(2);
                }
            } else {
                self.currentidx = Some(2);
            }
        } else if self.hasmore1 {
            self.currentidx = Some(1);
        } else if self.hasmore2 {
            self.currentidx = Some(2);
        }
        Ok(true)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.s1.lock().unwrap().close()?;
        let s2 = self.s2.clone();
        if let Some(s2) = s2 {
            s2.lock().unwrap().close()?;
        }
        Ok(())
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        self.current_scan()?.lock().unwrap().get_val(fldname)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        self.current_scan()?.lock().unwrap().get_int(fldname)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        self.current_scan()?.lock().unwrap().get_string(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        let scan = match self.currentidx {
            Some(1) => self.s1.clone(),
            Some(2) => {
                let s2 = self.s2.clone();
                match s2 {
                    Some(s2) => s2,
                    _ => return false,
                }
            }
            _ => return false,
        };
        let s = scan.lock().unwrap();
        s.has_field(fldname)
    }
}
