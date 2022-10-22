use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::{
    plan::plan::{Plan, PlanControl},
    query::{
        scan::{Scan, ScanControl},
        updatescan::UpdateScanControl,
    },
    record::{schema::Schema, tablescan::TableScan},
    tx::transaction::{Transaction, TransactionError},
};

use super::{
    materializeplan::MaterializePlan, recordcomparator::RecordComparator, sortscan::SortScan,
    temptable::TempTable,
};

#[derive(Clone)]
pub struct SortPlan {
    tx: Arc<Mutex<Transaction>>,
    p: Plan,
    sch: Arc<Schema>,
    comp: RecordComparator,
}

impl SortPlan {
    pub fn new(tx: Arc<Mutex<Transaction>>, p: Plan, sortfields: Vec<String>) -> SortPlan {
        let sch = p.schema();
        let comp = RecordComparator::new(sortfields);
        SortPlan { tx, p, sch, comp }
    }

    fn split_into_runs(
        &self,
        src: Arc<Mutex<Scan>>,
    ) -> Result<Vec<Arc<TempTable>>, TransactionError> {
        let mut temps = Vec::new();
        src.lock().unwrap().before_first()?;
        if !src.lock().unwrap().next()? {
            return Ok(temps);
        }
        let mut currenttemp = Arc::new(TempTable::new(self.tx.clone(), self.sch.clone()));
        temps.push(currenttemp.clone());
        let mut currentscan = Arc::new(Mutex::new(currenttemp.open()?));
        while self.copy(src.clone(), currentscan.clone())? {
            if self.comp.partial_cmp(src.clone(), currentscan.clone()) == Some(Ordering::Less) {
                currentscan.lock().unwrap().close()?;
                currenttemp = Arc::new(TempTable::new(self.tx.clone(), self.sch.clone()));
                temps.push(currenttemp.clone());
                currentscan = Arc::new(Mutex::new(currenttemp.open()?));
            }
        }
        currentscan.lock().unwrap().close()?;
        Ok(temps)
    }

    fn do_a_merge_iteration(
        &self,
        runs: &mut Vec<Arc<TempTable>>,
    ) -> Result<Vec<Arc<TempTable>>, TransactionError> {
        let mut result = Vec::new();
        while runs.len() > 1 {
            let p1 = runs.remove(0);
            let p2 = runs.remove(0);
            result.push(Arc::new(self.merge_two_runs(&p1, &p2)?));
        }
        if runs.len() == 1 {
            if let Some(run) = runs.get(0) {
                result.push(run.clone());
            }
        }
        Ok(result)
    }

    fn merge_two_runs(
        &self,
        p1: &TempTable,
        p2: &TempTable,
    ) -> Result<TempTable, TransactionError> {
        let src1 = Arc::new(Mutex::new(p1.open()?));
        let src2 = Arc::new(Mutex::new(p2.open()?));
        let result = TempTable::new(self.tx.clone(), self.sch.clone());
        let dest = Arc::new(Mutex::new(result.open()?));

        let mut hasmore1 = src1.lock().unwrap().next()?;
        let mut hasmore2 = src2.lock().unwrap().next()?;
        while hasmore1 && hasmore2 {
            if self.comp.partial_cmp(src1.clone(), src2.clone()) == Some(Ordering::Less) {
                hasmore1 = self.copy(src1.clone(), dest.clone())?;
            } else {
                hasmore2 = self.copy(src2.clone(), dest.clone())?;
            }
        }

        if hasmore1 {
            while hasmore1 {
                hasmore1 = self.copy(src1.clone(), dest.clone())?;
            }
        } else {
            while hasmore2 {
                hasmore2 = self.copy(src2.clone(), dest.clone())?;
            }
        }
        src1.lock().unwrap().close()?;
        src2.lock().unwrap().close()?;
        dest.lock().unwrap().close()?;
        Ok(result)
    }

    fn copy<T: ScanControl>(
        &self,
        src: Arc<Mutex<T>>,
        dest: Arc<Mutex<TableScan>>,
    ) -> Result<bool, TransactionError> {
        dest.lock().unwrap().insert()?;
        for fldname in self.sch.fields() {
            dest.lock()
                .unwrap()
                .set_val(fldname, src.lock().unwrap().get_val(fldname)?)?;
        }
        src.lock().unwrap().next()
    }
}

impl PlanControl for SortPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let src = Arc::new(Mutex::new(self.p.open()?));
        let mut runs = self.split_into_runs(src.clone())?;
        src.lock().unwrap().close()?;
        while runs.len() > 2 {
            runs = self.do_a_merge_iteration(&mut runs)?;
        }
        Ok(SortScan::new(runs, self.comp.clone())?.into())
    }

    fn blocks_accessed(&self) -> usize {
        let mp = MaterializePlan::new(self.tx.clone(), self.p.clone());
        mp.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.p.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        self.p.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.sch.clone()
    }
}
