use std::sync::{Arc, Mutex};

use crate::{
    plan::plan::{Plan, PlanControl},
    query::{
        scan::{Scan, ScanControl},
        updatescan::UpdateScanControl,
    },
    tx::transaction::{Transaction, TransactionError},
};

use super::temptable::TempTable;

#[derive(Clone)]
pub struct MaterializePlan {
    srcplan: Box<Plan>,
    tx: Arc<Mutex<Transaction>>,
}

impl MaterializePlan {
    pub fn new(tx: Arc<Mutex<Transaction>>, srcplan: Plan) -> MaterializePlan {
        MaterializePlan {
            srcplan: Box::new(srcplan),
            tx,
        }
    }
}

impl PlanControl for MaterializePlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let sch = self.srcplan.schema();
        let temp = TempTable::new(self.tx.clone(), sch.clone());
        let mut src = self.srcplan.open()?;
        let mut dest = temp.open()?;
        while src.next()? {
            dest.insert()?;
            for fldname in sch.fields() {
                dest.set_val(fldname, src.get_val(fldname)?)?;
            }
        }
        src.close()?;
        dest.before_first()?;
        Ok(Scan::Table(dest))
    }

    fn records_output(&self) -> usize {
        self.srcplan.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        self.srcplan.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<crate::record::schema::Schema> {
        self.srcplan.schema()
    }
}
