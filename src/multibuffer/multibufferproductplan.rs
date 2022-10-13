use std::sync::{Arc, Mutex};

use crate::{
    materialize::{materializeplan::MaterializePlan, temptable::TempTable},
    plan::plan::{Plan, PlanControl},
    query::{
        scan::{Scan, ScanControl},
        updatescan::UpdateScanControl,
    },
    record::schema::Schema,
    tx::transaction::{Transaction, TransactionError},
};

use super::multibufferproductscan::MultibufferProductScan;

#[derive(Clone)]
pub struct MultibufferProductPlan {
    tx: Arc<Mutex<Transaction>>,
    lhs: Box<Plan>,
    rhs: Box<Plan>,
    schema: Arc<Schema>,
}

impl MultibufferProductPlan {
    pub fn new(tx: Arc<Mutex<Transaction>>, lhs: Plan, rhs: Plan) -> MultibufferProductPlan {
        let lhs: Plan = MaterializePlan::new(tx.clone(), lhs).into();
        let mut s = Schema::new();
        s.add_all(&lhs.schema());
        s.add_all(&rhs.schema());
        let schema = Arc::new(s);
        MultibufferProductPlan {
            tx,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            schema,
        }
    }

    fn copy_records_from(&self, p: &Plan) -> Result<TempTable, TransactionError> {
        let mut src = p.open()?;
        let sch = p.schema();
        let t = TempTable::new(self.tx.clone(), sch.clone());
        let mut dest = t.open()?;
        while src.next()? {
            dest.insert()?;
            for fldname in sch.fields() {
                dest.set_val(fldname, src.get_val(fldname)?)?;
            }
        }
        src.close()?;
        dest.close()?;
        Ok(t)
    }
}

impl PlanControl for MultibufferProductPlan {
    fn open(&self) -> Result<Scan, TransactionError> {
        let leftscan = self.lhs.open()?;
        let tt = self.copy_records_from(&self.rhs)?;
        Ok(MultibufferProductScan::new(
            self.tx.clone(),
            leftscan,
            &tt.table_name(),
            tt.get_layout(),
        )?
        .into())
    }

    fn blocks_accessed(&self) -> usize {
        let avail = self.tx.lock().unwrap().available_buffs();
        let size = MaterializePlan::new(self.tx.clone(), *self.rhs.clone()).blocks_accessed();
        let numchunks = size / avail;
        self.rhs.blocks_accessed() + (self.lhs.blocks_accessed() * numchunks)
    }

    fn records_output(&self) -> usize {
        self.lhs.records_output() * self.rhs.records_output()
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        if self.lhs.schema().has_field(fldname) {
            return self.lhs.distinct_values(fldname);
        }
        self.rhs.distinct_values(fldname)
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
