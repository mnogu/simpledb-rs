use crate::{
    buffer::buffermgr::AbortError,
    index::index::{Index, IndexControl},
    query::{constant::Constant, scan::ScanControl, updatescan::UpdateScanControl},
    record::tablescan::TableScan,
    tx::transaction::TransactionError,
};

pub struct IndexSelectScan {
    ts: TableScan,
    idx: Index,
    val: Constant,
}

impl IndexSelectScan {
    pub fn new(
        ts: TableScan,
        idx: Index,
        val: Constant,
    ) -> Result<IndexSelectScan, TransactionError> {
        let mut s = IndexSelectScan { ts, idx, val };
        s.before_first()?;
        Ok(s)
    }
}

impl ScanControl for IndexSelectScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.idx.before_first(self.val.clone())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        let ok = self.idx.next()?;
        if ok {
            let rid = self.idx.get_data_rid()?;
            self.ts.move_to_rid(&rid)?;
        }
        Ok(ok)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        self.ts.get_int(fldname)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        self.ts.get_string(fldname)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        self.ts.get_val(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.ts.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.idx.close()?;
        self.ts.close()?;
        Ok(())
    }
}
