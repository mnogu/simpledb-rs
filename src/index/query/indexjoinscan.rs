use crate::{
    buffer::buffermgr::AbortError,
    index::index::{Index, IndexControl},
    query::{
        contant::Constant,
        scan::{Scan, ScanControl},
        updatescan::UpdateScanControl,
    },
    record::tablescan::TableScan,
    tx::transaction::TransactionError,
};

pub struct IndexJoinScan {
    lhs: Box<Scan>,
    idx: Index,
    joinfield: String,
    rhs: TableScan,
}

impl IndexJoinScan {
    pub fn new(
        lhs: Scan,
        idx: Index,
        joinfield: &str,
        rhs: TableScan,
    ) -> Result<IndexJoinScan, TransactionError> {
        let mut s = IndexJoinScan {
            lhs: Box::new(lhs),
            idx,
            joinfield: joinfield.to_string(),
            rhs,
        };
        s.before_first()?;
        Ok(s)
    }

    fn reset_index(&mut self) -> Result<(), TransactionError> {
        let searchkey = self.lhs.get_val(&self.joinfield)?;
        self.idx.before_first(searchkey)?;
        Ok(())
    }
}

impl ScanControl for IndexJoinScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.lhs.before_first()?;
        self.lhs.next()?;
        self.reset_index()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        loop {
            if self.idx.next()? {
                self.rhs.move_to_rid(&self.idx.get_data_rid()?)?;
                return Ok(true);
            }
            if !self.lhs.next()? {
                return Ok(false);
            }
            self.reset_index()?;
        }
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if self.rhs.has_field(fldname) {
            return self.rhs.get_int(fldname);
        }
        self.lhs.get_int(fldname)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        if self.rhs.has_field(fldname) {
            return self.rhs.get_val(fldname);
        }
        self.lhs.get_val(fldname)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if self.rhs.has_field(fldname) {
            return self.rhs.get_string(fldname);
        }
        self.lhs.get_string(fldname)
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.rhs.has_field(fldname) || self.lhs.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        self.lhs.close()?;
        self.idx.close()?;
        self.rhs.close()?;
        Ok(())
    }
}
