use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::{scan::Scan, updatescan::UpdateScan},
    record::{layout::Layout, tablescan::TableScan},
    tx::transaction::{Transaction, TransactionError},
};

use super::{statinfo::StatInfo, tablemgr::TableMgr};

pub struct StatMgr {
    tbl_mgr: Arc<TableMgr>,
    tablestats: HashMap<String, StatInfo>,
    numcalls: usize,
}

impl StatMgr {
    pub fn new(
        tbl_mgr: Arc<TableMgr>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatMgr, TransactionError> {
        let tablestats = HashMap::new();
        let numcalls = 0;
        let mut sm = StatMgr {
            tbl_mgr,
            tablestats,
            numcalls,
        };
        sm.refresh_statistics(tx)?;
        Ok(sm)
    }

    pub fn get_stat_info(
        &mut self,
        tblname: &str,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo, TransactionError> {
        self.numcalls += 1;
        if self.numcalls > 100 {
            self.refresh_statistics(tx.clone())?;
        }
        let si = self.tablestats.get(tblname);
        if let Some(si) = si {
            return Ok(*si);
        }
        let si = self.calc_table_stats(tblname, layout, tx)?;
        self.tablestats.insert(tblname.to_string(), si);
        Ok(si)
    }

    fn refresh_statistics(&mut self, tx: Arc<Mutex<Transaction>>) -> Result<(), TransactionError> {
        self.tablestats = HashMap::new();
        self.numcalls = 0;
        let tcatlayout = self.tbl_mgr.get_layout("tblcat", tx.clone())?;
        let mut tcat = TableScan::new(tx.clone(), "tblcat", Arc::new(tcatlayout))?;
        while tcat.next()? {
            let tblname = tcat.get_string("tblname")?;
            let layout = self.tbl_mgr.get_layout(&tblname, tx.clone())?;
            let si = self.calc_table_stats(&tblname, Arc::new(layout), tx.clone())?;
            self.tablestats.insert(tblname, si);
        }
        tcat.close()?;
        Ok(())
    }

    fn calc_table_stats(
        &self,
        tblname: &str,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo, TransactionError> {
        let mut num_recs = 0;
        let mut numblocks = 0;
        let mut ts = TableScan::new(tx.clone(), tblname, layout)?;
        while ts.next()? {
            num_recs += 1;
            if let Some(rid) = ts.get_rid() {
                numblocks = rid.block_number() as usize + 1;
            }
        }
        ts.close()?;
        Ok(StatInfo::new(numblocks, num_recs))
    }
}
