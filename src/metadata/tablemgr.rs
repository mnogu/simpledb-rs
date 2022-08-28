use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::{scan::ScanControl, updatescan::UpdateScanControl},
    record::{
        layout::Layout,
        schema::{Schema, Type},
        tablescan::TableScan,
    },
    tx::transaction::{Transaction, TransactionError},
};

pub struct TableMgr {
    tcat_layout: Arc<Layout>,
    fcat_layout: Arc<Layout>,
}

impl TableMgr {
    pub const MAX_NAME: usize = 16;

    pub fn new(is_new: bool, tx: Arc<Mutex<Transaction>>) -> Result<TableMgr, TransactionError> {
        let mut tcat_schema = Schema::new();
        tcat_schema.add_string_field("tblname", TableMgr::MAX_NAME);
        tcat_schema.add_int_field("slotsize");
        let ts = Arc::new(tcat_schema);
        let tcat_layout = Arc::new(Layout::new(ts.clone()));

        let mut fcat_schema = Schema::new();
        fcat_schema.add_string_field("tblname", TableMgr::MAX_NAME);
        fcat_schema.add_string_field("fldname", TableMgr::MAX_NAME);
        fcat_schema.add_int_field("type");
        fcat_schema.add_int_field("length");
        fcat_schema.add_int_field("offset");
        let fs = Arc::new(fcat_schema);
        let fcat_layout = Arc::new(Layout::new(fs.clone()));

        let tm = TableMgr {
            tcat_layout,
            fcat_layout,
        };

        if is_new {
            tm.create_table("tblcat", ts, tx.clone())?;
            tm.create_table("fldcat", fs, tx)?;
        }

        Ok(tm)
    }

    pub fn create_table(
        &self,
        tblname: &str,
        sch: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), TransactionError> {
        let layout = Layout::new(sch.clone());

        let mut tcat = TableScan::new(tx.clone(), "tblcat", self.tcat_layout.clone())?;
        tcat.insert()?;
        tcat.set_string("tblname", tblname)?;
        tcat.set_int("slotsize", layout.slot_size() as i32)?;
        tcat.close()?;

        let mut fcat = TableScan::new(tx, "fldcat", self.fcat_layout.clone())?;
        for fldname in sch.fields() {
            fcat.insert()?;
            fcat.set_string("tblname", tblname)?;
            fcat.set_string("fldname", fldname)?;
            fcat.set_int("type", sch.type_(fldname) as i32)?;
            fcat.set_int("length", sch.length(fldname) as i32)?;
            fcat.set_int("offset", layout.offset(fldname) as i32)?;
        }
        fcat.close()?;

        Ok(())
    }

    pub fn get_layout(
        &self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Layout, TransactionError> {
        let mut size = None;
        let mut tcat = TableScan::new(tx.clone(), "tblcat", self.tcat_layout.clone())?;
        while tcat.next()? {
            if tcat.get_string("tblname")? == tblname {
                size = Some(tcat.get_int("slotsize")? as usize);
                break;
            }
        }
        tcat.close()?;

        let mut sch = Schema::new();
        let mut offsets = HashMap::new();
        let mut fcat = TableScan::new(tx, "fldcat", self.fcat_layout.clone())?;
        while fcat.next()? {
            if fcat.get_string("tblname")? == tblname {
                let fldname = fcat.get_string("fldname")?;
                let fldtype = fcat.get_int("type")?;
                let fldlen = fcat.get_int("length")?;
                let offset = fcat.get_int("offset")?;
                offsets.insert(fldname.clone(), offset as usize);

                let t = match fldtype {
                    x if x == Type::Integer as i32 => Type::Integer,
                    x if x == Type::Varchar as i32 => Type::Varchar,
                    _ => return Err(TransactionError::General),
                };
                sch.add_field(&fldname, t, fldlen as usize);
            }
        }
        fcat.close()?;

        if let Some(size) = size {
            return Ok(Layout::with_metadata(Arc::new(sch), offsets, size));
        }
        Err(TransactionError::General)
    }
}
