use std::sync::{Arc, Mutex};

use crate::{
    index::hash::hashindex::HashIndex,
    record::{
        layout::Layout,
        schema::{Schema, Type},
    },
    tx::transaction::Transaction,
};

use super::statinfo::StatInfo;

pub struct IndexInfo {
    idxname: String,
    fldname: String,
    tx: Arc<Mutex<Transaction>>,
    tbl_schema: Arc<Schema>,
    idx_layout: Layout,
    si: StatInfo,
}

fn create_idx_layout(fldname: &str, tbl_schema: &Schema) -> Layout {
    let mut sch = Schema::new();
    sch.add_int_field("block");
    sch.add_int_field("id");
    match tbl_schema.type_(fldname) {
        Type::Integer => sch.add_int_field("dataval"),
        Type::Varchar => {
            let fldlen = tbl_schema.length(fldname);
            sch.add_string_field("dataval", fldlen)
        }
    }
    Layout::new(Arc::new(sch))
}

impl IndexInfo {
    pub fn new(
        idxname: &str,
        fldname: &str,
        tbl_schema: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
        si: StatInfo,
    ) -> IndexInfo {
        let idx_layout = create_idx_layout(fldname, &tbl_schema);
        IndexInfo {
            idxname: idxname.to_string(),
            fldname: fldname.to_string(),
            tx,
            tbl_schema,
            idx_layout,
            si,
        }
    }

    pub fn blocks_accessed(&self) -> usize {
        let rpb = self.tx.lock().unwrap().block_size() / self.idx_layout.slot_size();
        let numblocks = self.si.records_output() / rpb;
        HashIndex::search_cost(numblocks, rpb)
    }

    pub fn records_output(&self) -> usize {
        self.si.records_output() / self.si.distinct_values(&self.fldname)
    }

    pub fn distinct_values(&self, fname: &str) -> usize {
        if self.fldname == fname {
            return 1;
        }
        self.si.distinct_values(&self.fldname)
    }
}
