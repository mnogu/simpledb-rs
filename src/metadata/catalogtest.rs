#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{
        metadata::tablemgr::TableMgr, query::scan::ScanControl, record::tablescan::TableScan,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn catalogtest() {
        let db = SimpleDB::with_params("catalogtest", 400, 8).unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
        let tm = TableMgr::new(true, tx.clone()).unwrap();
        let tcat_layout = tm.get_layout("tblcat", tx.clone()).unwrap();

        let mut ts = TableScan::new(tx.clone(), "tblcat", tcat_layout).unwrap();
        let mut i = 0;
        let e = [("tblcat", 28), ("fldcat", 56)];
        while ts.next().unwrap() {
            let tname = ts.get_string("tblname").unwrap();
            let slotsize = ts.get_int("slotsize").unwrap();
            assert_eq!(tname, e[i].0);
            assert_eq!(slotsize, e[i].1);
            i += 1;
        }
        assert_eq!(i, e.len());
        ts.close().unwrap();

        let fcat_layout = tm.get_layout("fldcat", tx.clone()).unwrap();
        ts = TableScan::new(tx.clone(), "fldcat", fcat_layout).unwrap();
        let mut i = 0;
        let e = [
            ("tblcat", "tblname", 4),
            ("tblcat", "slotsize", 24),
            ("fldcat", "tblname", 4),
            ("fldcat", "fldname", 24),
            ("fldcat", "type", 44),
            ("fldcat", "length", 48),
            ("fldcat", "offset", 52),
        ];
        while ts.next().unwrap() {
            let tname = ts.get_string("tblname").unwrap();
            let fname = ts.get_string("fldname").unwrap();
            let offset = ts.get_int("offset").unwrap();
            assert_eq!(tname, e[i].0);
            assert_eq!(fname, e[i].1);
            assert_eq!(offset, e[i].2);
            i += 1;
        }
        assert_eq!(i, e.len());
        ts.close().unwrap();

        fs::remove_dir_all("catalogtest").unwrap();
    }
}
