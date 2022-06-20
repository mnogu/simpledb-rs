#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{
        query::{productscan::ProductScan, scan::Scan, updatescan::UpdateScan},
        record::{layout::Layout, schema::Schema, tablescan::TableScan},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn projecttest() {
        let db = SimpleDB::new("producttest").unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let mut sch1 = Schema::new();
        sch1.add_int_field("A");
        sch1.add_string_field("B", 9);
        let layout1 = Arc::new(Layout::new(Arc::new(sch1)));
        let mut ts1 = TableScan::new(tx.clone(), "T1", layout1.clone()).unwrap();

        let mut sch2 = Schema::new();
        sch2.add_int_field("C");
        sch2.add_string_field("D", 9);
        let layout2 = Arc::new(Layout::new(Arc::new(sch2)));
        let mut ts2 = TableScan::new(tx.clone(), "T2", layout2.clone()).unwrap();

        ts1.before_first().unwrap();
        let n = 200;
        for i in 0..n {
            ts1.insert().unwrap();
            ts1.set_int("A", i).unwrap();
            ts1.set_string("B", &format!("aaa{}", i)).unwrap();
        }
        ts1.close().unwrap();

        ts2.before_first().unwrap();
        for i in 0..n {
            ts2.insert().unwrap();
            ts2.set_int("C", n - i - 1).unwrap();
            ts2.set_string("D", &format!("bbb{}", n - i - 1)).unwrap();
        }
        ts2.close().unwrap();

        let s1 = TableScan::new(tx.clone(), "T1", layout1).unwrap();
        let s2 = TableScan::new(tx.clone(), "T2", layout2).unwrap();
        let mut s3 = ProductScan::new(s1, s2).unwrap();
        let mut count = 0;
        while s3.next().unwrap() {
            assert_eq!(format!("aaa{}", count / n), s3.get_string("B").unwrap());
            count += 1;
        }
        s3.close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("producttest").unwrap();
    }
}
