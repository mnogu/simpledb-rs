#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use rand::{distributions::Uniform, prelude::Distribution};

    use crate::{
        query::{scan::Scan, updatescan::UpdateScan},
        record::{layout::Layout, schema::Schema, tablescan::TableScan},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn tablescantest() {
        let db = SimpleDB::with_params("tabletest", 400, 8).unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let mut sch = Schema::new();
        sch.add_int_field("A");
        sch.add_string_field("B", 9);
        let layout = Arc::new(Layout::new(Arc::new(sch)));

        let e = [("A", 4), ("B", 8)];
        for (i, fldname) in layout.schema().fields().iter().enumerate() {
            assert_eq!(fldname, e[i].0);

            let offset = layout.offset(fldname);
            assert_eq!(offset, e[i].1);
        }

        let mut ts = TableScan::new(tx.clone(), "T", layout.clone()).unwrap();
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..50);
        for i in 0..50 {
            ts.insert().unwrap();
            let n = die.sample(&mut rng);
            ts.set_int("A", n).unwrap();
            ts.set_string("B", &format!("rec{}", n)).unwrap();

            let rid = ts.get_rid().unwrap();
            assert_eq!(rid.block_number(), i / 19);
            assert_eq!(rid.slot(), i as usize % 19);
            assert!(n >= 0 && n < 50);
        }

        let mut count = 0;
        ts.before_first().unwrap();
        while ts.next().unwrap() {
            let a = ts.get_int("A").unwrap();
            let b = ts.get_string("B").unwrap();
            if a < 25 {
                count += 1;

                let rid = ts.get_rid().unwrap();

                let blknum = rid.block_number();
                assert!(blknum >= 0 && blknum <= 2);

                let slot = rid.slot();
                assert!(slot < 19);

                assert!(blknum * 19 + (slot as i32) < 50);

                assert!(a < 25);
                assert_eq!(format!("rec{}", a), b);

                ts.delete().unwrap();
            }
        }
        assert!(count >= 0 && count <= 50);

        ts.before_first().unwrap();
        while ts.next().unwrap() {
            let a = ts.get_int("A").unwrap();
            let b = ts.get_string("B").unwrap();

            let rid = ts.get_rid().unwrap();

            let blknum = rid.block_number();
            assert!(blknum >= 0 && blknum <= 2);

            let slot = rid.slot();
            assert!(slot < 19);

            assert!(blknum * 19 + (slot as i32) < 50);

            assert!(a >= 25);
            assert_eq!(format!("rec{}", a), b);
        }
        ts.close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("tabletest").unwrap();
    }
}
