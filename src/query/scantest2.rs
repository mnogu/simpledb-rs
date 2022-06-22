#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{
        query::{
            expression::Expression, predicate::Predicate, productscan::ProductScan,
            projectscan::ProjectScan, scan::Scan, selectscan::SelectScan, term::Term,
            updatescan::UpdateScan,
        },
        record::{layout::Layout, schema::Schema, tablescan::TableScan},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn scantest2() {
        let db = SimpleDB::new("scantest2").unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let mut sch1 = Schema::new();
        sch1.add_int_field("A");
        sch1.add_string_field("B", 9);
        let layout1 = Arc::new(Layout::new(Arc::new(sch1)));
        let mut us1 = TableScan::new(tx.clone(), "T1", layout1.clone()).unwrap();
        us1.before_first().unwrap();
        let n = 200;
        for i in 0..n {
            us1.insert().unwrap();
            us1.set_int("A", i).unwrap();
            us1.set_string("B", &format!("bbb{}", i)).unwrap();
        }
        us1.close().unwrap();

        let mut sch2 = Schema::new();
        sch2.add_int_field("C");
        sch2.add_string_field("D", 9);
        let layout2 = Arc::new(Layout::new(Arc::new(sch2)));
        let mut us2 = TableScan::new(tx.clone(), "T2", layout2.clone()).unwrap();
        us2.before_first().unwrap();
        for i in 0..n {
            us2.insert().unwrap();
            us2.set_int("C", n - i - 1).unwrap();
            us2.set_string("D", &format!("ddd{}", n - i - 1)).unwrap();
        }
        us2.close().unwrap();

        let s1 = TableScan::new(tx.clone(), "T1", layout1).unwrap();
        let s2 = TableScan::new(tx.clone(), "T2", layout2).unwrap();
        let s3 = ProductScan::new(s1, s2).unwrap();

        let t = Term::new(Expression::with_string("A"), Expression::with_string("C"));
        let pred = Predicate::with_term(t);
        assert_eq!(format!("{}", pred), "A=C");
        let s4 = SelectScan::new(s3, pred);

        let c = vec!["B".to_string(), "D".to_string()];
        let mut s5 = ProjectScan::new(s4, c);
        let mut count = 0;
        while s5.next().unwrap() {
            assert_eq!(s5.get_string("B").unwrap(), format!("bbb{}", count));
            assert_eq!(s5.get_string("D").unwrap(), format!("ddd{}", count));
            count += 1;
        }
        assert_eq!(count, 200);
        s5.close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("scantest2").unwrap();
    }
}
