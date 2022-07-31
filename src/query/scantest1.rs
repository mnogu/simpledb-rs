#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use rand::{distributions::Uniform, prelude::Distribution};

    use crate::{
        query::{
            contant::Constant, expression::Expression, predicate::Predicate,
            projectscan::ProjectScan, scan::ScanControl, selectscan::SelectScan, term::Term,
            updatescan::UpdateScanControl,
        },
        record::{layout::Layout, schema::Schema, tablescan::TableScan},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn scantest1() {
        let db = SimpleDB::new("scantest1").unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let mut sch1 = Schema::new();
        sch1.add_int_field("A");
        sch1.add_string_field("B", 9);
        let layout1 = Arc::new(Layout::new(Arc::new(sch1)));
        let mut s1 = TableScan::new(tx.clone(), "T", layout1.clone()).unwrap();

        s1.before_first().unwrap();
        let n = 200;
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..50);
        for _ in 0..n {
            s1.insert().unwrap();
            let k = die.sample(&mut rng);
            s1.set_int("A", k).unwrap();
            s1.set_string("B", &format!("rec{}", k)).unwrap();
        }
        s1.close().unwrap();

        let s2 = TableScan::new(tx.clone(), "T", layout1).unwrap();
        let c = Constant::with_int(10);
        let t = Term::new(Expression::with_string("A"), Expression::with_constant(c));
        let pred = Predicate::with_term(t);
        assert_eq!(format!("{}", pred), "A=10");
        let s3 = SelectScan::new(s2.into(), Arc::new(pred));
        let fields = vec!["B".to_string()];
        let mut s4 = ProjectScan::new(s3.into(), fields);
        let mut count = 0;
        while s4.next().unwrap() {
            assert_eq!(s4.get_string("B").unwrap(), "rec10");
            count += 1;
        }
        assert!(count >= 0 && count <= n);
        s4.close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("scantest1").unwrap();
    }
}
