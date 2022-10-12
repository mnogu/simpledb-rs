#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{plan::plan::PlanControl, query::scan::ScanControl, server::simpledb::SimpleDB};

    #[test]
    fn plannertest2() {
        let db = SimpleDB::new("plannertest2").unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
        let planner = db.planner().unwrap();

        let mut cmd = "create table T1(A int, B varchar(9))";
        planner
            .lock()
            .unwrap()
            .execute_update(cmd, tx.clone())
            .unwrap();
        let n = 200;
        for i in 0..n {
            let a = i;
            let cmd = format!("insert into T1(A,B) values({0}, 'bbb{0}')", a);
            planner
                .lock()
                .unwrap()
                .execute_update(&cmd, tx.clone())
                .unwrap();
        }

        cmd = "create table T2(C int, D varchar(9))";
        planner
            .lock()
            .unwrap()
            .execute_update(cmd, tx.clone())
            .unwrap();
        for i in 0..n {
            let c = n - i - 1;
            let cmd = format!("insert into T2(C,D) values ({0}, 'ddd{0}')", c);
            planner
                .lock()
                .unwrap()
                .execute_update(&cmd, tx.clone())
                .unwrap();
        }

        let qry = "select B,D from T1,T2 where A=C";
        let p = planner
            .lock()
            .unwrap()
            .create_query_plan(qry, tx.clone())
            .unwrap();
        let mut s = p.open().unwrap();
        let mut bs: HashSet<String> = HashSet::from_iter((0..n).map(|x| format!("bbb{}", x)));
        let mut ds: HashSet<String> = HashSet::from_iter((0..n).map(|x| format!("ddd{}", x)));
        while s.next().unwrap() {
            let b = s.get_string("b").unwrap();
            assert!(bs.contains(&b));
            bs.remove(&b);

            let d = s.get_string("d").unwrap();
            assert!(ds.contains(&d));
            ds.remove(&d);
        }
        assert!(bs.is_empty());
        assert!(ds.is_empty());
        s.close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("plannertest2").unwrap();
    }
}
