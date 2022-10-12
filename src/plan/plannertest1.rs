#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use rand::{distributions::Uniform, prelude::Distribution};

    use crate::{plan::plan::PlanControl, query::scan::ScanControl, server::simpledb::SimpleDB};

    #[test]
    fn plannertest1() {
        let db = SimpleDB::new("plannertest1").unwrap();
        let planner = db.planner().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
        let cmd = "create table T1(A int, B varchar(9))";
        planner
            .lock()
            .unwrap()
            .execute_update(cmd, tx.clone())
            .unwrap();

        let n = 200;
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..50);
        for _ in 0..n {
            let a = die.sample(&mut rng);
            let cmd = format!("insert into T1(A,B) values({0}, 'rec{0}')", a);
            planner
                .lock()
                .unwrap()
                .execute_update(&cmd, tx.clone())
                .unwrap();
        }

        let qry = "select B from T1 where A=10";
        let p = planner
            .lock()
            .unwrap()
            .create_query_plan(qry, tx.clone())
            .unwrap();
        let mut s = p.open().unwrap();
        while s.next().unwrap() {
            assert_eq!(s.get_string("b").unwrap(), "rec10");
        }
        s.close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("plannertest1").unwrap();
    }
}
