#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{
        api::{
            connection::ConnectionControl, driver::DriverControl,
            embedded::embeddeddriver::EmbeddedDriver, statement::StatementControl,
        },
        materialize::mergejoinplan::MergeJoinPlan,
        plan::{plan::PlanControl, tableplan::TablePlan},
        query::scan::ScanControl,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn mergejointest() {
        create_student_db();

        let db = SimpleDB::new("mergejointest").unwrap();
        let mdm = db.md_mgr().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let p1 = TablePlan::new(tx.clone(), "dept", mdm.clone())
            .unwrap()
            .into();
        let p2 = TablePlan::new(tx.clone(), "student", mdm).unwrap().into();

        let p3 = MergeJoinPlan::new(tx, p1, p2, "did", "majorid");

        let mut records = HashSet::from([
            (10, "compsci".to_string(), 1, "joe".to_string(), 10, 2021),
            (20, "math".to_string(), 2, "amy".to_string(), 20, 2020),
            (10, "compsci".to_string(), 3, "max".to_string(), 10, 2022),
            (20, "math".to_string(), 4, "sue".to_string(), 20, 2022),
            (30, "drama".to_string(), 5, "bob".to_string(), 30, 2020),
            (20, "math".to_string(), 6, "kim".to_string(), 20, 2020),
            (30, "drama".to_string(), 7, "art".to_string(), 30, 2021),
            (20, "math".to_string(), 8, "pat".to_string(), 20, 2019),
            (10, "compsci".to_string(), 9, "lee".to_string(), 10, 2021),
        ]);
        let mut s = p3.open().unwrap();
        while s.next().unwrap() {
            let record = (
                s.get_int("did").unwrap(),
                s.get_string("dname").unwrap(),
                s.get_int("sid").unwrap(),
                s.get_string("sname").unwrap(),
                s.get_int("majorid").unwrap(),
                s.get_int("gradyear").unwrap(),
            );
            assert!(records.contains(&record));
            records.remove(&record);
        }
        assert!(records.is_empty());
        s.close().unwrap();

        fs::remove_dir_all("mergejointest").unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("mergejointest").unwrap();
        let mut stmt = conn.create_statement();

        let s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
        stmt.execute_update(s).unwrap();

        let s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
        let studvals = [
            "(1, 'joe', 10, 2021)",
            "(2, 'amy', 20, 2020)",
            "(3, 'max', 10, 2022)",
            "(4, 'sue', 20, 2022)",
            "(5, 'bob', 30, 2020)",
            "(6, 'kim', 20, 2020)",
            "(7, 'art', 30, 2021)",
            "(8, 'pat', 20, 2019)",
            "(9, 'lee', 10, 2021)",
        ];
        for studval in studvals {
            stmt.execute_update(&format!("{}{}", s, studval)).unwrap();
        }

        let s = "create table DEPT(DId int, DName varchar(8))";
        stmt.execute_update(s).unwrap();

        let s = "insert into DEPT(DId, DName) values ";
        let deptvals = ["(10, 'compsci')", "(20, 'math')", "(30, 'drama')"];
        for deptval in deptvals {
            stmt.execute_update(&format!("{}{}", s, deptval)).unwrap();
        }

        conn.close().unwrap();
    }
}
