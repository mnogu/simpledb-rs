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
        materialize::{countfn::CountFn, groupbyplan::GroupByPlan, maxfn::MaxFn},
        plan::{plan::PlanControl, tableplan::TablePlan},
        query::scan::ScanControl,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn groupbytest() {
        create_student_db();

        let db = SimpleDB::new("groupbytest").unwrap();
        let mdm = db.md_mgr().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let p1 = TablePlan::new(tx.clone(), "student", mdm).unwrap();

        let groupfields = vec!["majorid".to_string()];
        let aggfns = vec![
            MaxFn::new("gradyear").into(),
            CountFn::new("gradyear").into(),
        ];
        let p2 = GroupByPlan::new(tx, p1.into(), groupfields, aggfns);

        let mut records = HashSet::from([(10, 2022, 3), (20, 2022, 4), (30, 2021, 2)]);
        let mut s = p2.open().unwrap();
        while s.next().unwrap() {
            let record = (
                s.get_int("majorid").unwrap(),
                s.get_int("maxofgradyear").unwrap(),
                s.get_int("countofgradyear").unwrap(),
            );
            assert!(records.contains(&record));
            records.remove(&record);
        }
        assert!(records.is_empty());
        s.close().unwrap();

        fs::remove_dir_all("groupbytest").unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("groupbytest").unwrap();
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

        conn.close().unwrap();
    }
}
