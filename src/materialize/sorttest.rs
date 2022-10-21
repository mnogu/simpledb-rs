#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{
        api::{
            connection::ConnectionControl, driver::DriverControl,
            embedded::embeddeddriver::EmbeddedDriver, statement::StatementControl,
        },
        materialize::sortplan::SortPlan,
        plan::{plan::PlanControl, tableplan::TablePlan},
        query::scan::ScanControl,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn sorttest() {
        create_student_db();

        let db = SimpleDB::new("sorttest").unwrap();
        let mdm = db.md_mgr().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let p1 = TablePlan::new(tx.clone(), "student", mdm).unwrap();

        let sortfields = vec!["gradyear".to_string(), "sname".to_string()];
        let p2 = SortPlan::new(tx, p1.into(), sortfields);

        let students = [
            ("pat", 2019),
            ("amy", 2020),
            ("bob", 2020),
            ("kim", 2020),
            ("art", 2021),
            ("joe", 2021),
            ("lee", 2021),
            ("max", 2022),
            ("sue", 2022),
        ];
        let mut count = 0;
        let mut s = p2.open().unwrap();
        while s.next().unwrap() {
            assert_eq!(s.get_string("sname").unwrap(), students[count].0);
            assert_eq!(s.get_int("gradyear").unwrap(), students[count].1);
            count += 1;
        }
        assert_eq!(count, students.len());
        s.close().unwrap();

        fs::remove_dir_all("sorttest").unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("sorttest").unwrap();
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
