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
        plan::plan::PlanControl,
        query::scan::ScanControl,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn plannerstudenttest() {
        create_student_db();

        let db = SimpleDB::new("plannerstudenttest").unwrap();
        let planner = db.planner().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let qry = "select sname, gradyear from student";
        let p = planner
            .lock()
            .unwrap()
            .create_query_plan(qry, tx.clone())
            .unwrap();
        let mut s = p.open().unwrap();
        let mut i = 0;
        let snames = [
            "joe", "amy", "max", "sue", "bob", "kim", "art", "pat", "lee",
        ];
        let gradyears = [2021, 2020, 2022, 2022, 2020, 2020, 2021, 2019, 2021];
        while s.next().unwrap() {
            assert_eq!(snames[i], s.get_string("sname").unwrap());
            assert_eq!(gradyears[i], s.get_int("gradyear").unwrap());
            i += 1;
        }
        assert_eq!(i, snames.len());
        assert_eq!(i, gradyears.len());
        s.close().unwrap();

        let cmd = "delete from STUDENT where MajorId = 30";
        assert_eq!(
            planner
                .lock()
                .unwrap()
                .execute_update(cmd, tx.clone())
                .unwrap(),
            2
        );

        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("plannerstudenttest").unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("plannerstudenttest").unwrap();
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
