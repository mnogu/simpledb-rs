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
        index::index::IndexControl,
        plan::{plan::Plan, tableplan::TablePlan},
        query::{
            contant::Constant,
            scan::{Scan, ScanControl},
            updatescan::UpdateScanControl,
        },
        server::simpledb::SimpleDB,
    };

    #[test]
    fn indexretrievaltest() {
        create_student_db();

        let db = SimpleDB::new("studentdb").unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
        let mdm = db.md_mgr().unwrap();

        let studentplan = TablePlan::new(tx.clone(), "student", mdm.clone()).unwrap();
        let mut studentscan = match studentplan.open().unwrap() {
            Scan::Table(s) => s,
            _ => unreachable!(),
        };

        let indexes = mdm
            .lock()
            .unwrap()
            .get_index_info("student", tx.clone())
            .unwrap();
        let ii = indexes.get("majorid").unwrap();
        let mut idx = ii.open();

        let mut i = 0;
        let snames = ["amy", "sue", "kim", "pat"];
        idx.before_first(Constant::with_int(20)).unwrap();
        while idx.next().unwrap() {
            let datarid = idx.get_data_rid().unwrap();
            studentscan.move_to_rid(&datarid).unwrap();
            assert_eq!(studentscan.get_string("sname").unwrap(), snames[i]);
            i += 1;
        }

        idx.close().unwrap();
        studentscan.close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("studentdb").unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("studentdb").unwrap();
        let mut stmt = conn.create_statement();

        let s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
        stmt.execute_update(s).unwrap();

        let s = "create index idx on STUDENT(MajorId)";
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
