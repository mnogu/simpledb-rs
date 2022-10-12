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
        index::{index::IndexControl, planner::indexjoinplan::IndexJoinPlan},
        metadata::indexinfo::IndexInfo,
        plan::{
            plan::{Plan, PlanControl},
            tableplan::TablePlan,
        },
        query::{
            scan::{Scan, ScanControl},
            updatescan::UpdateScanControl,
        },
        server::simpledb::SimpleDB,
    };

    #[test]
    fn indexjointest() {
        create_student_db();

        let db = SimpleDB::new("indexjointest").unwrap();
        let mdm = db.md_mgr().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let indexes = mdm
            .lock()
            .unwrap()
            .get_index_info("enroll", tx.clone())
            .unwrap();
        let sid_idx = indexes.get("studentid").unwrap();

        let studentplan = TablePlan::new(tx.clone(), "student", mdm.clone())
            .unwrap()
            .into();
        let enrollplan = TablePlan::new(tx, "enroll", mdm).unwrap().into();

        use_index_manually(&studentplan, &enrollplan, sid_idx, "sid");
        use_index_scan(studentplan, enrollplan, sid_idx, "sid");

        fs::remove_dir_all("indexjointest").unwrap();
    }

    fn use_index_manually(p1: &Plan, p2: &Plan, ii: &IndexInfo, joinfield: &str) {
        let mut s1 = p1.open().unwrap();
        let mut s2 = match p2.open().unwrap() {
            Scan::Table(s2) => s2,
            _ => unreachable!(),
        };
        let mut idx = ii.open().unwrap();

        let mut i = 0;
        let grades = ["C", "A", "B+", "A", "B", "A"];
        while s1.next().unwrap() {
            let c = s1.get_val(joinfield).unwrap();
            idx.before_first(c).unwrap();
            while idx.next().unwrap() {
                let datarid = idx.get_data_rid().unwrap();
                s2.move_to_rid(&datarid).unwrap();
                assert_eq!(grades[i], s2.get_string("grade").unwrap());
                i += 1;
            }
        }
        assert_eq!(i, grades.len());
        idx.close().unwrap();
        s1.close().unwrap();
        s2.close().unwrap();
    }

    fn use_index_scan(p1: Plan, p2: Plan, ii: &IndexInfo, joinfield: &str) {
        let idxplan = IndexJoinPlan::new(p1, p2, ii.clone(), joinfield);
        let mut s = idxplan.open().unwrap();

        let mut i = 0;
        let grades = ["C", "A", "B+", "A", "B", "A"];
        while s.next().unwrap() {
            assert_eq!(grades[i], s.get_string("grade").unwrap());
            i += 1;
        }
        assert_eq!(i, grades.len());
        s.close().unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("indexjointest").unwrap();
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

        let s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
        stmt.execute_update(s).unwrap();

        let s = "create index idx on ENROLL(StudentId)";
        stmt.execute_update(s).unwrap();

        let s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
        let studvals = [
            "(14, 1, 13, 'A')",
            "(24, 1, 43, 'C' )",
            "(34, 2, 43, 'B+')",
            "(44, 4, 33, 'B' )",
            "(54, 4, 53, 'A' )",
            "(64, 6, 53, 'A' )",
        ];
        for studval in studvals {
            stmt.execute_update(&format!("{}{}", s, studval)).unwrap();
        }

        conn.close().unwrap();
    }
}
