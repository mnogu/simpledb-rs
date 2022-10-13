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
        index::{index::IndexControl, planner::indexselectplan::IndexSelectPlan},
        metadata::indexinfo::IndexInfo,
        plan::{
            plan::{Plan, PlanControl},
            tableplan::TablePlan,
        },
        query::{
            constant::Constant,
            scan::{Scan, ScanControl},
            updatescan::UpdateScanControl,
        },
        server::simpledb::SimpleDB,
    };

    #[test]
    fn indexselecttest() {
        create_student_db();

        let db = SimpleDB::new("indexselecttest").unwrap();
        let mdm = db.md_mgr().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let indexes = mdm
            .lock()
            .unwrap()
            .get_index_info("enroll", tx.clone())
            .unwrap();
        let sid_idx = indexes.get("studentid").unwrap();

        let enrollplan = TablePlan::new(tx.clone(), "enroll", mdm).unwrap().into();

        let c = Constant::with_int(6);

        use_index_manually(sid_idx, &enrollplan, c.clone());
        use_index_scan(sid_idx, enrollplan, c);

        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("indexselecttest").unwrap();
    }

    fn use_index_manually(ii: &IndexInfo, p: &Plan, c: Constant) {
        let mut s = match p.open().unwrap() {
            Scan::Table(s) => s,
            _ => unreachable!(),
        };
        let mut idx = ii.open().unwrap();

        idx.before_first(c).unwrap();
        let mut i = 0;
        while idx.next().unwrap() {
            let datarid = idx.get_data_rid().unwrap();
            s.move_to_rid(&datarid).unwrap();
            assert_eq!("A", s.get_string("grade").unwrap());
            assert_eq!(0, i);
            i += 1;
        }
        idx.close().unwrap();
        s.close().unwrap();
    }

    fn use_index_scan(ii: &IndexInfo, p: Plan, c: Constant) {
        let idxplan = IndexSelectPlan::new(p, ii.clone(), c);
        let mut s = idxplan.open().unwrap();

        let mut i = 0;
        while s.next().unwrap() {
            assert_eq!("A", s.get_string("grade").unwrap());
            assert_eq!(0, i);
            i += 1;
        }
        s.close().unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("indexselecttest").unwrap();
        let mut stmt = conn.create_statement();

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
