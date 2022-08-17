#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
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
            scan::{Scan, ScanControl},
            updatescan::UpdateScanControl,
        },
        server::simpledb::SimpleDB,
    };

    #[test]
    fn indexupdatetest() {
        create_student_db();

        let db = SimpleDB::new("studentdb2").unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
        let mdm = db.md_mgr().unwrap();
        let studentplan = TablePlan::new(tx.clone(), "student", mdm.clone()).unwrap();
        let mut studentscan = match studentplan.open().unwrap() {
            Scan::Table(s) => s,
            _ => unreachable!(),
        };

        let mut indexes = HashMap::new();
        let idxinfo = mdm
            .lock()
            .unwrap()
            .get_index_info("student", tx.clone())
            .unwrap();
        for (fldname, ii) in idxinfo {
            let idx = ii.open();
            indexes.insert(fldname, idx);
        }

        studentscan.insert().unwrap();
        studentscan.set_int("sid", 11).unwrap();
        studentscan.set_string("sname", "sam").unwrap();
        studentscan.set_int("gradyear", 2023).unwrap();
        studentscan.set_int("majorid", 30).unwrap();

        let datarid = studentscan.get_rid().unwrap();
        for (fldname, idx) in &mut indexes {
            let dataval = studentscan.get_val(&fldname).unwrap();
            idx.insert(dataval, &datarid).unwrap();
        }

        studentscan.before_first().unwrap();
        while studentscan.next().unwrap() {
            if studentscan.get_string("sname").unwrap() == "joe" {
                let joe_rid = studentscan.get_rid().unwrap();
                for (fldname, idx) in &mut indexes {
                    let dataval = studentscan.get_val(&fldname).unwrap();
                    idx.delete(dataval, &joe_rid).unwrap();
                }

                studentscan.delete().unwrap();
                break;
            }
        }

        studentscan.before_first().unwrap();
        let mut i = 0;
        let snames = [
            "amy", "max", "sue", "bob", "kim", "art", "pat", "lee", "sam",
        ];
        let sids = [2, 3, 4, 5, 6, 7, 8, 9, 11];
        while studentscan.next().unwrap() {
            assert_eq!(studentscan.get_string("sname").unwrap(), snames[i]);
            assert_eq!(studentscan.get_int("sid").unwrap(), sids[i]);
            i += 1;
        }
        studentscan.close().unwrap();

        for (_, mut idx) in indexes {
            idx.close().unwrap();
        }
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("studentdb2").unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("studentdb2").unwrap();
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
