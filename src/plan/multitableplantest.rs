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
        plan::{
            plan::{Plan, PlanControl},
            productplan::ProductPlan,
            selectplan::SelectPlan,
            tableplan::TablePlan,
        },
        query::{expression::Expression, predicate::Predicate, scan::ScanControl, term::Term},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn multitableplantest() {
        create_student_db();

        let db = SimpleDB::new("multitableplantest").unwrap();
        let mdm = db.md_mgr().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let p1 = TablePlan::new(tx.clone(), "student", mdm.clone()).unwrap();

        let p2 = TablePlan::new(tx, "dept", mdm).unwrap();

        let p3 = ProductPlan::new(p1.clone().into(), p2.clone().into());

        let t = Term::new(
            Expression::with_string("majorid"),
            Expression::with_string("did"),
        );
        let pred = Predicate::with_term(t);
        let p4 = SelectPlan::new(p3.clone().into(), pred);

        let e = [(9, 1), (3, 1), (27, 10), (6, 10)];
        assert_stats(1, &p1.into(), &e);
        assert_stats(2, &p2.into(), &e);
        assert_stats(3, &p3.clone().into(), &e);
        assert_stats(4, &p4.into(), &e);

        let snames = [
            "joe", "amy", "max", "sue", "bob", "kim", "art", "pat", "lee",
        ];
        let dnames = ["compsci", "math", "drama"];
        let mut students = HashSet::new();
        for sname in snames {
            for dname in dnames {
                students.insert((sname.to_string(), dname.to_string()));
            }
        }
        let mut s = p3.open().unwrap();
        while s.next().unwrap() {
            let student = (
                s.get_string("sname").unwrap(),
                s.get_string("dname").unwrap(),
            );
            assert!(students.contains(&student));
            students.remove(&student);
        }
        assert!(students.is_empty());
        s.close().unwrap();

        fs::remove_dir_all("multitableplantest").unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("multitableplantest").unwrap();
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

    fn assert_stats(n: usize, p: &Plan, e: &[(usize, usize); 4]) {
        let i = n - 1;
        assert_eq!(e[i].0, p.records_output());
        assert_eq!(e[i].1, p.blocks_accessed());
    }
}
