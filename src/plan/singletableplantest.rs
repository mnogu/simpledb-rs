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
        plan::{
            plan::{Plan, PlanControl},
            projectplan::ProjectPlan,
            selectplan::SelectPlan,
            tableplan::TablePlan,
        },
        query::{
            constant::Constant, expression::Expression, predicate::Predicate, scan::ScanControl,
            term::Term,
        },
        server::simpledb::SimpleDB,
    };

    #[test]
    fn singletableplantest() {
        create_student_db();

        let db = SimpleDB::new("singletableplantest").unwrap();
        let mdm = db.md_mgr().unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let p1 = TablePlan::new(tx, "student", mdm).unwrap();

        let t = Term::new(
            Expression::with_string("majorid"),
            Expression::with_constant(Constant::with_int(10)),
        );
        let pred = Arc::new(Predicate::with_term(t));
        let p2 = SelectPlan::new(p1.clone().into(), pred);

        let t2 = Term::new(
            Expression::with_string("gradyear"),
            Expression::with_constant(Constant::with_int(2020)),
        );
        let pred2 = Arc::new(Predicate::with_term(t2));
        let p3 = SelectPlan::new(p2.clone().into(), pred2);

        let c = vec!["sname", "majorid", "gradyear"]
            .iter()
            .map(|x| x.to_string())
            .collect();
        let p4 = ProjectPlan::new(p3.clone().into(), c);

        let e = [(9, 1), (2, 1), (0, 1), (0, 1)];
        assert_stats(1, &p1.into(), &e);
        assert_stats(2, &p2.clone().into(), &e);
        assert_stats(3, &p3.into(), &e);
        assert_stats(4, &p4.into(), &e);

        let students = [
            (1, "joe", 10, 2021),
            (3, "max", 10, 2022),
            (9, "lee", 10, 2021),
        ];
        let mut count = 0;
        let mut s = p2.open().unwrap();
        while s.next().unwrap() {
            assert_eq!(s.get_int("sid").unwrap(), students[count].0);
            assert_eq!(s.get_string("sname").unwrap(), students[count].1);
            assert_eq!(s.get_int("majorid").unwrap(), students[count].2);
            assert_eq!(s.get_int("gradyear").unwrap(), students[count].3);
            count += 1;
        }
        assert_eq!(count, students.len());
        s.close().unwrap();

        fs::remove_dir_all("singletableplantest").unwrap();
    }

    fn create_student_db() {
        let d = EmbeddedDriver::new();
        let mut conn = d.connect("singletableplantest").unwrap();
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

    fn assert_stats(n: usize, p: &Plan, e: &[(usize, usize); 4]) {
        let i = n - 1;
        assert_eq!(e[i].0, p.records_output());
        assert_eq!(e[i].1, p.blocks_accessed());
    }
}
