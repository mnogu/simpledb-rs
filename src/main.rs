use std::{
    fs,
    sync::{Arc, Mutex},
};

use query::scan::Scan;
use rand::{distributions::Uniform, prelude::Distribution};
use record::{layout::Layout, schema::Schema};
use server::simpledb::SimpleDB;

use crate::{
    query::{
        contant::Constant, expression::Expression, predicate::Predicate, projectscan::ProjectScan,
        selectscan::SelectScan, term::Term, updatescan::UpdateScan,
    },
    record::tablescan::TableScan,
};

mod buffer;
mod file;
mod index;
mod log;
mod metadata;
mod query;
mod record;
mod server;
mod tx;

fn main() {
    let db = SimpleDB::new("scantest1").unwrap();
    let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

    let mut sch1 = Schema::new();
    sch1.add_int_field("A");
    sch1.add_string_field("B", 9);
    let layout1 = Arc::new(Layout::new(Arc::new(sch1)));
    let mut s1 = TableScan::new(tx.clone(), "T", layout1.clone()).unwrap();

    s1.before_first().unwrap();
    let n = 200;
    println!("Inserting {} random records", n);
    let mut rng = rand::thread_rng();
    let die = Uniform::from(0..50);
    for _ in 0..n {
        s1.insert().unwrap();
        let k = die.sample(&mut rng);
        s1.set_int("A", k).unwrap();
        s1.set_string("B", &format!("rec{}", k)).unwrap();
    }
    s1.close().unwrap();

    let s2 = TableScan::new(tx.clone(), "T", layout1).unwrap();
    let c = Constant::with_int(10);
    let t = Term::new(Expression::with_string("A"), Expression::with_constant(c));
    let pred = Predicate::with_term(t);
    println!("The predicate is {}", pred);
    let s3 = SelectScan::new(s2, pred);
    let fields = vec!["B".to_string()];
    let mut s4 = ProjectScan::new(s3, fields);
    while s4.next().unwrap() {
        println!("{}", s4.get_string("B").unwrap());
    }
    s4.close().unwrap();
    tx.lock().unwrap().commit().unwrap();

    fs::remove_dir_all("scantest1").unwrap();
}
