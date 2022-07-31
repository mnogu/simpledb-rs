use std::{
    fs,
    sync::{Arc, Mutex},
};

use rand::{distributions::Uniform, prelude::Distribution};
use server::simpledb::SimpleDB;

use crate::query::scan::ScanControl;

mod buffer;
mod file;
mod index;
mod log;
mod metadata;
mod parse;
mod plan;
mod query;
mod record;
mod server;
mod tx;

fn main() {
    let db = SimpleDB::new("plannertest1").unwrap();
    let planner = db.planner().unwrap();
    let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
    let cmd = "create table T1(A int, B varchar(9))";
    planner.execute_update(cmd, tx.clone()).unwrap();

    let n = 200;
    println!("Inserting {} random records.", n);
    let mut rng = rand::thread_rng();
    let die = Uniform::from(0..50);
    for _ in 0..n {
        let a = die.sample(&mut rng);
        let cmd = format!("insert into T1(A,B) values({0}, 'rec{0}')", a);
        planner.execute_update(&cmd, tx.clone()).unwrap();
    }

    let qry = "select B from T1 where A=10";
    let p = planner.create_query_plan(qry, tx.clone()).unwrap();
    let mut s = p.open().unwrap();
    while s.next().unwrap() {
        println!("{}", s.get_string("b").unwrap());
    }
    s.close().unwrap();
    tx.lock().unwrap().commit().unwrap();

    fs::remove_dir_all("plannertest1").unwrap();
}
