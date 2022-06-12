use std::{
    fs,
    sync::{Arc, Mutex},
};

use rand::{distributions::Uniform, prelude::Distribution};

use record::{layout::Layout, schema::Schema};
use server::simpledb::SimpleDB;

use crate::{
    query::{scan::Scan, updatescan::UpdateScan},
    record::tablescan::TableScan,
};

mod buffer;
mod file;
mod log;
mod query;
mod record;
mod server;
mod tx;

fn main() {
    let db = SimpleDB::new("tabletest", 400, 8).unwrap();
    let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

    let mut sch = Schema::new();
    sch.add_int_field("A");
    sch.add_string_field("B", 9);
    let layout = Arc::new(Layout::new(sch));
    for fldname in layout.schema().fields() {
        let offset = layout.offset(fldname);
        println!("{} has offset {}", fldname, offset);
    }

    println!("Filling the table with 50 random records.");
    let mut ts = TableScan::new(tx.clone(), "T", layout.clone()).unwrap();
    let mut rng = rand::thread_rng();
    let die = Uniform::from(0..50);
    for _ in 0..50 {
        ts.insert().unwrap();
        let n = die.sample(&mut rng);
        ts.set_int("A", n).unwrap();
        ts.set_string("B", &format!("rec{}", n)).unwrap();
        println!(
            "inserting into slot {}: {{{}, rec{}}}",
            ts.get_rid().unwrap(),
            n,
            n
        );
    }

    println!("Deleting these records, whose A-values are less than 25.");
    let mut count = 0;
    ts.before_first().unwrap();
    while ts.next().unwrap() {
        let a = ts.get_int("A").unwrap();
        let b = ts.get_string("B").unwrap();
        if a < 25 {
            count += 1;
            println!("slot {}: {{{}, {}}}", ts.get_rid().unwrap(), a, b);
            ts.delete().unwrap();
        }
    }
    println!("{} values under 25 were deleted.\n", count);

    println!("Here are the remaining records.");
    ts.before_first().unwrap();
    while ts.next().unwrap() {
        let a = ts.get_int("A").unwrap();
        let b = ts.get_string("B").unwrap();
        println!("slot {}: {{{}, {}}}", ts.get_rid().unwrap(), a, b);
    }
    ts.close().unwrap();
    tx.lock().unwrap().commit().unwrap();

    fs::remove_dir_all("tabletest").unwrap();
}
