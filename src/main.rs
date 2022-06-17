use std::{
    fs,
    sync::{Arc, Mutex},
};

use metadata::metadatamgr::MetadataMgr;

use rand::{distributions::Uniform, prelude::Distribution};
use record::schema::Schema;
use server::simpledb::SimpleDB;

use crate::{
    query::updatescan::UpdateScan,
    record::{schema::Type, tablescan::TableScan},
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
    let db = SimpleDB::new("metadatamgrtest", 400, 8).unwrap();
    let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
    let mut mdm = MetadataMgr::new(true, tx.clone()).unwrap();

    let mut sch = Schema::new();
    sch.add_int_field("A");
    sch.add_string_field("B", 9);

    mdm.create_table("MyTable", Arc::new(sch), tx.clone())
        .unwrap();
    let layout = Arc::new(mdm.get_layout("MyTable", tx.clone()).unwrap());
    let size = layout.slot_size();
    let sch2 = layout.schema();
    println!("MyTable has slot size {}", size);
    println!("Its fields are:");
    for fldname in sch2.fields() {
        let type_ = match sch2.type_(fldname) {
            Type::Integer => "int".to_string(),
            Type::Varchar => {
                let strlen = sch2.length(fldname);
                format!("varchar({})", strlen)
            }
        };
        println!("{}: {}", fldname, type_);
    }

    let mut ts = TableScan::new(tx.clone(), "MyTable", layout.clone()).unwrap();
    let mut rng = rand::thread_rng();
    let die = Uniform::from(0..50);
    for _ in 0..50 {
        ts.insert().unwrap();
        let n = die.sample(&mut rng);
        ts.set_int("A", n).unwrap();
        ts.set_string("B", &format!("rec{}", n)).unwrap();
    }
    let si = mdm.get_stat_info("MyTable", layout, tx.clone()).unwrap();
    println!("B(MyTable) = {}", si.blocks_accessed());
    println!("R(MyTable) = {}", si.records_output());
    println!("V(MyTable,A) = {}", si.distinct_values("A"));
    println!("V(MyTable,B) = {}", si.distinct_values("B"));

    let viewdef = "select B from MyTable where A = 1";
    mdm.create_view("viewA", viewdef, tx.clone()).unwrap();
    let v = mdm.get_view_def("viewA", tx.clone()).unwrap();
    println!("View def = {}", v);

    mdm.create_index("indexA", "MyTable", "A", tx.clone())
        .unwrap();
    mdm.create_index("indexB", "MyTable", "B", tx.clone())
        .unwrap();
    let idxmap = mdm.get_index_info("MyTable", tx.clone()).unwrap();

    let mut ii = idxmap.get("A").unwrap();
    println!("B(indexA) = {}", ii.blocks_accessed());
    println!("R(indexA) = {}", ii.records_output());
    println!("V(indexA,A) = {}", ii.distinct_values("A"));
    println!("V(indexA,B) = {}", ii.distinct_values("B"));

    ii = idxmap.get("B").unwrap();
    println!("B(indexB) = {}", ii.blocks_accessed());
    println!("R(indexB) = {}", ii.records_output());
    println!("V(indexB,A) = {}", ii.distinct_values("A"));
    println!("V(indexB,B) = {}", ii.distinct_values("B"));
    tx.lock().unwrap().commit().unwrap();

    fs::remove_dir_all("metadatamgrtest").unwrap();
}
