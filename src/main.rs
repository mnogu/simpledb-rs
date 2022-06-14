use std::{
    fs,
    sync::{Arc, Mutex},
};

use metadata::tablemgr::TableMgr;

use record::schema::Schema;
use server::simpledb::SimpleDB;

use crate::record::schema::Type;

mod buffer;
mod file;
mod log;
mod metadata;
mod query;
mod record;
mod server;
mod tx;

fn main() {
    let db = SimpleDB::new("tblmgrtest", 400, 8).unwrap();
    let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
    let tm = TableMgr::new(true, tx.clone()).unwrap();

    let mut sch = Schema::new();
    sch.add_int_field("A");
    sch.add_string_field("B", 9);
    tm.create_table("MyTable", Arc::new(sch), tx.clone())
        .unwrap();

    let layout = tm.get_layout("MyTable", tx.clone()).unwrap();
    let size = layout.slot_size();
    let sch2 = layout.schema();
    println!("MyTable has slot size {}", size);
    println!("Its fields are:");
    for fldname in sch2.fields() {
        let type_ = match sch2.type_(&fldname) {
            Type::Integer => "int".to_string(),
            Type::Varchar => {
                let strlen = sch2.length(fldname);
                format!("varchar({})", strlen)
            }
        };
        println!("{}: {}", fldname, type_);
    }
    tx.lock().unwrap().commit().unwrap();

    fs::remove_dir_all("tblmgrtest").unwrap();
}
