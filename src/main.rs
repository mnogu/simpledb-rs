use std::{
    fs,
    sync::{Arc, Mutex},
};

use rand::{distributions::Uniform, prelude::Distribution};

use record::{layout::Layout, recordpage::RecordPage, schema::Schema};
use server::simpledb::SimpleDB;

mod buffer;
mod file;
mod log;
mod record;
mod server;
mod tx;

fn main() {
    let db = SimpleDB::new("recordtest", 400, 8).unwrap();
    let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

    let mut sch = Schema::new();
    sch.add_int_field("A");
    sch.add_string_field("B", 9);
    let layout = Layout::new(sch);
    for fldname in layout.schema().fields() {
        let offset = layout.offset(fldname);
        println!("{} has offset {}", fldname, offset);
    }
    let blk = tx.lock().unwrap().append("testfile").unwrap();
    tx.lock().unwrap().pin(&blk).unwrap();
    let mut rp = RecordPage::new(tx.clone(), blk.clone(), layout).unwrap();
    rp.format().unwrap();

    println!("Filling the page with random records.");
    let mut slot = rp.insert_after(None).unwrap();
    let mut rng = rand::thread_rng();
    let die = Uniform::from(0..50);
    while let Some(s) = slot {
        let n = die.sample(&mut rng);
        rp.set_int(s, "A", n).unwrap();
        rp.set_string(s, "B", &format!("rec{}", n)).unwrap();
        println!("inserting into slot {}: {{{}, rec{}}}", s, n, n);
        slot = rp.insert_after(slot).unwrap();
    }

    println!("Deleting these records, whose A-values are less than 25.");
    let mut count = 0;
    slot = rp.next_after(None).unwrap();
    while let Some(s) = slot {
        let a = rp.get_int(s, "A").unwrap();
        let b = rp.get_string(s, "B").unwrap();
        if a < 25 {
            count += 1;
            println!("slot {}: {{{}, {}}}", s, a, b);
            rp.delete(s).unwrap();
        }
        slot = rp.next_after(slot).unwrap();
    }
    println!("{} values under 25 were delted.\n", count);

    println!("Here are the remaining records.");
    slot = rp.next_after(None).unwrap();
    while let Some(s) = slot {
        let a = rp.get_int(s, "A").unwrap();
        let b = rp.get_string(s, "B").unwrap();
        println!("slot {}: {{{}, {}}}", s, a, b);
        slot = rp.next_after(slot).unwrap();
    }
    tx.lock().unwrap().unpin(&blk).unwrap();
    tx.lock().unwrap().commit().unwrap();

    fs::remove_dir_all("recordtest").unwrap();
}
