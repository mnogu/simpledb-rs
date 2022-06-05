#[macro_use]
extern crate lazy_static;

use std::fs;

use file::blockid::BlockId;
use server::simpledb::SimpleDB;
use tx::transaction::Transaction;

mod buffer;
mod file;
mod log;
mod server;
mod tx;

fn main() {
    let mut db = SimpleDB::new("txtest", 400, 8).unwrap();
    let lm = db.log_mgr();

    let bm = db.buffer_mgr();

    let mut tx1 = Transaction::new(lm.clone(), bm.clone()).unwrap();
    let blk = BlockId::new("testfile", 1);
    tx1.pin(&blk).unwrap();
    tx1.set_int(&blk, 80, 1, false).unwrap();
    tx1.set_string(&blk, 40, "one", false).unwrap();
    tx1.commit().unwrap();

    let mut tx2 = Transaction::new(lm.clone(), bm.clone()).unwrap();
    tx2.pin(&blk).unwrap();
    let ival = tx2.get_int(&blk, 80).unwrap();
    let sval = tx2.get_string(&blk, 40).unwrap();
    println!("initial value at location 80 = {}", ival);
    println!("initial value at location 40 = {}", sval);
    let newival = ival + 1;
    let newsval = sval + "!";
    tx2.set_int(&blk, 80, newival, true).unwrap();
    tx2.set_string(&blk, 40, &newsval, true).unwrap();
    tx2.commit().unwrap();
    let mut tx3 = Transaction::new(lm.clone(), bm.clone()).unwrap();
    tx3.pin(&blk).unwrap();
    println!(
        "new value at location 80 = {}",
        tx3.get_int(&blk, 80).unwrap()
    );
    println!(
        "new value at location 40 = {}",
        tx3.get_string(&blk, 40).unwrap()
    );
    tx3.set_int(&blk, 80, 9999, true).unwrap();
    println!(
        "pre-rollback value at location 80 = {}",
        tx3.get_int(&blk, 80).unwrap()
    );
    tx3.rollback().unwrap();

    let mut tx4 = Transaction::new(lm.clone(), bm.clone()).unwrap();
    tx4.pin(&blk).unwrap();
    println!(
        "post-rollback value at location 80 = {}",
        tx4.get_int(&blk, 80).unwrap()
    );
    tx4.commit().unwrap();

    fs::remove_dir_all("txtest").unwrap();
}
