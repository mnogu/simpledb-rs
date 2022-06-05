use std::{
    fs,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use buffer::buffermgr::BufferMgr;
use file::blockid::BlockId;
use log::logmgr::LogMgr;
use server::simpledb::SimpleDB;
use tx::transaction::Transaction;

mod buffer;
mod file;
mod log;
mod server;
mod tx;

fn main() {
    let mut db = SimpleDB::new("concurrencytest", 400, 8).unwrap();
    let lm = db.log_mgr();
    let bm = db.buffer_mgr();

    let lm_a = lm.clone();
    let bm_a = bm.clone();
    let handler_a = thread::spawn(move || run_a(lm_a, bm_a));

    let lm_b = lm.clone();
    let bm_b = bm.clone();
    let handler_b = thread::spawn(move || run_b(lm_b, bm_b));

    let lm_c = lm.clone();
    let bm_c = bm.clone();
    let handler_c = thread::spawn(move || run_c(lm_c, bm_c));

    handler_a.join().unwrap();
    handler_b.join().unwrap();
    handler_c.join().unwrap();

    fs::remove_dir_all("concurrencytest").unwrap();
}

fn run_a(lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) {
    let mut tx_a = Transaction::new(lm, bm).unwrap();
    let blk1 = BlockId::new("testfile", 1);
    let blk2 = BlockId::new("testfile", 2);
    tx_a.pin(&blk1).unwrap();
    tx_a.pin(&blk2).unwrap();
    println!("Tx A: request slock 1");
    tx_a.get_int(&blk1, 0).unwrap();
    println!("Tx A: receive slock 1");
    thread::sleep(Duration::from_millis(1000));
    println!("Tx A: request slock 2");
    tx_a.get_int(&blk2, 0).unwrap();
    println!("Tx A: receive slock 2");
    tx_a.commit().unwrap();
    println!("Tx A: commit");
}

fn run_b(lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) {
    let mut tx_b = Transaction::new(lm, bm).unwrap();
    let blk1 = BlockId::new("testfile", 1);
    let blk2 = BlockId::new("testfile", 2);
    tx_b.pin(&blk1).unwrap();
    tx_b.pin(&blk2).unwrap();
    println!("Tx B: request xlock 2");
    tx_b.set_int(&blk2, 0, 0, false).unwrap();
    println!("Tx B: receive xlock 2");
    thread::sleep(Duration::from_millis(1000));
    println!("Tx B: request slock 1");
    tx_b.get_int(&blk1, 0).unwrap();
    println!("Tx B: receive slock 1");
    tx_b.commit().unwrap();
    println!("Tx B: commit");
}

fn run_c(lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) {
    let mut tx_c = Transaction::new(lm, bm).unwrap();
    let blk1 = BlockId::new("testfile", 1);
    let blk2 = BlockId::new("testfile", 2);
    tx_c.pin(&blk1).unwrap();
    tx_c.pin(&blk2).unwrap();
    thread::sleep(Duration::from_millis(500));
    println!("Tx C: request xlock 1");
    tx_c.set_int(&blk1, 0, 0, false).unwrap();
    println!("Tx C: receive xlock 1");
    thread::sleep(Duration::from_millis(1000));
    println!("Tx C: request slock 2");
    tx_c.get_int(&blk2, 0).unwrap();
    println!("Tx C: receive slock 2");
    tx_c.commit().unwrap();
    println!("Tx C: commit");
}
