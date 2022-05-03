use file::blockid::BlockId;

use crate::server::simpledb::SimpleDB;

mod buffer;
mod file;
mod log;
mod server;

fn main() {
    let mut db = SimpleDB::new("buffermgrtest", 400, 3).unwrap();
    let bm = db.buffer_mgr();
    bm.set_max_time(1);

    let mut buff = Vec::with_capacity(6);
    buff.push(bm.pin(&BlockId::new("testfile", 0)).unwrap());
    buff.push(bm.pin(&BlockId::new("testfile", 1)).unwrap());
    buff.push(bm.pin(&BlockId::new("testfile", 2)).unwrap());
    bm.unpin(buff[1]);
    buff[1] = 10;
    buff.push(bm.pin(&BlockId::new("testfile", 0)).unwrap());
    buff.push(bm.pin(&BlockId::new("testfile", 1)).unwrap());
    println!("Available buffers: {}", bm.available());

    println!("Attempting to pin block 3...");
    if let Err(_) = bm.pin(&BlockId::new("testfile", 3)) {
        println!("Error: No available buffers\n");
    }
    bm.unpin(buff[2]);
    buff[2] = 10;
    buff.push(bm.pin(&BlockId::new("testfile", 3)).unwrap());

    println!("Final Buffer Allocation:");
    for (i, idx) in buff.iter().enumerate() {
        if *idx != 10 {
            let b = bm.buffer(*idx);
            println!(
                "buff[{}] pinned to block {}",
                i,
                b.block().as_ref().unwrap()
            );
        }
    }
}
