mod file;
mod server;

use file::blockid::BlockId;
use file::page::Page;
use server::simpledb::SimpleDB;

fn main() {
    let db = SimpleDB::new("filetest", 400, 8);
    let fm = db.file_mgr();

    let blk = BlockId::new("testfile", 2);
    let mut p1 = Page::new(fm.block_size());
    let pos1 = 88;
    p1.set_string(pos1, "abcdefghijklm");
    let size = Page::max_length("abcdefghijklm".len());
    let pos2 = pos1 + size;
    p1.set_int(pos2, 345);
    fm.write(&blk, &mut p1).unwrap();

    let mut p2 = Page::new(fm.block_size());
    fm.read(&blk, &mut p2).unwrap();

    println!("offset {} contains {}", pos2, p2.get_int(pos2));
    println!("offset {} contains {}", pos1, p2.get_string(pos1).unwrap());
}
