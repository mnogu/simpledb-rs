mod file;
mod log;
mod server;

use file::page::Page;
use log::logmgr::LogMgr;
use server::simpledb::SimpleDB;

fn main() {
    let mut db = SimpleDB::new("logtest", 400, 8).unwrap();
    let lm = db.log_mgr();

    print_log_records(lm, "The initial empty log file:");
    println!("done");
    create_records(lm, 1, 35);
    print_log_records(lm, "The log file now has these records:");
    create_records(lm, 36, 70);
    lm.flush(65).unwrap();
    print_log_records(lm, "The log file now has these records:");
}

fn print_log_records(lm: &mut LogMgr, msg: &str) {
    println!("{}", msg);
    let iter = lm.iterator().unwrap();
    for rec in iter {
        let p = Page::new_with_vec(rec);
        let s = p.get_string(0).unwrap();
        let npos = Page::max_length(s.len());
        let val = p.get_int(npos);
        println!("[{}, {}]", s, val);
    }
    println!();
}

fn create_records(lm: &mut LogMgr, start: usize, end: usize) {
    print!("Creating records: ");
    for i in start..=end {
        let s = format!("{}{}", "record", i);
        let rec = create_log_record(s.as_str(), i + 100);
        let lsn = lm.append(&rec).unwrap();
        print!("{} ", lsn);
    }
    println!();
}

fn create_log_record(s: &str, n: usize) -> Vec<u8> {
    let spos = 0;
    let npos = Page::max_length(s.len());
    let b: Vec<u8> = vec![0; npos + 4];
    let mut p = Page::new_with_vec(b);
    p.set_string(spos, s);
    p.set_int(npos, n as i32);
    p.contents().to_vec()
}
