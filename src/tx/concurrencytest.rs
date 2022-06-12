#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use crate::{
        buffer::buffermgr::BufferMgr,
        file::{blockid::BlockId, filemgr::FileMgr},
        log::logmgr::LogMgr,
        server::simpledb::SimpleDB,
        tx::transaction::Transaction,
    };

    #[test]
    fn concurrencytest() {
        let mut db = SimpleDB::new("concurrencytest", 400, 8).unwrap();
        let fm = db.file_mgr();
        let lm = db.log_mgr();
        let bm = db.buffer_mgr();

        let fm_a = fm.clone();
        let lm_a = lm.clone();
        let bm_a = bm.clone();
        let handler_a = thread::spawn(move || run_a(fm_a, lm_a, bm_a));

        let fm_b = fm.clone();
        let lm_b = lm.clone();
        let bm_b = bm.clone();
        let handler_b = thread::spawn(move || run_b(fm_b, lm_b, bm_b));

        let fm_c = fm.clone();
        let lm_c = lm.clone();
        let bm_c = bm.clone();
        let handler_c = thread::spawn(move || run_c(fm_c, lm_c, bm_c));

        handler_a.join().unwrap();
        handler_b.join().unwrap();
        handler_c.join().unwrap();

        fs::remove_dir_all("concurrencytest").unwrap();
    }

    fn run_a(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) {
        let mut tx_a = Transaction::new(fm, lm, bm).unwrap();
        let blk1 = BlockId::new("testfile", 1);
        let blk2 = BlockId::new("testfile", 2);
        tx_a.pin(&blk1).unwrap();
        tx_a.pin(&blk2).unwrap();

        tx_a.get_int(&blk1, 0).unwrap();

        thread::sleep(Duration::from_millis(20));

        tx_a.get_int(&blk2, 0).unwrap();

        tx_a.commit().unwrap();
    }

    fn run_b(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) {
        let mut tx_b = Transaction::new(fm, lm, bm).unwrap();
        let blk1 = BlockId::new("testfile", 1);
        let blk2 = BlockId::new("testfile", 2);
        tx_b.pin(&blk1).unwrap();
        tx_b.pin(&blk2).unwrap();

        tx_b.set_int(&blk2, 0, 0, false).unwrap();

        thread::sleep(Duration::from_millis(20));

        tx_b.get_int(&blk1, 0).unwrap();

        tx_b.commit().unwrap();
    }

    fn run_c(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) {
        let mut tx_c = Transaction::new(fm, lm, bm).unwrap();
        let blk1 = BlockId::new("testfile", 1);
        let blk2 = BlockId::new("testfile", 2);
        tx_c.pin(&blk1).unwrap();
        tx_c.pin(&blk2).unwrap();
        thread::sleep(Duration::from_millis(10));

        tx_c.set_int(&blk1, 0, 0, false).unwrap();

        thread::sleep(Duration::from_millis(20));

        tx_c.get_int(&blk2, 0).unwrap();

        tx_c.commit().unwrap();
    }
}
