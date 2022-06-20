#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{file::blockid::BlockId, server::simpledb::SimpleDB, tx::transaction::Transaction};

    #[test]
    fn txtest() {
        let mut db = SimpleDB::with_params("txtest", 400, 8).unwrap();
        let fm = db.file_mgr();
        let lm = db.log_mgr();

        let bm = db.buffer_mgr();

        let mut tx1 = Transaction::new(fm.clone(), lm.clone(), bm.clone()).unwrap();
        let blk = BlockId::new("testfile", 1);
        tx1.pin(&blk).unwrap();
        tx1.set_int(&blk, 80, 1, false).unwrap();
        tx1.set_string(&blk, 40, "one", false).unwrap();
        tx1.commit().unwrap();

        let mut tx2 = Transaction::new(fm.clone(), lm.clone(), bm.clone()).unwrap();
        tx2.pin(&blk).unwrap();
        let ival = tx2.get_int(&blk, 80).unwrap();
        let sval = tx2.get_string(&blk, 40).unwrap();
        assert_eq!(1, ival);
        assert_eq!("one", sval);
        let newival = ival + 1;
        let newsval = sval + "!";
        tx2.set_int(&blk, 80, newival, true).unwrap();
        tx2.set_string(&blk, 40, &newsval, true).unwrap();
        tx2.commit().unwrap();
        let mut tx3 = Transaction::new(fm.clone(), lm.clone(), bm.clone()).unwrap();
        tx3.pin(&blk).unwrap();
        assert_eq!(2, tx3.get_int(&blk, 80).unwrap());
        assert_eq!("one!", tx3.get_string(&blk, 40).unwrap());
        tx3.set_int(&blk, 80, 9999, true).unwrap();
        assert_eq!(9999, tx3.get_int(&blk, 80).unwrap());
        tx3.rollback().unwrap();

        let mut tx4 = Transaction::new(fm.clone(), lm.clone(), bm.clone()).unwrap();
        tx4.pin(&blk).unwrap();
        assert_eq!(2, tx4.get_int(&blk, 80).unwrap());
        tx4.commit().unwrap();

        fs::remove_dir_all("txtest").unwrap();
    }
}
