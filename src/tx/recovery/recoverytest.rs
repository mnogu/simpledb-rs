#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{
        buffer::buffermgr::BufferMgr,
        file::{blockid::BlockId, filemgr::FileMgr, page::Page},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn recoverytest() {
        let mut db = SimpleDB::new("recoverytest", 400, 8).unwrap();
        let fm = db.file_mgr();
        let bm = db.buffer_mgr();
        let blk0 = BlockId::new("testfile", 0);
        let blk1 = BlockId::new("testfile", 1);

        for _ in 0..2 {
            if fm.length("testfile").unwrap() == 0 {
                initialize(&db, fm.clone(), &blk0, &blk1);
                modify(&db, bm.clone(), &blk0, &blk1);
            } else {
                recover(&db, fm.clone(), &blk0, &blk1);
            }
        }
        fs::remove_dir_all("recoverytest").unwrap();
    }

    fn initialize(db: &SimpleDB, fm: Arc<FileMgr>, blk0: &BlockId, blk1: &BlockId) {
        let mut tx1 = db.new_tx().unwrap();
        let mut tx2 = db.new_tx().unwrap();
        tx1.pin(blk0).unwrap();
        tx2.pin(blk1).unwrap();
        let bytes = 4;
        let mut pos = 0;
        for _ in 0..6 {
            tx1.set_int(blk0, pos, pos as i32, false).unwrap();
            tx2.set_int(blk1, pos, pos as i32, false).unwrap();
            pos += bytes;
        }
        tx1.set_string(blk0, 30, "abc", false).unwrap();
        tx2.set_string(blk1, 30, "def", false).unwrap();
        tx1.commit().unwrap();
        tx2.commit().unwrap();
        assert_values(
            fm,
            blk0,
            blk1,
            [[0, 0], [4, 4], [8, 8], [12, 12], [16, 16], [20, 20]],
            ["abc", "def"],
        );
    }

    fn modify(db: &SimpleDB, bm: Arc<Mutex<BufferMgr>>, blk0: &BlockId, blk1: &BlockId) {
        let mut tx3 = db.new_tx().unwrap();
        let mut tx4 = db.new_tx().unwrap();
        tx3.pin(blk0).unwrap();
        tx4.pin(blk1).unwrap();
        let mut pos = 0;
        let bytes = 4;
        for _ in 0..6 {
            tx3.set_int(blk0, pos, pos as i32 + 100, true).unwrap();
            tx4.set_int(blk1, pos, pos as i32 + 100, true).unwrap();
            pos += bytes;
        }
        tx3.set_string(blk0, 30, "uvw", true).unwrap();
        tx4.set_string(blk1, 30, "xyz", true).unwrap();
        bm.lock().unwrap().flush_all(3).unwrap();
        bm.lock().unwrap().flush_all(4).unwrap();

        tx3.rollback().unwrap();
    }

    fn recover(db: &SimpleDB, fm: Arc<FileMgr>, blk0: &BlockId, blk1: &BlockId) {
        let mut tx = db.new_tx().unwrap();
        tx.recover().unwrap();
        assert_values(
            fm,
            blk0,
            blk1,
            [[0, 0], [4, 4], [8, 8], [12, 12], [16, 16], [20, 20]],
            ["abc", "def"],
        );
    }

    fn assert_values(
        fm: Arc<FileMgr>,
        blk0: &BlockId,
        blk1: &BlockId,
        e1: [[i32; 2]; 6],
        e2: [&str; 2],
    ) {
        let mut p0 = Page::new(fm.block_size());
        let mut p1 = Page::new(fm.block_size());
        fm.read(blk0, &mut p0).unwrap();
        fm.read(blk1, &mut p1).unwrap();
        let mut pos = 0;
        let bytes = 4;
        for i in 0..6 {
            assert_eq!(p0.get_int(pos), e1[i][0]);
            assert_eq!(p1.get_int(pos), e1[i][1]);
            pos += bytes;
        }
        assert_eq!(p0.get_string(30).unwrap(), e2[0]);
        assert_eq!(p1.get_string(30).unwrap(), e2[1]);
    }
}
