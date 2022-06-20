#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        file::{blockid::BlockId, page::Page},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn bufferfiletest() {
        let mut db = SimpleDB::with_params("bufferfiletest", 400, 8).unwrap();
        let m = db.buffer_mgr();
        let mut bm = m.lock().unwrap();
        let blk = BlockId::new("testfile", 2);
        let pos1 = 88;

        let idx1 = bm.pin(&blk).unwrap();
        let b1 = bm.buffer(idx1);
        let p1 = b1.contents();
        p1.set_string(pos1, "abcdefghijklm");
        let size = Page::max_length("abcdefghijklm".len());
        let pos2 = pos1 + size;
        p1.set_int(pos2, 345);
        b1.set_modified(1, Some(0));
        bm.unpin(idx1);

        let idx2 = bm.pin(&blk).unwrap();
        let b2 = bm.buffer(idx2);
        let p2 = b2.contents();
        assert_eq!(pos2, 105);
        assert_eq!(p2.get_int(pos2), 345);
        assert_eq!(pos1, 88);
        assert_eq!(p2.get_string(pos1).unwrap(), "abcdefghijklm");
        bm.unpin(idx2);

        fs::remove_dir_all("bufferfiletest").unwrap();
    }
}
