#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs};

    use crate::{file::blockid::BlockId, server::simpledb::SimpleDB};

    #[test]
    fn buffermgrtest() {
        let mut db = SimpleDB::new("buffermgrtest", 400, 3).unwrap();
        let m = db.buffer_mgr();
        let mut bm = m.lock().unwrap();
        bm.set_max_time(1);

        let mut buff = Vec::with_capacity(6);
        buff.push(bm.pin(&BlockId::new("testfile", 0)).unwrap());
        buff.push(bm.pin(&BlockId::new("testfile", 1)).unwrap());
        buff.push(bm.pin(&BlockId::new("testfile", 2)).unwrap());
        bm.unpin(buff[1]);
        buff[1] = 10;
        buff.push(bm.pin(&BlockId::new("testfile", 0)).unwrap());
        buff.push(bm.pin(&BlockId::new("testfile", 1)).unwrap());
        assert_eq!(0, bm.available());

        assert!(bm.pin(&BlockId::new("testfile", 3)).is_err());

        bm.unpin(buff[2]);
        buff[2] = 10;
        buff.push(bm.pin(&BlockId::new("testfile", 3)).unwrap());

        let exp = HashMap::from([
            (0, BlockId::new("testfile", 0)),
            (3, BlockId::new("testfile", 0)),
            (4, BlockId::new("testfile", 1)),
            (5, BlockId::new("testfile", 3)),
        ]);
        for (i, idx) in buff.iter().enumerate() {
            if *idx != 10 {
                let b = bm.buffer(*idx);
                assert_eq!(exp.get(&i).unwrap(), b.block().as_ref().unwrap());
            } else {
                assert!(i == 1 || i == 2);
            }
        }

        fs::remove_dir_all("buffermgrtest").unwrap();
    }
}
