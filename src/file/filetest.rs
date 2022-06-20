#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        file::{blockid::BlockId, page::Page},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn filetest() {
        let db = SimpleDB::with_params("filetest", 400, 8).unwrap();
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

        assert_eq!(105, pos2);
        assert_eq!(345, p2.get_int(pos2));
        assert_eq!(88, pos1);
        assert_eq!("abcdefghijklm", p2.get_string(pos1).unwrap());

        fs::remove_dir_all("filetest").unwrap();
    }
}
