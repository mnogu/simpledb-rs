#[cfg(test)]
mod tests {
    use std::{cell::RefMut, fs, iter::zip};

    use crate::{file::page::Page, log::logmgr::LogMgr, server::simpledb::SimpleDB};

    #[test]
    fn logtest() {
        let mut db = SimpleDB::new("logtest", 400, 8).unwrap();
        let m = db.log_mgr();
        let mut lm = m.borrow_mut();

        assert_log_records(&mut lm, Vec::new());
        create_records(&mut lm, 1, 35);
        assert_log_records(&mut lm, (1..=35).rev().collect());
        create_records(&mut lm, 36, 70);
        lm.flush(65).unwrap();
        assert_log_records(&mut lm, (1..=70).rev().collect());

        fs::remove_dir_all("logtest").unwrap();
    }

    fn assert_log_records(lm: &mut RefMut<LogMgr>, expected: Vec<i32>) {
        let iter = lm.iterator().unwrap();
        for (rec, exp) in zip(iter, expected) {
            let p = Page::new_with_vec(rec);
            let s = p.get_string(0).unwrap();
            let npos = Page::max_length(s.len());
            let val = p.get_int(npos);

            assert_eq!(format!("record{}", exp), s);
            assert_eq!(exp + 100, val);
        }
    }

    fn create_records(lm: &mut RefMut<LogMgr>, start: usize, end: usize) {
        for i in start..=end {
            let s = format!("{}{}", "record", i);
            let rec = create_log_record(s.as_str(), i + 100);
            let lsn = lm.append(&rec).unwrap();
            assert_eq!(i, lsn);
        }
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
}
