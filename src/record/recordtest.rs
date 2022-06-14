#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        sync::{Arc, Mutex},
    };

    use rand::{distributions::Uniform, prelude::Distribution};

    use crate::{
        record::{layout::Layout, recordpage::RecordPage, schema::Schema},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn recordtest() {
        let db = SimpleDB::new("recordtest", 400, 8).unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));

        let mut sch = Schema::new();
        sch.add_int_field("A");
        sch.add_string_field("B", 9);
        let layout = Arc::new(Layout::new(Arc::new(sch)));

        let mut m = HashMap::new();
        m.insert("A".to_string(), 4);
        m.insert("B".to_string(), 8);

        for fldname in layout.schema().fields() {
            let offset = layout.offset(fldname);
            assert_eq!(m[fldname], offset);
        }
        let blk = tx.lock().unwrap().append("testfile").unwrap();
        tx.lock().unwrap().pin(&blk).unwrap();
        let mut rp = RecordPage::new(tx.clone(), blk.clone(), layout).unwrap();
        rp.format().unwrap();

        let mut slot = rp.insert_after(None).unwrap();
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..50);
        while let Some(s) = slot {
            let n = die.sample(&mut rng);
            rp.set_int(s, "A", n).unwrap();
            rp.set_string(s, "B", &format!("rec{}", n)).unwrap();
            assert!(s <= 18);
            assert!(n >= 0 && n < 50);
            slot = rp.insert_after(slot).unwrap();
        }

        let mut count = 0;
        slot = rp.next_after(None).unwrap();
        while let Some(s) = slot {
            let a = rp.get_int(s, "A").unwrap();
            let b = rp.get_string(s, "B").unwrap();
            if a < 25 {
                count += 1;
                assert!(s <= 18);
                assert!(a < 25);
                assert_eq!(format!("rec{}", a), b);
                rp.delete(s).unwrap();
            }
            slot = rp.next_after(slot).unwrap();
        }
        assert!(count >= 0 && count <= 18);

        slot = rp.next_after(None).unwrap();
        while let Some(s) = slot {
            let a = rp.get_int(s, "A").unwrap();
            let b = rp.get_string(s, "B").unwrap();
            assert!(s <= 18);
            assert!(a >= 25);
            assert_eq!(format!("rec{}", a), b);
            slot = rp.next_after(slot).unwrap();
        }
        tx.lock().unwrap().unpin(&blk).unwrap();
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("recordtest").unwrap();
    }
}
