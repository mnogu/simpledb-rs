#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use rand::{distributions::Uniform, prelude::Distribution};

    use crate::{
        metadata::metadatamgr::MetadataMgr,
        query::updatescan::UpdateScanControl,
        record::{
            schema::{Schema, Type},
            tablescan::TableScan,
        },
        server::simpledb::SimpleDB,
    };

    #[test]
    fn metadatamgrtest() {
        let db = SimpleDB::with_params("metadatamgrtest", 400, 8).unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
        let mut mdm = MetadataMgr::new(true, tx.clone()).unwrap();

        let mut sch = Schema::new();
        sch.add_int_field("A");
        sch.add_string_field("B", 9);

        mdm.create_table("MyTable", Arc::new(sch), tx.clone())
            .unwrap();
        let layout = mdm.get_layout("MyTable", tx.clone()).unwrap();
        let size = layout.slot_size();
        let sch2 = layout.schema();
        assert_eq!(size, 21);
        for (i, fldname) in sch2.fields().iter().enumerate() {
            match sch2.type_(fldname) {
                Type::Integer => assert_eq!(i, 0),
                Type::Varchar => {
                    assert_eq!(i, 1);
                    assert_eq!(sch2.length(&fldname), 9);
                }
            };
        }

        let mut ts = TableScan::new(tx.clone(), "MyTable", layout.clone()).unwrap();
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..50);
        for _ in 0..50 {
            ts.insert().unwrap();
            let n = die.sample(&mut rng);
            ts.set_int("A", n).unwrap();
            ts.set_string("B", &format!("rec{}", n)).unwrap();
        }
        let si = mdm.get_stat_info("MyTable", layout, tx.clone()).unwrap();
        assert_eq!(si.blocks_accessed(), 3);
        assert_eq!(si.records_output(), 50);
        assert_eq!(si.distinct_values("A"), 17);
        assert_eq!(si.distinct_values("B"), 17);

        let viewdef = "select B from MyTable where A = 1";
        mdm.create_view("viewA", viewdef, tx.clone()).unwrap();
        let v = mdm.get_view_def("viewA", tx.clone()).unwrap();
        assert_eq!(v, Some("select B from MyTable where A = 1".to_string()));

        mdm.create_index("indexA", "MyTable", "A", tx.clone())
            .unwrap();
        mdm.create_index("indexB", "MyTable", "B", tx.clone())
            .unwrap();
        let idxmap = mdm.get_index_info("MyTable", tx.clone()).unwrap();

        let mut ii = idxmap.get("A").unwrap();
        assert_eq!(ii.blocks_accessed(), 1);
        assert_eq!(ii.records_output(), 2);
        assert_eq!(ii.distinct_values("A"), 1);
        assert_eq!(ii.distinct_values("B"), 17);

        ii = idxmap.get("B").unwrap();
        assert_eq!(ii.blocks_accessed(), 1);
        assert_eq!(ii.records_output(), 2);
        assert_eq!(ii.distinct_values("A"), 17);
        assert_eq!(ii.distinct_values("B"), 1);
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("metadatamgrtest").unwrap();
    }
}
