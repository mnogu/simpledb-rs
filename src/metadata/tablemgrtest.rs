#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use crate::{
        metadata::tablemgr::TableMgr,
        record::schema::{Schema, Type},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn tablemgrtest() {
        let db = SimpleDB::new("tblmgrtest", 400, 8).unwrap();
        let tx = Arc::new(Mutex::new(db.new_tx().unwrap()));
        let tm = TableMgr::new(true, tx.clone()).unwrap();

        let mut sch = Schema::new();
        sch.add_int_field("A");
        sch.add_string_field("B", 9);
        tm.create_table("MyTable", Arc::new(sch), tx.clone())
            .unwrap();

        let layout = tm.get_layout("MyTable", tx.clone()).unwrap();
        let size = layout.slot_size();
        let sch2 = layout.schema();
        assert_eq!(size, 21);
        for (i, fldname) in (0..).zip(sch2.fields()) {
            match sch2.type_(&fldname) {
                Type::Integer => assert_eq!(i, 0),
                Type::Varchar => {
                    assert_eq!(i, 1);
                    assert_eq!(sch2.length(&fldname), 9);
                }
            };
        }
        tx.lock().unwrap().commit().unwrap();

        fs::remove_dir_all("tblmgrtest").unwrap();
    }
}
