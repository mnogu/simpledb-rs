#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::record::{layout::Layout, schema::Schema};

    #[test]
    fn layouttest() {
        let mut sch = Schema::new();
        sch.add_int_field("A");
        sch.add_string_field("B", 9);
        let layout = Layout::new(Arc::new(sch));

        let e = [("A", 4), ("B", 8)];
        for (i, fldname) in layout.schema().fields().iter().enumerate() {
            assert_eq!(fldname, e[i].0);

            let offset = layout.offset(fldname);
            assert_eq!(offset, e[i].1);
        }
    }
}
