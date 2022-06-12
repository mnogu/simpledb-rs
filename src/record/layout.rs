use std::collections::HashMap;

use crate::file::page::Page;

use super::schema::Schema;

pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, usize>,
    slotsize: usize,
}

fn length_in_bytes(l: &Layout, fldname: &str) -> usize {
    let fldtype = l.schema.type_(fldname);
    let bytes = 4;
    match fldtype {
        super::schema::Type::Integer => bytes,
        super::schema::Type::Varchar => Page::max_length(l.schema.length(fldname)),
    }
}

impl Layout {
    pub fn new(schema: Schema) -> Layout {
        let offsets = HashMap::new();
        let bytes = 4;
        let pos = bytes;

        let mut l = Layout {
            schema,
            offsets,
            slotsize: pos,
        };

        for fldname in l.schema.fields() {
            l.offsets.insert(fldname.to_string(), l.slotsize);
            l.slotsize += length_in_bytes(&l, &fldname);
        }

        l
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn offset(&self, fldname: &str) -> usize {
        self.offsets[fldname]
    }

    pub fn slot_size(&self) -> usize {
        self.slotsize
    }
}
