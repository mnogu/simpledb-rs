use std::{collections::HashMap, sync::Arc};

use crate::file::page::Page;

use super::schema::Schema;

pub struct Layout {
    schema: Arc<Schema>,
    offsets: HashMap<String, usize>,
    slotsize: usize,
}

impl Layout {
    pub fn new(schema: Arc<Schema>) -> Layout {
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
            l.slotsize += l.length_in_bytes(&fldname);
        }

        l
    }

    pub fn with_metadata(
        schema: Arc<Schema>,
        offsets: HashMap<String, usize>,
        slotsize: usize,
    ) -> Layout {
        Layout {
            schema,
            offsets,
            slotsize,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn offset(&self, fldname: &str) -> usize {
        self.offsets[fldname]
    }

    pub fn slot_size(&self) -> usize {
        self.slotsize
    }

    fn length_in_bytes(&self, fldname: &str) -> usize {
        let fldtype = self.schema.type_(fldname);
        let bytes = 4;
        match fldtype {
            super::schema::Type::Integer => bytes,
            super::schema::Type::Varchar => Page::max_length(self.schema.length(fldname)),
        }
    }
}
