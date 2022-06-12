use std::collections::HashMap;

#[derive(Clone, Copy)]
pub enum Type {
    Integer = 4,
    Varchar = 12,
}

pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema {
            fields: Vec::new(),
            info: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, fldname: &str, type_: Type, length: usize) {
        self.fields.push(fldname.to_string());
        self.info
            .insert(fldname.to_string(), FieldInfo { type_, length });
    }

    pub fn add_int_field(&mut self, fldname: &str) {
        self.add_field(fldname, Type::Integer, 0)
    }

    pub fn add_string_field(&mut self, fldname: &str, length: usize) {
        self.add_field(fldname, Type::Varchar, length)
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }

    pub fn type_(&self, fldname: &str) -> Type {
        self.info[fldname].type_
    }

    pub fn length(&self, fldname: &str) -> usize {
        self.info[fldname].length
    }
}

struct FieldInfo {
    type_: Type,
    length: usize,
}

impl FieldInfo {
    fn new(type_: Type, length: usize) -> FieldInfo {
        FieldInfo { type_, length }
    }

    fn type_(&self) -> Type {
        self.type_
    }

    fn length(&self) -> usize {
        self.length
    }
}
