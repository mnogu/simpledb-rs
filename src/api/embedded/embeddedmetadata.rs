use std::{cmp, sync::Arc};

use crate::{
    api::{driver::SQLError, metadata::MetaDataControl},
    record::schema::{Schema, Type},
};

pub struct EmbeddedMetaData {
    sch: Arc<Schema>,
}

impl EmbeddedMetaData {
    pub fn new(sch: Arc<Schema>) -> EmbeddedMetaData {
        EmbeddedMetaData { sch }
    }
}

impl MetaDataControl for EmbeddedMetaData {
    fn get_column_count(&mut self) -> Result<usize, SQLError> {
        Ok(self.sch.fields().len())
    }

    fn get_column_name(&mut self, column: usize) -> Result<String, SQLError> {
        if let Some(name) = self.sch.fields().get(column - 1) {
            return Ok(name.to_string());
        }
        Err(SQLError::General)
    }

    fn get_column_type(&mut self, column: usize) -> Result<Type, SQLError> {
        let fldname = self.get_column_name(column)?;
        Ok(self.sch.type_(&fldname))
    }

    fn get_column_display_size(&mut self, column: usize) -> Result<usize, SQLError> {
        let fldname = self.get_column_name(column)?;
        let fldtype = self.sch.type_(&fldname);
        let fldlength = match fldtype {
            Type::Integer => 6,
            Type::Varchar => self.sch.length(&fldname),
        };
        Ok(cmp::max(fldname.len(), fldlength) + 1)
    }
}
