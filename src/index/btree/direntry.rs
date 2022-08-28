use crate::query::contant::Constant;

pub struct DirEntry {
    dataval: Constant,
    blocknum: i32,
}

impl DirEntry {
    pub fn new(dataval: Constant, blocknum: i32) -> DirEntry {
        DirEntry { dataval, blocknum }
    }

    pub fn data_val(&self) -> Constant {
        self.dataval.clone()
    }

    pub fn block_number(&self) -> i32 {
        self.blocknum
    }
}
