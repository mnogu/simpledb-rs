use std::fmt::{Display, Formatter};

#[derive(Clone, PartialEq)]
pub struct BlockId {
    filename: String,
    blknum: i32,
}

impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.blknum)
    }
}

impl BlockId {
    pub fn new(filename: &str, blknum: i32) -> BlockId {
        BlockId {
            filename: filename.to_string(),
            blknum,
        }
    }

    pub fn file_name(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> i32 {
        self.blknum
    }
}
