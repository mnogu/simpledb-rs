#[derive(Clone)]
pub struct BlockId {
    filename: String,
    blknum: i32,
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
