pub struct BlockId {
    filename: String,
    blknum: usize,
}

impl BlockId {
    pub fn new(filename: &str, blknum: usize) -> BlockId {
        BlockId {
            filename: filename.to_string(),
            blknum,
        }
    }

    pub fn file_name(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> usize {
        self.blknum
    }
}
