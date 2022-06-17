#[derive(Clone, Copy)]
pub struct StatInfo {
    num_blocks: usize,
    num_recs: usize,
}

impl StatInfo {
    pub fn new(num_blocks: usize, num_recs: usize) -> StatInfo {
        StatInfo {
            num_blocks,
            num_recs,
        }
    }

    pub fn blocks_accessed(&self) -> usize {
        self.num_blocks
    }

    pub fn records_output(&self) -> usize {
        self.num_recs
    }

    pub fn distinct_values(&self, _fldname: &str) -> usize {
        1 + (self.num_recs / 3)
    }
}
