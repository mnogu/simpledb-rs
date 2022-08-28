use std::fmt;

#[derive(PartialEq, Clone)]
pub struct Rid {
    blknum: i32,
    slot: usize,
}

impl fmt::Display for Rid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {}]", self.blknum, self.slot)
    }
}

impl Rid {
    pub fn new(blknum: i32, slot: usize) -> Rid {
        Rid { blknum, slot }
    }

    pub fn block_number(&self) -> i32 {
        self.blknum
    }

    pub fn slot(&self) -> usize {
        self.slot
    }
}
