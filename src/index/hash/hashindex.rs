pub struct HashIndex {}

impl HashIndex {
    const NUM_BUCKETS: usize = 100;

    pub fn search_cost(numblocks: usize, _rpb: usize) -> usize {
        numblocks / HashIndex::NUM_BUCKETS
    }
}
