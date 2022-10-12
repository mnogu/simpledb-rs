pub struct BufferNeeds {}

impl BufferNeeds {
    pub fn best_factor(available: usize, size: usize) -> usize {
        let avail = available - 2;
        if avail <= 1 {
            return 1;
        }
        let mut k = size;
        let mut i = 1.0;
        while k > avail {
            i += 1.0;
            k = (size as f64 / i).ceil() as usize;
        }
        k
    }
}
