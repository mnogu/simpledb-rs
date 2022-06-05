use std::string::FromUtf8Error;

pub struct Page {
    bb: Vec<u8>,
}

impl Page {
    pub fn new(blocksize: usize) -> Page {
        let mut vec: Vec<u8> = Vec::new();
        vec.resize(blocksize, 0);
        Page { bb: vec }
    }

    pub fn with_vec(b: Vec<u8>) -> Page {
        Page { bb: b }
    }

    pub fn get_int(&self, offset: usize) -> i32 {
        let mut buf: [u8; 4] = [0; 4];
        buf.copy_from_slice(&self.bb[offset..offset + 4]);
        i32::from_be_bytes(buf)
    }

    pub fn set_int(&mut self, offset: usize, n: i32) {
        self.bb.as_mut_slice()[offset..offset + 4].copy_from_slice(&n.to_be_bytes());
    }

    pub fn get_bytes(&self, offset: usize) -> &[u8] {
        let len = self.get_int(offset) as usize;
        &self.bb[offset + 4..offset + 4 + len]
    }

    pub fn set_bytes(&mut self, offset: usize, b: &[u8]) {
        self.bb.as_mut_slice()[offset..offset + 4].copy_from_slice(&(b.len() as i32).to_be_bytes());
        self.bb.as_mut_slice()[offset + 4..offset + 4 + b.len()].copy_from_slice(b);
    }

    pub fn get_string(&self, offset: usize) -> Result<String, FromUtf8Error> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(self.get_bytes(offset));
        String::from_utf8(buf)
    }

    pub fn set_string(&mut self, offset: usize, s: &str) {
        self.set_bytes(offset, s.as_bytes());
    }

    pub fn max_length(strlen: usize) -> usize {
        let bytes_per_char = 1;
        4 + strlen * bytes_per_char
    }

    pub(in crate) fn contents(&mut self) -> &mut Vec<u8> {
        &mut self.bb
    }
}
