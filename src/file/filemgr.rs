use std::fs::{File, OpenOptions};
use std::io::{Read, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;
use std::{io::Error, io::Seek};

use super::blockid::BlockId;
use super::page::Page;

pub struct FileMgr {
    db_directory: String,
    blocksize: usize,
}

impl FileMgr {
    pub fn new(db_directory: &str, blocksize: usize) -> FileMgr {
        if !Path::new(db_directory).exists() {
            std::fs::create_dir(db_directory).unwrap();
        }
        FileMgr {
            db_directory: db_directory.to_string(),
            blocksize,
        }
    }

    pub fn read(&self, blk: &BlockId, p: &mut Page) -> Result<(), Error> {
        let file = self.get_file(blk.file_name())?;
        let m = Mutex::new(file);
        let mut f = m.lock().unwrap();

        f.seek(SeekFrom::Start((blk.number() * self.blocksize) as u64))?;
        f.read_exact(&mut p.contents())?;

        Ok(())
    }

    pub fn write(&self, blk: &BlockId, p: &mut Page) -> Result<(), Error> {
        let file = self.get_file(blk.file_name())?;
        let m = Mutex::new(file);
        let mut f = m.lock().unwrap();

        f.seek(SeekFrom::Start((blk.number() * self.blocksize) as u64))?;
        f.write_all(p.contents())?;
        f.sync_all()?;

        Ok(())
    }

    pub fn block_size(&self) -> usize {
        self.blocksize
    }

    fn get_file(&self, filename: &str) -> Result<File, Error> {
        let filename = Path::new(&self.db_directory).join(filename);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&filename)?;

        Ok(file)
    }
}
