use std::fs::{self, File, OpenOptions};
use std::io::{Read, SeekFrom, Write};
use std::path::Path;
use std::{io::Error, io::Seek};

use super::blockid::BlockId;
use super::page::Page;

pub struct FileMgr {
    db_directory: String,
    blocksize: usize,
    is_new: bool,
}

impl FileMgr {
    pub fn new(db_directory: &str, blocksize: usize) -> Result<FileMgr, Error> {
        let is_new = !Path::new(db_directory).exists();
        let fm = FileMgr {
            db_directory: db_directory.to_string(),
            blocksize,
            is_new,
        };
        if fm.is_new {
            fs::create_dir_all(db_directory)?;
        }

        for entry in fs::read_dir(db_directory)? {
            let path = entry?.path();
            if path.starts_with("temp") {
                fs::remove_file(path)?;
            }
        }
        Ok(fm)
    }

    pub fn read(&self, blk: &BlockId, p: &mut Page) -> Result<(), Error> {
        let mut f = self.get_file(blk.file_name())?;
        let pos = (blk.number() as usize * self.blocksize) as u64;
        f.seek(SeekFrom::Start(pos))?;
        if f.metadata()?.len() >= pos + p.contents().len() as u64 {
            f.read_exact(&mut p.contents())?;
        }
        Ok(())
    }

    pub fn write(&self, blk: &BlockId, p: &mut Page) -> Result<(), Error> {
        let mut f = self.get_file(blk.file_name())?;
        f.seek(SeekFrom::Start(
            (blk.number() as usize * self.blocksize) as u64,
        ))?;
        f.write_all(p.contents())?;
        f.sync_all()?;

        Ok(())
    }

    pub fn append(&self, filename: &str) -> Result<BlockId, Error> {
        let newblknum = self.length(filename)?;
        let blk = BlockId::new(filename, newblknum as i32);
        let b: Vec<u8> = vec![0; self.blocksize];

        let mut f = self.get_file(blk.file_name())?;
        f.seek(SeekFrom::Start(
            (blk.number() as usize * self.blocksize) as u64,
        ))?;
        f.write(&b)?;

        Ok(blk)
    }

    pub fn length(&self, filename: &str) -> Result<usize, Error> {
        let file = self.get_file(filename)?;
        let metadata = file.metadata()?;
        Ok(metadata.len() as usize / self.blocksize)
    }

    pub fn is_new(&self) -> bool {
        self.is_new
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
            .open(filename)?;

        Ok(file)
    }
}
