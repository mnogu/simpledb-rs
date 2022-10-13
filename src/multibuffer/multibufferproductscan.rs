use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    query::{
        constant::Constant,
        productscan::ProductScan,
        scan::{Scan, ScanControl},
    },
    record::layout::Layout,
    tx::transaction::{Transaction, TransactionError},
};

use super::{bufferneeds::BufferNeeds, chunkscan::ChunkScan};

pub struct MultibufferProductScan {
    tx: Arc<Mutex<Transaction>>,
    lhsscan: Arc<Mutex<Scan>>,
    rhsscan: Option<Arc<Mutex<Scan>>>,
    prodscan: Option<Box<Scan>>,
    filename: String,
    layout: Arc<Layout>,
    chunksize: usize,
    nextblknum: usize,
    filesize: usize,
}

impl MultibufferProductScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        lhsscan: Scan,
        tblname: &str,
        layout: Arc<Layout>,
    ) -> Result<MultibufferProductScan, TransactionError> {
        let filename = format!("{}.tbl", tblname);
        let filesize = tx.lock().unwrap().size(&filename)?;
        let available = tx.lock().unwrap().available_buffs();
        let chunksize = BufferNeeds::best_factor(available, filesize);
        let mut s = MultibufferProductScan {
            tx,
            lhsscan: Arc::new(Mutex::new(lhsscan)),
            rhsscan: None,
            prodscan: None,
            filename,
            layout,
            chunksize,
            nextblknum: 0,
            filesize,
        };
        s.before_first()?;
        Ok(s)
    }

    fn use_next_chunk(&mut self) -> Result<bool, TransactionError> {
        if self.nextblknum >= self.filesize {
            return Ok(false);
        }
        if let Some(rhsscan) = &mut self.rhsscan {
            rhsscan.lock().unwrap().close()?;
        }
        let mut end = self.nextblknum + self.chunksize - 1;
        if end >= self.filesize {
            end = self.filesize - 1;
        }
        self.rhsscan = Some(Arc::new(Mutex::new(
            ChunkScan::new(
                self.tx.clone(),
                &self.filename,
                self.layout.clone(),
                self.nextblknum,
                end,
            )?
            .into(),
        )));
        self.lhsscan.lock().unwrap().before_first()?;
        if let Some(rhsscan) = &self.rhsscan {
            self.prodscan = Some(Box::new(
                ProductScan::new(self.lhsscan.clone(), rhsscan.clone())?.into(),
            ));
        }
        self.nextblknum = end + 1;
        Ok(true)
    }
}

impl ScanControl for MultibufferProductScan {
    fn before_first(&mut self) -> Result<(), TransactionError> {
        self.nextblknum = 0;
        self.use_next_chunk()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        while let Some(prodscan) = &mut self.prodscan {
            if prodscan.next()? {
                return Ok(true);
            }
            if !self.use_next_chunk()? {
                return Ok(false);
            }
        }
        Err(TransactionError::General)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        if let Some(prodscan) = &mut self.prodscan {
            prodscan.close()?;
            return Ok(());
        }
        Err(AbortError::General)
    }

    fn get_val(&mut self, fldname: &str) -> Result<Constant, TransactionError> {
        if let Some(prodscan) = &mut self.prodscan {
            return prodscan.get_val(fldname);
        }
        Err(TransactionError::General)
    }

    fn get_int(&mut self, fldname: &str) -> Result<i32, TransactionError> {
        if let Some(prodscan) = &mut self.prodscan {
            return prodscan.get_int(fldname);
        }
        Err(TransactionError::General)
    }

    fn get_string(&mut self, fldname: &str) -> Result<String, TransactionError> {
        if let Some(prodscan) = &mut self.prodscan {
            return prodscan.get_string(fldname);
        }
        Err(TransactionError::General)
    }

    fn has_field(&self, fldname: &str) -> bool {
        if let Some(prodscan) = &self.prodscan {
            return prodscan.has_field(fldname);
        }
        false
    }
}
