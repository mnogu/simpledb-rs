use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffermgr::AbortError,
    file::blockid::BlockId,
    index::index::IndexControl,
    query::constant::Constant,
    record::{
        layout::Layout,
        rid::Rid,
        schema::{self, Schema},
    },
    tx::transaction::{Transaction, TransactionError},
};

use super::{btpage::BTPage, btreedir::BTreeDir, btreeleaf::BTreeLeaf};

pub struct BTreeIndex {
    tx: Arc<Mutex<Transaction>>,
    dir_layout: Arc<Layout>,
    leaf_layout: Arc<Layout>,
    leaftbl: String,
    leaf: Option<BTreeLeaf>,
    rootblk: BlockId,
}

impl BTreeIndex {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        idxname: &str,
        leaf_layout: Arc<Layout>,
    ) -> Result<BTreeIndex, TransactionError> {
        let leaftbl = format!("{}leaf", idxname);
        if tx.lock().unwrap().size(&leaftbl)? == 0 {
            let blk = tx.lock().unwrap().append(&leaftbl)?;
            let node = BTPage::new(tx.clone(), blk.clone(), leaf_layout.clone())?;
            node.format(&blk, -1)?;
        }

        let mut dirsch = Schema::new();
        dirsch.add("block", &leaf_layout.schema());
        dirsch.add("dataval", &leaf_layout.schema());
        let dirsch = Arc::new(dirsch);
        let dirtbl = format!("{}dir", idxname);
        let dir_layout = Arc::new(Layout::new(dirsch.clone()));
        let rootblk = BlockId::new(&dirtbl, 0);
        if tx.lock().unwrap().size(&dirtbl)? == 0 {
            tx.lock().unwrap().append(&dirtbl)?;
            let mut node = BTPage::new(tx.clone(), rootblk.clone(), dir_layout.clone())?;
            node.format(&rootblk, 0)?;

            let fldtype = dirsch.type_("dataval");
            let minval = match fldtype {
                schema::Type::Integer => Constant::with_int(-2147483648),
                schema::Type::Varchar => Constant::with_string(""),
            };
            node.insert_dir(0, minval, 0)?;
            node.close()?;
        }
        Ok(BTreeIndex {
            tx,
            dir_layout,
            leaf_layout,
            leaftbl,
            leaf: None,
            rootblk,
        })
    }

    pub fn search_cost(numblocks: usize, rpb: usize) -> usize {
        1 + (((numblocks as f64).ln() / (rpb as f64).ln()) as usize)
    }
}

impl IndexControl for BTreeIndex {
    fn before_first(&mut self, searchkey: Constant) -> Result<(), TransactionError> {
        self.close()?;
        let mut root = BTreeDir::new(
            self.tx.clone(),
            self.rootblk.clone(),
            self.dir_layout.clone(),
        )?;
        let blknum = root.search(&searchkey)?;
        root.close()?;
        let leafblk = BlockId::new(&self.leaftbl, blknum);
        self.leaf = Some(BTreeLeaf::new(
            self.tx.clone(),
            leafblk,
            self.leaf_layout.clone(),
            searchkey,
        )?);
        Ok(())
    }

    fn next(&mut self) -> Result<bool, TransactionError> {
        if let Some(leaf) = &mut self.leaf {
            return leaf.next();
        }
        Err(TransactionError::General)
    }

    fn get_data_rid(&mut self) -> Result<Rid, TransactionError> {
        if let Some(leaf) = &self.leaf {
            return leaf.get_data_rid();
        }
        Err(TransactionError::General)
    }

    fn insert(&mut self, dataval: Constant, datarid: &Rid) -> Result<(), TransactionError> {
        self.before_first(dataval)?;
        if let Some(leaf) = &mut self.leaf {
            let e = leaf.insert(datarid.clone())?;
            leaf.close()?;
            if let Some(e) = &e {
                let mut root = BTreeDir::new(
                    self.tx.clone(),
                    self.rootblk.clone(),
                    self.dir_layout.clone(),
                )?;
                let e2 = root.insert(e)?;
                if let Some(e2) = &e2 {
                    root.make_new_root(e2)?;
                }
                root.close()?;
            }
            return Ok(());
        }
        Err(TransactionError::General)
    }

    fn delete(&mut self, dataval: Constant, datarid: &Rid) -> Result<(), TransactionError> {
        self.before_first(dataval)?;
        if let Some(leaf) = &mut self.leaf {
            leaf.delete(datarid.clone())?;
            leaf.close()?;
            return Ok(());
        }
        Err(TransactionError::General)
    }

    fn close(&mut self) -> Result<(), AbortError> {
        if let Some(leaf) = &mut self.leaf {
            leaf.close()?;
        }
        Ok(())
    }
}
