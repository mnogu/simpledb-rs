use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    index::planner::{indexjoinplan::IndexJoinPlan, indexselectplan::IndexSelectPlan},
    metadata::{indexinfo::IndexInfo, metadatamgr::MetadataMgr},
    multibuffer::multibufferproductplan::MultibufferProductPlan,
    plan::{
        plan::{Plan, PlanControl},
        selectplan::SelectPlan,
        tableplan::TablePlan,
    },
    query::predicate::Predicate,
    record::schema::Schema,
    tx::transaction::{Transaction, TransactionError},
};

pub struct TablePlanner {
    myplan: TablePlan,
    mypred: Arc<Predicate>,
    myschema: Arc<Schema>,
    indexes: HashMap<String, IndexInfo>,
    tx: Arc<Mutex<Transaction>>,
}

impl TablePlanner {
    pub fn new(
        tblname: &str,
        mypred: Arc<Predicate>,
        tx: Arc<Mutex<Transaction>>,
        mdm: Arc<Mutex<MetadataMgr>>,
    ) -> Result<TablePlanner, TransactionError> {
        let myplan = TablePlan::new(tx.clone(), tblname, mdm.clone())?;
        let myschema = myplan.schema();
        let indexes = mdm.lock().unwrap().get_index_info(tblname, tx.clone())?;
        Ok(TablePlanner {
            myplan,
            mypred,
            myschema,
            indexes,
            tx,
        })
    }

    pub fn make_select_plan(&self) -> Plan {
        let p = if let Some(p) = self.make_index_select() {
            p
        } else {
            self.myplan.clone().into()
        };
        self.add_select_pred(p)
    }

    pub fn make_join_plan(&self, current: &Plan) -> Option<Plan> {
        let currsch = current.schema();
        let joinpred = self
            .mypred
            .join_sub_pred(self.myschema.clone(), currsch.clone());

        joinpred.as_ref()?;
        let p = self.make_index_join(current, currsch.clone());
        if let Some(p) = p {
            Some(p)
        } else {
            Some(self.make_product_join(current, currsch))
        }
    }

    pub fn make_product_plan(&self, current: &Plan) -> Plan {
        let p = self.add_select_pred(self.myplan.clone().into());
        MultibufferProductPlan::new(self.tx.clone(), current.clone(), p).into()
    }

    fn make_index_select(&self) -> Option<Plan> {
        for fldname in self.indexes.keys() {
            let val = self.mypred.equates_with_constant(fldname);
            if let Some(val) = val {
                let ii = self.indexes.get(fldname);
                println!("index on {} used", fldname);
                if let Some(ii) = ii {
                    return Some(
                        IndexSelectPlan::new(self.myplan.clone().into(), ii.clone(), val).into(),
                    );
                }
            }
        }
        None
    }

    fn make_index_join(&self, current: &Plan, currsch: Arc<Schema>) -> Option<Plan> {
        for fldname in self.indexes.keys() {
            let outerfield = self.mypred.equates_with_field(fldname);
            if let Some(outerfield) = outerfield {
                if currsch.has_field(&outerfield) {
                    let ii = self.indexes.get(fldname);
                    if let Some(ii) = ii {
                        let mut p: Plan = IndexJoinPlan::new(
                            current.clone(),
                            self.myplan.clone().into(),
                            ii.clone(),
                            &outerfield,
                        )
                        .into();
                        p = self.add_select_pred(p);
                        return Some(self.add_join_pred(p, currsch));
                    }
                }
            }
        }
        None
    }

    fn make_product_join(&self, current: &Plan, currsch: Arc<Schema>) -> Plan {
        let p = self.make_product_plan(current);
        self.add_join_pred(p, currsch)
    }

    fn add_select_pred(&self, p: Plan) -> Plan {
        let selectpred = self.mypred.select_sub_pred(self.myschema.clone());
        if let Some(selectpred) = selectpred {
            return SelectPlan::new(p, Arc::new(selectpred)).into();
        }
        p
    }

    fn add_join_pred(&self, p: Plan, currsch: Arc<Schema>) -> Plan {
        let joinpred = self.mypred.join_sub_pred(currsch, self.myschema.clone());
        if let Some(joinpred) = joinpred {
            return SelectPlan::new(p, Arc::new(joinpred)).into();
        }
        p
    }
}
