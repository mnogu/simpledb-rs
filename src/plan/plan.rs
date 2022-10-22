use std::sync::Arc;

use enum_dispatch::enum_dispatch;

use crate::{
    index::planner::{indexjoinplan::IndexJoinPlan, indexselectplan::IndexSelectPlan},
    materialize::{
        groupbyplan::GroupByPlan, materializeplan::MaterializePlan, mergejoinplan::MergeJoinPlan,
        sortplan::SortPlan,
    },
    multibuffer::multibufferproductplan::MultibufferProductPlan,
    parse::badsyntaxerror::BadSyntaxError,
    query::scan::Scan,
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::{
    optimizedproductplan::OptimizedProductPlan, productplan::ProductPlan, projectplan::ProjectPlan,
    selectplan::SelectPlan, tableplan::TablePlan,
};

#[derive(Debug)]
pub enum PlanError {
    Transaction(TransactionError),
    Syntax(BadSyntaxError),
    General,
}

impl From<TransactionError> for PlanError {
    fn from(e: TransactionError) -> Self {
        PlanError::Transaction(e)
    }
}

impl From<BadSyntaxError> for PlanError {
    fn from(e: BadSyntaxError) -> Self {
        PlanError::Syntax(e)
    }
}

#[enum_dispatch(Plan)]
pub trait PlanControl {
    fn open(&self) -> Result<Scan, TransactionError>;
    fn blocks_accessed(&self) -> usize;
    fn records_output(&self) -> usize;
    fn distinct_values(&self, fldname: &str) -> usize;
    fn schema(&self) -> Arc<Schema>;
}

#[derive(Clone)]
#[enum_dispatch]
pub enum Plan {
    Table(TablePlan),
    Select(SelectPlan),
    Project(ProjectPlan),
    Product(ProductPlan),
    IndexSelect(IndexSelectPlan),
    IndexJoin(IndexJoinPlan),
    Materialize(MaterializePlan),
    MultibufferProduct(MultibufferProductPlan),
    Sort(SortPlan),
    GroupBy(GroupByPlan),
    MergeJoin(MergeJoinPlan),
    OptimizedProduct(OptimizedProductPlan),
}
