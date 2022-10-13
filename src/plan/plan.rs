use std::sync::Arc;

use crate::{
    index::planner::{indexjoinplan::IndexJoinPlan, indexselectplan::IndexSelectPlan},
    materialize::materializeplan::MaterializePlan,
    multibuffer::multibufferproductplan::MultibufferProductPlan,
    parse::badsyntaxerror::BadSyntaxError,
    query::scan::Scan,
    record::schema::Schema,
    tx::transaction::TransactionError,
};

use super::{
    productplan::ProductPlan, projectplan::ProjectPlan, selectplan::SelectPlan,
    tableplan::TablePlan,
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

pub trait PlanControl {
    fn open(&self) -> Result<Scan, TransactionError>;
    fn blocks_accessed(&self) -> usize;
    fn records_output(&self) -> usize;
    fn distinct_values(&self, fldname: &str) -> usize;
    fn schema(&self) -> Arc<Schema>;
}

#[derive(Clone)]
pub enum Plan {
    Table(TablePlan),
    Select(SelectPlan),
    Project(ProjectPlan),
    Product(ProductPlan),
    IndexSelect(IndexSelectPlan),
    IndexJoin(IndexJoinPlan),
    Materialize(MaterializePlan),
    MultibufferProduct(MultibufferProductPlan),
}

impl PlanControl for Plan {
    fn open(&self) -> Result<Scan, TransactionError> {
        match self {
            Plan::Table(plan) => plan.open(),
            Plan::Select(plan) => plan.open(),
            Plan::Project(plan) => plan.open(),
            Plan::Product(plan) => plan.open(),
            Plan::IndexSelect(plan) => plan.open(),
            Plan::IndexJoin(plan) => plan.open(),
            Plan::Materialize(plan) => plan.open(),
            Plan::MultibufferProduct(plan) => plan.open(),
        }
    }

    fn blocks_accessed(&self) -> usize {
        match self {
            Plan::Table(plan) => plan.blocks_accessed(),
            Plan::Select(plan) => plan.blocks_accessed(),
            Plan::Project(plan) => plan.blocks_accessed(),
            Plan::Product(plan) => plan.blocks_accessed(),
            Plan::IndexSelect(plan) => plan.blocks_accessed(),
            Plan::IndexJoin(plan) => plan.blocks_accessed(),
            Plan::Materialize(plan) => plan.blocks_accessed(),
            Plan::MultibufferProduct(plan) => plan.blocks_accessed(),
        }
    }

    fn records_output(&self) -> usize {
        match self {
            Plan::Table(plan) => plan.records_output(),
            Plan::Select(plan) => plan.records_output(),
            Plan::Project(plan) => plan.records_output(),
            Plan::Product(plan) => plan.records_output(),
            Plan::IndexSelect(plan) => plan.records_output(),
            Plan::IndexJoin(plan) => plan.records_output(),
            Plan::Materialize(plan) => plan.records_output(),
            Plan::MultibufferProduct(plan) => plan.records_output(),
        }
    }

    fn distinct_values(&self, fldname: &str) -> usize {
        match self {
            Plan::Table(plan) => plan.distinct_values(fldname),
            Plan::Select(plan) => plan.distinct_values(fldname),
            Plan::Project(plan) => plan.distinct_values(fldname),
            Plan::Product(plan) => plan.distinct_values(fldname),
            Plan::IndexSelect(plan) => plan.distinct_values(fldname),
            Plan::IndexJoin(plan) => plan.distinct_values(fldname),
            Plan::Materialize(plan) => plan.distinct_values(fldname),
            Plan::MultibufferProduct(plan) => plan.distinct_values(fldname),
        }
    }

    fn schema(&self) -> Arc<Schema> {
        match self {
            Plan::Table(plan) => plan.schema(),
            Plan::Select(plan) => plan.schema(),
            Plan::Project(plan) => plan.schema(),
            Plan::Product(plan) => plan.schema(),
            Plan::IndexSelect(plan) => plan.schema(),
            Plan::IndexJoin(plan) => plan.schema(),
            Plan::Materialize(plan) => plan.schema(),
            Plan::MultibufferProduct(plan) => plan.schema(),
        }
    }
}

impl From<TablePlan> for Plan {
    fn from(p: TablePlan) -> Self {
        Plan::Table(p)
    }
}

impl From<SelectPlan> for Plan {
    fn from(p: SelectPlan) -> Self {
        Plan::Select(p)
    }
}

impl From<ProjectPlan> for Plan {
    fn from(p: ProjectPlan) -> Self {
        Plan::Project(p)
    }
}

impl From<ProductPlan> for Plan {
    fn from(p: ProductPlan) -> Self {
        Plan::Product(p)
    }
}

impl From<IndexSelectPlan> for Plan {
    fn from(p: IndexSelectPlan) -> Self {
        Plan::IndexSelect(p)
    }
}

impl From<IndexJoinPlan> for Plan {
    fn from(p: IndexJoinPlan) -> Self {
        Plan::IndexJoin(p)
    }
}

impl From<MaterializePlan> for Plan {
    fn from(p: MaterializePlan) -> Self {
        Plan::Materialize(p)
    }
}

impl From<MultibufferProductPlan> for Plan {
    fn from(p: MultibufferProductPlan) -> Self {
        Plan::MultibufferProduct(p)
    }
}
