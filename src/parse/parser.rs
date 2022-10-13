use crate::{
    query::{constant::Constant, expression::Expression, predicate::Predicate, term::Term},
    record::schema::Schema,
};

use super::{
    badsyntaxerror::BadSyntaxError, createindexdata::CreateIndexData,
    createtabledata::CreateTableData, createviewdata::CreateViewData, deletedata::DeleteData,
    insertdata::InsertData, lexer::Lexer, modifydata::ModifyData, querydata::QueryData,
};

pub trait ObjectControl {}

pub enum Object {
    Insert(InsertData),
    Delete(DeleteData),
    Modify(ModifyData),
    CreateTable(CreateTableData),
    CreateView(CreateViewData),
    CreateIndex(CreateIndexData),
}

impl From<InsertData> for Object {
    fn from(d: InsertData) -> Self {
        Object::Insert(d)
    }
}

impl From<DeleteData> for Object {
    fn from(d: DeleteData) -> Self {
        Object::Delete(d)
    }
}

impl From<ModifyData> for Object {
    fn from(d: ModifyData) -> Self {
        Object::Modify(d)
    }
}

impl From<CreateTableData> for Object {
    fn from(d: CreateTableData) -> Self {
        Object::CreateTable(d)
    }
}

impl From<CreateViewData> for Object {
    fn from(d: CreateViewData) -> Self {
        Object::CreateView(d)
    }
}

impl From<CreateIndexData> for Object {
    fn from(d: CreateIndexData) -> Self {
        Object::CreateIndex(d)
    }
}

pub struct Parser {
    lex: Lexer,
}

impl Parser {
    pub fn new(s: &str) -> Parser {
        let lex = Lexer::new(s);
        Parser { lex }
    }

    pub fn field(&mut self) -> Result<String, BadSyntaxError> {
        self.lex.eat_id()
    }

    pub fn constant(&mut self) -> Result<Constant, BadSyntaxError> {
        if self.lex.match_string_constant() {
            return Ok(Constant::with_string(&self.lex.eat_string_constant()?));
        }
        Ok(Constant::with_int(self.lex.eat_int_constant()?))
    }

    pub fn expression(&mut self) -> Result<Expression, BadSyntaxError> {
        if self.lex.match_id() {
            return Ok(Expression::with_string(&self.field()?));
        }
        Ok(Expression::with_constant(self.constant()?))
    }

    pub fn term(&mut self) -> Result<Term, BadSyntaxError> {
        let lhs = self.expression()?;
        self.lex.eat_delim('=')?;
        let rhs = self.expression()?;
        Ok(Term::new(lhs, rhs))
    }

    pub fn predicate(&mut self) -> Result<Predicate, BadSyntaxError> {
        let mut pred = Predicate::with_term(self.term()?);
        if self.lex.match_keyword("and") {
            self.lex.eat_keyword("and")?;
            pred.conjoin_with(self.predicate()?);
        }
        Ok(pred)
    }

    pub fn query(&mut self) -> Result<QueryData, BadSyntaxError> {
        self.lex.eat_keyword("select")?;
        let fields = self.select_list()?;
        self.lex.eat_keyword("from")?;
        let tables = self.table_list()?;
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }
        Ok(QueryData::new(fields, tables, pred))
    }

    fn select_list(&mut self) -> Result<Vec<String>, BadSyntaxError> {
        let mut l = vec![self.field()?];
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            l.extend(self.select_list()?);
        }
        Ok(l)
    }

    fn table_list(&mut self) -> Result<Vec<String>, BadSyntaxError> {
        let mut l = vec![self.lex.eat_id()?];
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            l.extend(self.table_list()?);
        }
        Ok(l)
    }

    pub fn update_cmd(&mut self) -> Result<Object, BadSyntaxError> {
        if self.lex.match_keyword("insert") {
            return Ok(self.insert()?.into());
        } else if self.lex.match_keyword("delete") {
            return Ok(self.delete()?.into());
        } else if self.lex.match_keyword("update") {
            return Ok(self.modify()?.into());
        }
        self.create()
    }

    fn create(&mut self) -> Result<Object, BadSyntaxError> {
        self.lex.eat_keyword("create")?;
        if self.lex.match_keyword("table") {
            return Ok(self.create_table()?.into());
        } else if self.lex.match_keyword("view") {
            return Ok(self.create_view()?.into());
        }
        Ok(self.create_index()?.into())
    }

    pub fn delete(&mut self) -> Result<DeleteData, BadSyntaxError> {
        self.lex.eat_keyword("delete")?;
        self.lex.eat_keyword("from")?;
        let tblname = self.lex.eat_id()?;
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }
        Ok(DeleteData::new(&tblname, pred))
    }

    pub fn insert(&mut self) -> Result<InsertData, BadSyntaxError> {
        self.lex.eat_keyword("insert")?;
        self.lex.eat_keyword("into")?;
        let tblname = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let flds = self.field_list()?;
        self.lex.eat_delim(')')?;
        self.lex.eat_keyword("values")?;
        self.lex.eat_delim('(')?;
        let vals = self.const_list()?;
        self.lex.eat_delim(')')?;
        Ok(InsertData::new(&tblname, flds, vals))
    }

    fn field_list(&mut self) -> Result<Vec<String>, BadSyntaxError> {
        let mut l = vec![self.field()?];
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            l.extend(self.field_list()?);
        }
        Ok(l)
    }

    fn const_list(&mut self) -> Result<Vec<Constant>, BadSyntaxError> {
        let mut l = vec![self.constant()?];
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            l.extend(self.const_list()?);
        }
        Ok(l)
    }

    pub fn modify(&mut self) -> Result<ModifyData, BadSyntaxError> {
        self.lex.eat_keyword("update")?;
        let tblname = self.lex.eat_id()?;
        self.lex.eat_keyword("set")?;
        let fldname = self.field()?;
        self.lex.eat_delim('=')?;
        let newval = self.expression()?;
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }
        Ok(ModifyData::new(&tblname, &fldname, newval, pred))
    }

    pub fn create_table(&mut self) -> Result<CreateTableData, BadSyntaxError> {
        self.lex.eat_keyword("table")?;
        let tblname = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let sch = self.field_defs()?;
        self.lex.eat_delim(')')?;
        Ok(CreateTableData::new(&tblname, sch))
    }

    fn field_defs(&mut self) -> Result<Schema, BadSyntaxError> {
        let mut schema = self.field_def()?;
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            let schema2 = self.field_defs()?;
            schema.add_all(&schema2);
        }
        Ok(schema)
    }

    fn field_def(&mut self) -> Result<Schema, BadSyntaxError> {
        let fldname = self.field()?;
        self.field_type(&fldname)
    }

    fn field_type(&mut self, fldname: &str) -> Result<Schema, BadSyntaxError> {
        let mut schema = Schema::new();
        if self.lex.match_keyword("int") {
            self.lex.eat_keyword("int")?;
            schema.add_int_field(fldname);
        } else {
            self.lex.eat_keyword("varchar")?;
            self.lex.eat_delim('(')?;
            let str_len = self.lex.eat_int_constant()?;
            self.lex.eat_delim(')')?;
            schema.add_string_field(fldname, str_len as usize);
        }
        Ok(schema)
    }

    pub fn create_view(&mut self) -> Result<CreateViewData, BadSyntaxError> {
        self.lex.eat_keyword("view")?;
        let viewname = self.lex.eat_id()?;
        self.lex.eat_keyword("as")?;
        let qd = self.query()?;
        Ok(CreateViewData::new(&viewname, qd))
    }

    pub fn create_index(&mut self) -> Result<CreateIndexData, BadSyntaxError> {
        self.lex.eat_keyword("index")?;
        let idxname = self.lex.eat_id()?;
        self.lex.eat_keyword("on")?;
        let tblname = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let fldname = self.field()?;
        self.lex.eat_delim(')')?;
        Ok(CreateIndexData::new(&idxname, &tblname, &fldname))
    }
}
