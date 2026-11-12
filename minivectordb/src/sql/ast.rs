//! SQL AST for the MiniVectorDB SQL subset.

use crate::types::{ColType, Value};

#[derive(Clone, Debug)]
pub enum Stmt {
    CreateTable {
        name: String,
        columns: Vec<ColumnSpec>,
        if_not_exists: bool,
    },
    CreateVectorIndex {
        index_name: String,
        table: String,
        column: String,
    },
    Insert {
        table: String,
        rows: Vec<Vec<Expr>>,
    },
    /// INSERT with an explicit column list.
    InsertOrdered {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<Expr>>,
    },
    Update {
        table: String,
        sets: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    Select {
        items: Vec<SelectItem>,
        table: String,
        filter: Option<Expr>,
        order_by: Vec<OrderKey>,
        limit: Option<usize>,
        offset: usize,
    },
    Begin,
    Commit,
    Rollback,
    /// Recognized but not supported by the competition subset (e.g. ALTER TABLE).
    Unsupported,
}

#[derive(Clone, Debug)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: ColType,
    pub primary_key: bool,
}

#[derive(Clone, Debug)]
pub enum SelectItem {
    Star,
    Column { name: String },
    Expr { expr: Expr, alias: String },
}

#[derive(Clone, Debug)]
pub struct OrderKey {
    pub expr: Expr,
    pub desc: bool,
}

#[derive(Clone, Debug)]
pub enum Expr {
    Lit(Value),
    Column(String),
    L2Distance { column: String, query: Box<Expr> },
    Binary { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    Unary { negate: bool, expr: Box<Expr> },
    /// Function call we do not support (kept so the statement can be rejected cleanly).
    Unsupported(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
}