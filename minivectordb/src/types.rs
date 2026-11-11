//! Shared types for MiniVectorDB.

use std::fmt;

pub const ERR_DUPLICATE_KEY: &str = "duplicate_key";
pub const ERR_DIMENSION_MISMATCH: &str = "dimension_mismatch";
pub const ERR_INVALID_VECTOR: &str = "invalid_vector";
pub const ERR_UNSUPPORTED_SQL: &str = "unsupported_sql";
pub const ERR_SYNTAX: &str = "syntax_error";
pub const ERR_TABLE_NOT_FOUND: &str = "table_not_found";
pub const ERR_COLUMN_NOT_FOUND: &str = "column_not_found";
pub const ERR_TYPE_MISMATCH: &str = "type_mismatch";
pub const ERR_TX_STATE: &str = "transaction_state";
pub const ERR_INTERNAL: &str = "internal_error";
pub const ERR_IO: &str = "io_error";
pub const ERR_RESPONSE_TOO_LARGE: &str = "response_too_large";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbError {
    pub code: String,
    pub message: String,
}

impl DbError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        DbError { code: code.to_string(), message: message.into() }
    }
    pub fn unsupported(message: impl Into<String>) -> Self {
        DbError::new(ERR_UNSUPPORTED_SQL, message)
    }
    pub fn syntax(message: impl Into<String>) -> Self {
        DbError::new(ERR_SYNTAX, message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        DbError::new(ERR_INTERNAL, message)
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DbError {}

impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::new(ERR_IO, e.to_string())
    }
}

pub type DbResult<T> = Result<T, DbError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColType {
    BigInt,
    Varchar(u32),
    Vector(u32),
}

impl ColType {
    pub fn name(&self) -> String {
        match self {
            ColType::BigInt => "BIGINT".to_string(),
            ColType::Varchar(n) => format!("VARCHAR({})", n),
            ColType::Vector(d) => format!("VECTOR({})", d),
        }
    }
    pub fn is_vector(&self) -> bool {
        matches!(self, ColType::Vector(_))
    }
    pub fn vector_dim(&self) -> Option<usize> {
        match self {
            ColType::Vector(d) => Some(*d as usize),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Str(String),
    Vec(Vec<f32>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }
    pub fn as_vec(&self) -> Option<&[f32]> {
        match self {
            Value::Vec(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

pub type Row = Vec<Value>;

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColType,
    pub is_pk: bool,
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub pk: usize,
}

impl TableDef {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        let lower = name.to_ascii_lowercase();
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(&lower))
    }
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
    pub fn vector_columns(&self) -> Vec<usize> {
        self.columns.iter().enumerate().filter(|(_, c)| c.ty.is_vector()).map(|(i, _)| i).collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchMode {
    Exact,
    Ann,
}

impl SearchMode {
    pub fn from_wire(s: Option<&str>) -> SearchMode {
        match s {
            Some("ann") => SearchMode::Ann,
            _ => SearchMode::Exact,
        }
    }
}

/// Result of executing one SQL statement.
#[derive(Clone, Debug)]
pub enum ExecResult {
    /// Statement succeeded; no assertion fields (public {} expectations).
    Ack,
    /// Affected row count for DML.
    Affected(u64),
    /// Result set.
    Query { columns: Vec<String>, rows: Vec<Row> },
}