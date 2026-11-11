//! SQL frontend: AST, tokenizer, parser.

pub mod ast;
pub mod lexer;
pub mod parser;

pub use ast::*;

pub fn parse(sql: &str) -> crate::types::DbResult<Stmt> {
    parser::parse(sql)
}
