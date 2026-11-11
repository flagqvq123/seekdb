//! Recursive-descent SQL parser for the competition subset.

use crate::sql::ast::*;
use crate::sql::lexer::{tokenize, Tok};
use crate::types::{ColType, DbError, DbResult, Value};

pub fn parse(sql: &str) -> DbResult<Stmt> {
    let toks = tokenize(sql)?;
    let mut p = Parser { toks, pos: 0 };
    let stmt = p.statement()?;
    p.skip_semis();
    if !p.at_eof() {
        return Err(DbError::syntax("trailing tokens after statement"));
    }
    Ok(stmt)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        self.toks.get(self.pos).unwrap_or(&Tok::Eof)
    }
    fn peek_at(&self, n: usize) -> &Tok {
        self.toks.get(self.pos + n).unwrap_or(&Tok::Eof)
    }
    fn bump(&mut self) -> Tok {
        let t = self.toks.get(self.pos).cloned().unwrap_or(Tok::Eof);
        self.pos += 1;
        t
    }
    fn at_eof(&self) -> bool {
        matches!(self.peek(), Tok::Eof)
    }
    fn skip_semis(&mut self) {
        while matches!(self.peek(), Tok::Semi) {
            self.pos += 1;
        }
    }
    fn is_kw(&self, kw: &str) -> bool {
        match self.peek() {
            Tok::Ident(s) => s.eq_ignore_ascii_case(kw),
            _ => false,
        }
    }
    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect_kw(&mut self, kw: &str) -> DbResult<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(DbError::syntax(format!("expected keyword {}", kw)))
        }
    }
    fn eat_tok(&mut self, t: &Tok) -> bool {
        if self.peek() == t {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect_tok(&mut self, t: &Tok) -> DbResult<()> {
        if self.eat_tok(t) {
            Ok(())
        } else {
            Err(DbError::syntax(format!("expected {:?}, found {:?}", t, self.peek())))
        }
    }
    fn ident(&mut self) -> DbResult<String> {
        match self.bump() {
            Tok::Ident(s) => Ok(s),
            other => Err(DbError::syntax(format!("expected identifier, found {:?}", other))),
        }
    }
    fn number(&mut self) -> DbResult<i64> {
        match self.bump() {
            Tok::Number(v) => Ok(v),
            other => Err(DbError::syntax(format!("expected integer, found {:?}", other))),
        }
    }

    fn statement(&mut self) -> DbResult<Stmt> {
        if self.at_eof() {
            return Err(DbError::syntax("empty statement"));
        }
        if self.is_kw("create") {
            return self.create();
        }
        if self.is_kw("insert") {
            return self.insert();
        }
        if self.is_kw("update") {
            return self.update();
        }
        if self.is_kw("delete") {
            return self.delete();
        }
        if self.is_kw("select") {
            return self.select();
        }
        if self.is_kw("begin") {
            self.pos += 1;
            self.eat_kw("work");
            self.eat_kw("transaction");
            return Ok(Stmt::Begin);
        }
        if self.is_kw("start") {
            self.pos += 1;
            self.expect_kw("transaction")?;
            return Ok(Stmt::Begin);
        }
        if self.is_kw("commit") {
            self.pos += 1;
            self.eat_kw("work");
            return Ok(Stmt::Commit);
        }
        if self.is_kw("rollback") {
            self.pos += 1;
            self.eat_kw("work");
            return Ok(Stmt::Rollback);
        }
        let kw = match self.peek() {
            Tok::Ident(s) => s.to_ascii_lowercase(),
            _ => String::new(),
        };
        match kw.as_str() {
            "alter" | "drop" | "truncate" | "rename" | "grant" | "revoke" | "analyze"
            | "optimize" | "set" | "use" | "show" | "describe" | "desc" | "explain"
            | "replace" | "merge" | "call" | "prepare" | "execute" | "deallocate"
            | "lock" | "unlock" | "load" | "import" | "export" => Ok(Stmt::Unsupported),
            "" => Err(DbError::syntax("expected SQL statement")),
            other => Err(DbError::syntax(format!("unrecognized statement '{}'", other))),
        }
    }

    fn create(&mut self) -> DbResult<Stmt> {
        self.expect_kw("create")?;
        if self.eat_kw("table") {
            return self.create_table();
        }
        if self.eat_kw("vector") {
            self.expect_kw("index")?;
            let index_name = self.ident()?;
            self.expect_kw("on")?;
            let table = self.ident()?;
            self.expect_tok(&Tok::LParen)?;
            let column = self.ident()?;
            self.expect_tok(&Tok::RParen)?;
            return Ok(Stmt::CreateVectorIndex { index_name, table, column });
        }
        if self.eat_kw("index") {
            // Non-vector index: accepted as a no-op would be wrong; treat as unsupported.
            return Ok(Stmt::Unsupported);
        }
        Ok(Stmt::Unsupported)
    }

    fn create_table(&mut self) -> DbResult<Stmt> {
        if self.eat_kw("if") {
            self.expect_kw("not")?;
            self.expect_kw("exists")?;
        }
        let name = self.ident()?;
        self.expect_tok(&Tok::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.ident()?;
            let ty = self.column_type()?;
            let mut primary_key = false;
            loop {
                if self.eat_kw("primary") {
                    self.expect_kw("key")?;
                    primary_key = true;
                } else if self.eat_kw("not") {
                    self.expect_kw("null")?;
                } else if self.eat_kw("null") {
                } else if self.eat_kw("unique") {
                    return Err(DbError::unsupported("UNIQUE constraints are not supported"));
                } else if self.eat_kw("auto_increment") {
                    return Err(DbError::unsupported("AUTO_INCREMENT is not supported"));
                } else if self.eat_kw("default") {
                    // consume one literal and ignore
                    let _ = self.expr()?;
                } else if self.eat_kw("comment") {
                    let _ = self.literal_value()?;
                } else {
                    break;
                }
            }
            columns.push(ColumnSpec { name: col_name, ty, primary_key });
            if self.eat_tok(&Tok::Comma) {
                continue;
            }
            self.expect_tok(&Tok::RParen)?;
            break;
        }
        if columns.is_empty() {
            return Err(DbError::syntax("CREATE TABLE requires at least one column"));
        }
        Ok(Stmt::CreateTable { name, columns })
    }

    fn column_type(&mut self) -> DbResult<ColType> {
        let name = self.ident()?.to_ascii_lowercase();
        match name.as_str() {
            "bigint" | "int8" | "int" | "integer" | "int4" => Ok(ColType::BigInt),
            "varchar" | "char" | "string" | "text" => {
                if self.eat_tok(&Tok::LParen) {
                    let n = self.number()?;
                    self.expect_tok(&Tok::RParen)?;
                    Ok(ColType::Varchar(n.max(1) as u32))
                } else {
                    Ok(ColType::Varchar(65535))
                }
            }
            "vector" => {
                if self.eat_tok(&Tok::LParen) {
                    let n = self.number()?;
                    self.expect_tok(&Tok::RParen)?;
                    if n <= 0 || n > 65535 {
                        return Err(DbError::syntax("invalid VECTOR dimension"));
                    }
                    Ok(ColType::Vector(n as u32))
                } else {
                    Err(DbError::syntax("VECTOR requires a dimension"))
                }
            }
            other => Err(DbError::unsupported(format!("unsupported column type {}", other))),
        }
    }

    fn insert(&mut self) -> DbResult<Stmt> {
        self.expect_kw("insert")?;
        self.expect_kw("into")?;
        let table = self.ident()?;
        let mut columns: Option<Vec<String>> = None;
        if self.peek() == &Tok::LParen {
            // Could be a column list; peek to distinguish from VALUES(...) - VALUES follows table only.
            self.pos += 1;
            let mut cols = Vec::new();
            loop {
                cols.push(self.ident()?);
                if self.eat_tok(&Tok::Comma) {
                    continue;
                }
                self.expect_tok(&Tok::RParen)?;
                break;
            }
            columns = Some(cols);
        }
        self.expect_kw("values")?;
        let mut rows = Vec::new();
        loop {
            self.expect_tok(&Tok::LParen)?;
            let mut items = Vec::new();
            loop {
                items.push(self.expr()?);
                if self.eat_tok(&Tok::Comma) {
                    continue;
                }
                self.expect_tok(&Tok::RParen)?;
                break;
            }
            rows.push(items);
            if self.eat_tok(&Tok::Comma) {
                continue;
            }
            break;
        }
        if let Some(cols) = columns {
            if cols.is_empty() {
                return Err(DbError::syntax("empty column list"));
            }
            // Reorder into table order: handled by executor via Stmt::InsertWithColumns.
            return Ok(Stmt::InsertOrdered { table, columns: cols, rows });
        }
        Ok(Stmt::Insert { table, rows })
    }

    fn update(&mut self) -> DbResult<Stmt> {
        self.expect_kw("update")?;
        let table = self.ident()?;
        self.expect_kw("set")?;
        let mut sets = Vec::new();
        loop {
            let col = self.ident()?;
            self.expect_tok(&Tok::Eq)?;
            let value = self.expr()?;
            sets.push((col, value));
            if self.eat_tok(&Tok::Comma) {
                continue;
            }
            break;
        }
        let filter = if self.eat_kw("where") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok(Stmt::Update { table, sets, filter })
    }

    fn delete(&mut self) -> DbResult<Stmt> {
        self.expect_kw("delete")?;
        self.expect_kw("from")?;
        let table = self.ident()?;
        let filter = if self.eat_kw("where") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok(Stmt::Delete { table, filter })
    }

    fn select(&mut self) -> DbResult<Stmt> {
        self.expect_kw("select")?;
        let mut items = Vec::new();
        loop {
            if self.eat_tok(&Tok::Star) {
                items.push(SelectItem::Star);
            } else if self.is_kw("distinct") {
                self.pos += 1;
                return Err(DbError::unsupported("SELECT DISTINCT is not supported"));
            } else {
                let expr = self.expr()?;
                if self.eat_kw("as") {
                    let alias = self.ident()?;
                    items.push(SelectItem::Expr { expr, alias });
                } else if let Tok::Ident(name) = self.peek().clone() {
                    if !Self::is_reserved_from(&name) {
                        self.pos += 1;
                        items.push(SelectItem::Expr { expr, alias: name });
                    } else {
                        items.push(SelectItem::Expr { expr, alias: String::new() });
                    }
                } else if let Expr::Column(name) = &expr {
                    items.push(SelectItem::Column { name: name.clone() });
                } else {
                    items.push(SelectItem::Expr { expr, alias: String::new() });
                }
            }
            if self.eat_tok(&Tok::Comma) {
                continue;
            }
            break;
        }
        self.expect_kw("from")?;
        let table = self.ident()?;
        let filter = if self.eat_kw("where") { Some(self.expr()?) } else { None };
        let mut order_by = Vec::new();
        if self.eat_kw("order") {
            self.expect_kw("by")?;
            loop {
                let expr = self.expr()?;
                let desc = if self.eat_kw("desc") {
                    true
                } else {
                    self.eat_kw("asc");
                    false
                };
                order_by.push(OrderKey { expr, desc });
                if self.eat_tok(&Tok::Comma) {
                    continue;
                }
                break;
            }
        }
        let mut limit = None;
        if self.eat_kw("limit") {
            let n = self.number()?;
            if n < 0 {
                return Err(DbError::syntax("negative LIMIT"));
            }
            limit = Some(n as usize);
        }
        Ok(Stmt::Select { items, table, filter, order_by, limit })
    }

    fn is_reserved_from(name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        matches!(
            lower.as_str(),
            "from" | "where" | "order" | "limit" | "group" | "having" | "asc" | "desc"
        )
    }

    fn literal_value(&mut self) -> DbResult<Value> {
        match self.bump() {
            Tok::Number(v) => Ok(Value::Int(v)),
            Tok::Str(s) => Ok(Value::Str(s)),
            Tok::Minus => {
                let v = self.number()?;
                Ok(Value::Int(-v))
            }
            other => Err(DbError::syntax(format!("expected literal, found {:?}", other))),
        }
    }

    fn expr(&mut self) -> DbResult<Expr> {
        self.expr_or()
    }

    fn expr_or(&mut self) -> DbResult<Expr> {
        let mut left = self.expr_and()?;
        while self.eat_kw("or") {
            let right = self.expr_and()?;
            left = Expr::Binary { op: BinOp::Or, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn expr_and(&mut self) -> DbResult<Expr> {
        let mut left = self.expr_not()?;
        while self.eat_kw("and") {
            let right = self.expr_not()?;
            left = Expr::Binary { op: BinOp::And, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn expr_not(&mut self) -> DbResult<Expr> {
        if self.eat_kw("not") {
            let inner = self.expr_not()?;
            return Ok(Expr::Unary { negate: false, expr: Box::new(Expr::Binary {
                op: BinOp::Eq,
                left: Box::new(inner),
                right: Box::new(Expr::Lit(Value::Int(0))),
            }) });
        }
        self.expr_cmp()
    }

    fn expr_cmp(&mut self) -> DbResult<Expr> {
        let left = self.expr_add()?;
        let op = match self.peek() {
            Tok::Eq => Some(BinOp::Eq),
            Tok::Ne => Some(BinOp::Ne),
            Tok::Lt => Some(BinOp::Lt),
            Tok::Le => Some(BinOp::Le),
            Tok::Gt => Some(BinOp::Gt),
            Tok::Ge => Some(BinOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.pos += 1;
            let right = self.expr_add()?;
            return Ok(Expr::Binary { op, left: Box::new(left), right: Box::new(right) });
        }
        Ok(left)
    }

    fn expr_add(&mut self) -> DbResult<Expr> {
        let mut left = self.expr_mul()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => BinOp::Add,
                Tok::Minus => BinOp::Sub,
                _ => break,
            };
            self.pos += 1;
            let right = self.expr_mul()?;
            left = Expr::Binary { op, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn expr_mul(&mut self) -> DbResult<Expr> {
        let mut left = self.expr_unary()?;
        loop {
            let op = match self.peek() {
                Tok::Star => BinOp::Mul,
                Tok::Slash => BinOp::Div,
                _ => break,
            };
            self.pos += 1;
            let right = self.expr_unary()?;
            left = Expr::Binary { op, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn expr_unary(&mut self) -> DbResult<Expr> {
        if self.eat_tok(&Tok::Minus) {
            let inner = self.expr_unary()?;
            return Ok(match inner {
                Expr::Lit(Value::Int(v)) => Expr::Lit(Value::Int(-v)),
                other => Expr::Unary { negate: true, expr: Box::new(other) },
            });
        }
        if self.eat_tok(&Tok::Plus) {
            return self.expr_unary();
        }
        self.expr_primary()
    }

    fn expr_primary(&mut self) -> DbResult<Expr> {
        match self.bump() {
            Tok::Number(v) => Ok(Expr::Lit(Value::Int(v))),
            Tok::Str(s) => Ok(Expr::Lit(Value::Str(s))),
            Tok::LParen => {
                let e = self.expr()?;
                self.expect_tok(&Tok::RParen)?;
                Ok(e)
            }
            Tok::Ident(name) => {
                if name.eq_ignore_ascii_case("null") {
                    return Ok(Expr::Lit(Value::Null));
                }
                if name.eq_ignore_ascii_case("true") {
                    return Ok(Expr::Lit(Value::Int(1)));
                }
                if name.eq_ignore_ascii_case("false") {
                    return Ok(Expr::Lit(Value::Int(0)));
                }
                if self.eat_tok(&Tok::LParen) {
                    let mut args = Vec::new();
                    if !self.eat_tok(&Tok::RParen) {
                        loop {
                            args.push(self.expr()?);
                            if self.eat_tok(&Tok::Comma) {
                                continue;
                            }
                            self.expect_tok(&Tok::RParen)?;
                            break;
                        }
                    }
                    if name.eq_ignore_ascii_case("l2_distance") {
                        if args.len() != 2 {
                            return Err(DbError::syntax("L2_DISTANCE expects two arguments"));
                        }
                        let column = match &args[0] {
                            Expr::Column(c) => c.clone(),
                            other => return Err(DbError::syntax(format!(
                                "L2_DISTANCE first argument must be a vector column, got {:?}", other))),
                        };
                        return Ok(Expr::L2Distance { column, query: Box::new(args[1].clone()) });
                    }
                    if name.eq_ignore_ascii_case("cosine_distance") {
                        return Err(DbError::unsupported("COSINE_DISTANCE is not supported"));
                    }
                    return Ok(Expr::Unsupported(name));
                }
                Ok(Expr::Column(name))
            }
            other => Err(DbError::syntax(format!("unexpected token in expression: {:?}", other))),
        }
    }
}
