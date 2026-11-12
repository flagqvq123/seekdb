//! Query engine: statement dispatch, DDL, DML, transaction control and SELECT.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::exec;
use crate::sql::ast::{ColumnSpec, Expr, SelectItem, Stmt};
use crate::storage::{DiskStorage, MemStorage, Storage};
use crate::types::*;

use crate::table_cache::{TableCache, DEFAULT_CACHE_BUDGET_BYTES};

pub struct Engine {
    storage: Box<dyn Storage>,
    /// Tables whose hot caches are stale after writes.
    dirty_tables: HashSet<u32>,
    /// Hot cache of the most recently queried table.
    cache: Option<TableCache>,
}

impl Engine {
    pub fn open(dir: &Path) -> DbResult<Engine> {
        let storage = DiskStorage::open(dir)?;
        Ok(Engine { storage: Box::new(storage), dirty_tables: HashSet::new(), cache: None })
    }

    pub fn open_memory() -> Engine {
        Engine { storage: Box::new(MemStorage::new()), dirty_tables: HashSet::new(), cache: None }
    }

    pub fn shutdown(&mut self) -> DbResult<()> {
        if self.storage.in_txn() {
            self.storage.rollback().ok();
        }
        self.storage.flush()
    }

    pub fn execute(&mut self, sql: &str, _mode: SearchMode) -> DbResult<ExecResult> {
        let stmt = crate::sql::parse(sql)?;
        self.execute_stmt(stmt)
    }

    pub fn execute_stmt(&mut self, stmt: Stmt) -> DbResult<ExecResult> {
        match stmt {
            Stmt::Unsupported => Err(DbError::unsupported("statement is not supported")),
            Stmt::Begin => {
                if self.storage.in_txn() {
                    return Err(DbError::new(ERR_TX_STATE, "transaction already active"));
                }
                self.storage.begin()?;
                Ok(ExecResult::Ack)
            }
            Stmt::Commit => {
                if self.storage.in_txn() {
                    self.storage.commit()?;
                    self.dirty_tables.clear();
                }
                Ok(ExecResult::Ack)
            }
            Stmt::Rollback => {
                if self.storage.in_txn() {
                    self.storage.rollback()?;
                    self.dirty_tables.clear();
                }
                Ok(ExecResult::Ack)
            }
            Stmt::CreateTable { name, columns } => self.mutate(|e| e.create_table(&name, &columns)),
            Stmt::CreateVectorIndex { index_name, table, column } => {
                self.mutate(|e| e.create_vector_index(&index_name, &table, &column))
            }
            Stmt::Insert { table, rows } => self.mutate(|e| e.insert(&table, None, &rows)),
            Stmt::InsertOrdered { table, columns, rows } => {
                self.mutate(|e| e.insert(&table, Some(&columns), &rows))
            }
            Stmt::Update { table, sets, filter } => self.mutate(|e| e.update(&table, &sets, filter.as_ref())),
            Stmt::Delete { table, filter } => self.mutate(|e| e.delete(&table, filter.as_ref())),
            Stmt::Select { items, table, filter, order_by, limit } => {
                self.select(&items, &table, filter.as_ref(), &order_by, limit)
            }
        }
    }

    /// Run a mutating statement with statement-level atomicity and autocommit.
    fn mutate<T>(&mut self, f: impl FnOnce(&mut Self) -> DbResult<T>) -> DbResult<T> {
        if self.storage.in_txn() {
            self.storage.statement_mark();
            match f(self) {
                Ok(v) => Ok(v),
                Err(e) => {
                    self.storage.rollback_statement().ok();
                    Err(e)
                }
            }
        } else {
            self.storage.begin()?;
            match f(self) {
                Ok(v) => match self.storage.commit() {
                    Ok(()) => Ok(v),
                    Err(e) => {
                        self.storage.rollback().ok();
                        Err(e)
                    }
                },
                Err(e) => {
                    self.storage.rollback().ok();
                    Err(e)
                }
            }
        }
    }

    fn require_table(&self, name: &str) -> DbResult<TableDef> {
        self.storage
            .table(name)
            .ok_or_else(|| DbError::new(ERR_TABLE_NOT_FOUND, format!("table {} does not exist", name)))
    }

    fn create_table(&mut self, name: &str, specs: &[ColumnSpec]) -> DbResult<ExecResult> {
        if self.storage.table(name).is_some() {
            return Err(DbError::new("table_exists", format!("table {} already exists", name)));
        }
        if specs.is_empty() {
            return Err(DbError::syntax("CREATE TABLE requires at least one column"));
        }
        let mut columns = Vec::with_capacity(specs.len());
        let mut pk = None;
        let mut seen: HashSet<String> = HashSet::new();
        for spec in specs {
            let lower = spec.name.to_ascii_lowercase();
            if !seen.insert(lower) {
                return Err(DbError::syntax(format!("duplicate column {}", spec.name)));
            }
            if spec.primary_key {
                if pk.is_some() {
                    return Err(DbError::unsupported("composite primary keys are not supported"));
                }
                if spec.ty != ColType::BigInt {
                    return Err(DbError::unsupported("primary key must be BIGINT"));
                }
                pk = Some(columns.len());
            }
            columns.push(ColumnDef { name: spec.name.clone(), ty: spec.ty.clone(), is_pk: spec.primary_key });
        }
        let pk = pk.ok_or_else(|| DbError::unsupported("a BIGINT PRIMARY KEY column is required"))?;
        self.storage.create_table(name, columns, pk)?;
        Ok(ExecResult::Ack)
    }

    fn create_vector_index(
        &mut self,
        _index_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> DbResult<ExecResult> {
        let table = self.require_table(table_name)?;
        let column = exec::resolve_column(&table, column_name)?;
        if !table.columns[column].ty.is_vector() {
            return Err(DbError::new(
                ERR_TYPE_MISMATCH,
                format!("{} is not a VECTOR column", column_name),
            ));
        }
        self.storage.create_vector_index(table.id, column)?;
        self.dirty_tables.insert(table.id);
        Ok(ExecResult::Ack)
    }

    fn insert(
        &mut self,
        table_name: &str,
        columns: Option<&[String]>,
        rows: &[Vec<Expr>],
    ) -> DbResult<ExecResult> {
        let table = self.require_table(table_name)?;
        // Build the mapping from value position to table column.
        let positions: Vec<usize> = match columns {
            None => (0..table.columns.len()).collect(),
            Some(names) => {
                let mut out = Vec::with_capacity(names.len());
                for name in names {
                    let idx = exec::resolve_column(&table, name)?;
                    if out.contains(&idx) {
                        return Err(DbError::syntax(format!("duplicate column {} in INSERT", name)));
                    }
                    out.push(idx);
                }
                out
            }
        };
        let mut count = 0u64;
        for spec in rows {
            if spec.len() != positions.len() {
                return Err(DbError::syntax(format!(
                    "column count mismatch: expected {}, got {}",
                    positions.len(),
                    spec.len()
                )));
            }
            let mut row: Row = vec![Value::Null; table.columns.len()];
            for (value_expr, &col_idx) in spec.iter().zip(positions.iter()) {
                let raw = exec::eval(value_expr, &table, None)?;
                let coerced = exec::coerce_to_column(&table.columns[col_idx].ty, raw, &table.columns[col_idx].name)?;
                row[col_idx] = coerced;
            }
            let pk = match &row[table.pk] {
                Value::Int(v) => *v,
                _ => {
                    return Err(DbError::syntax("primary key value must be an integer"));
                }
            };
            if self.storage.get(table.id, pk)?.is_some() {
                return Err(DbError::new(ERR_DUPLICATE_KEY, format!("duplicate primary key {}", pk)));
            }
            self.storage.put(table.id, pk, row)?;
            count += 1;
        }
        self.dirty_tables.insert(table.id);
        Ok(ExecResult::Affected(count))
    }

    fn update(
        &mut self,
        table_name: &str,
        sets: &[(String, Expr)],
        filter: Option<&Expr>,
    ) -> DbResult<ExecResult> {
        let table = self.require_table(table_name)?;
        let mut assignments = Vec::with_capacity(sets.len());
        for (name, expr) in sets {
            let idx = exec::resolve_column(&table, name)?;
            if idx == table.pk {
                return Err(DbError::unsupported("updating the primary key is not supported"));
            }
            if assignments.iter().any(|(i, _): &(usize, &Expr)| *i == idx) {
                return Err(DbError::syntax(format!("duplicate column {} in UPDATE", name)));
            }
            assignments.push((idx, expr));
        }
        let mut matches: Vec<(i64, Row)> = Vec::new();
        {
            let mut error: Option<DbError> = None;
            let table_ref = &table;
            let filter_ref = filter;
            self.storage.scan(table.id, &mut |pk, row| {
                let keep = match filter_ref {
                    Some(f) => match exec::eval(f, table_ref, Some(row)) {
                        Ok(v) => exec::truthy(&v),
                        Err(e) => {
                            error = Some(e);
                            return false;
                        }
                    },
                    None => true,
                };
                if keep {
                    let mut new_row = row.clone();
                    for (idx, expr) in &assignments {
                        match exec::eval(expr, table_ref, Some(row)) {
                            Ok(v) => match exec::coerce_to_column(&table_ref.columns[*idx].ty, v, &table_ref.columns[*idx].name) {
                                Ok(c) => new_row[*idx] = c,
                                Err(e) => {
                                    error = Some(e);
                                    return false;
                                }
                            },
                            Err(e) => {
                                error = Some(e);
                                return false;
                            }
                        }
                    }
                    matches.push((pk, new_row));
                }
                true
            })?;
            if let Some(e) = error {
                return Err(e);
            }
        }
        let count = matches.len() as u64;
        for (pk, row) in matches {
            self.storage.put(table.id, pk, row)?;
        }
        self.dirty_tables.insert(table.id);
        Ok(ExecResult::Affected(count))
    }

    fn delete(&mut self, table_name: &str, filter: Option<&Expr>) -> DbResult<ExecResult> {
        let table = self.require_table(table_name)?;
        let mut pks: Vec<i64> = Vec::new();
        {
            let mut error: Option<DbError> = None;
            let table_ref = &table;
            self.storage.scan(table.id, &mut |pk, row| {
                let keep = match filter {
                    Some(f) => match exec::eval(f, table_ref, Some(row)) {
                        Ok(v) => exec::truthy(&v),
                        Err(e) => {
                            error = Some(e);
                            return false;
                        }
                    },
                    None => true,
                };
                if keep {
                    pks.push(pk);
                }
                true
            })?;
            if let Some(e) = error {
                return Err(e);
            }
        }
        let mut count = 0u64;
        for pk in pks {
            if self.storage.delete(table.id, pk)? {
                count += 1;
            }
        }
        self.dirty_tables.insert(table.id);
        Ok(ExecResult::Affected(count))
    }


    /// Ensure the hot cache for this table/vector column is fresh.
    fn ensure_cache(&mut self, table: &TableDef, col: usize) -> DbResult<bool> {
        let dirty = self.dirty_tables.contains(&table.id);
        let reusable = match &self.cache {
            Some(cache) => cache.table_id == table.id && cache.vector_col == col,
            None => false,
        };
        if reusable && !dirty {
            return Ok(true);
        }
        self.cache = None;
        match TableCache::build(&*self.storage, table, col, DEFAULT_CACHE_BUDGET_BYTES)? {
            Some(cache) => {
                self.cache = Some(cache);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Recognise a simple <column> = <literal> predicate for dictionary lookup.
    fn simple_equality(filter: &Expr, table: &TableDef) -> Option<(usize, Value)> {
        let (left, right) = match filter {
            Expr::Binary { op: crate::sql::ast::BinOp::Eq, left, right } => {
                (left.as_ref(), right.as_ref())
            }
            _ => return None,
        };
        let (column, literal) = match (left, right) {
            (Expr::Column(name), Expr::Lit(value)) => (name, value),
            (Expr::Lit(value), Expr::Column(name)) => (name, value),
            _ => return None,
        };
        let idx = table.column_index(column)?;
        match literal {
            Value::Int(_) | Value::Str(_) => Some((idx, literal.clone())),
            _ => None,
        }
    }

    /// Detect and execute the hot pattern:
    /// SELECT ... L2_DISTANCE(col, q) ... [WHERE cond] ORDER BY distance LIMIT k
    fn try_vector_fast_path(
        &mut self,
        items: &[SelectItem],
        table: &TableDef,
        filter: Option<&Expr>,
        order_by: &[crate::sql::ast::OrderKey],
        limit: Option<usize>,
    ) -> DbResult<Option<ExecResult>> {
        let limit = match limit {
            Some(l) => l,
            None => return Ok(None),
        };
        if order_by.len() != 1 || order_by[0].desc {
            return Ok(None);
        }
        if self.storage.in_txn() {
            return Ok(None);
        }
        // Resolve the ordered distance expression and its vector column.
        let distance_expr: Expr = match &order_by[0].expr {
            Expr::L2Distance { .. } => order_by[0].expr.clone(),
            Expr::Column(alias) => {
                let mut found: Option<Expr> = None;
                for item in items {
                    if let SelectItem::Expr { expr, alias: item_alias } = item {
                        if item_alias.eq_ignore_ascii_case(alias) {
                            found = Some(expr.clone());
                            break;
                        }
                    }
                }
                match found {
                    Some(expr) => match expr {
                        Expr::L2Distance { .. } => expr,
                        _ => return Ok(None),
                    },
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        let (column_name, query_expr) = match &distance_expr {
            Expr::L2Distance { column, query } => (column.clone(), query.clone()),
            _ => return Ok(None),
        };
        let col = match table.column_index(&column_name) {
            Some(c) => c,
            None => return Ok(None),
        };
        let dim = match table.columns[col].ty.vector_dim() {
            Some(d) => d,
            None => return Ok(None),
        };
        // Only plain literal queries are hot-path eligible.
        let query_value = exec::eval(&query_expr, table, None)?;
        let query: Vec<f32> = match query_value {
            Value::Str(s) => match crate::vector::parse_vector(&s) {
                Ok(v) => v,
                Err(_) => return Ok(None),
            },
            Value::Vec(v) => v,
            _ => return Ok(None),
        };
        crate::vector::validate_dim(&query, dim)?;
        if !self.ensure_cache(table, col)? {
            return Ok(None);
        }
        let candidates: Option<Vec<u32>> = match filter {
            None => None,
            Some(cond) => {
                let cache = match self.cache.as_ref() {
                    Some(c) => c,
                    None => return Ok(None),
                };
                if let Some((eq_col, value)) = Self::simple_equality(cond, table) {
                    match cache.lookup_eq(eq_col, &value) {
                        Some(cands) => Some(cands),
                        None => return Ok(None),
                    }
                } else {
                    let mut cands: Vec<u32> = Vec::new();
                    for (idx, row) in cache.rows.iter().enumerate() {
                        match exec::eval(cond, table, Some(row)) {
                            Ok(v) => {
                                if exec::truthy(&v) {
                                    cands.push(idx as u32);
                                }
                            }
                            Err(e) => {
                                // Cached rows blank the vector column; such a
                                // predicate must use the streaming slow path.
                                if e.code == ERR_INVALID_VECTOR || e.code == ERR_COLUMN_NOT_FOUND {
                                    return Ok(None);
                                }
                                return Err(e);
                            }
                        }
                    }
                    Some(cands)
                }
            }
        };
        let scored = {
            let cache = match self.cache.as_ref() {
                Some(c) => c,
                None => return Ok(None),
            };
            cache.topk(&query, limit, candidates.as_deref())
        };
        let mut columns = Vec::with_capacity(items.len());
        for item in items {
            columns.push(exec::projection_name(item, table)?);
        }
        let mut rows: Vec<Row> = Vec::with_capacity(scored.len());
        for (pk, _dist) in &scored {
            let row = match self.storage.get(table.id, *pk)? {
                Some(r) => r,
                None => continue,
            };
            let mut projected = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    SelectItem::Star => {}
                    SelectItem::Column { name } => {
                        let idx = exec::resolve_column(table, name)?;
                        projected.push(row[idx].clone());
                    }
                    SelectItem::Expr { expr, .. } => {
                        projected.push(exec::eval(expr, table, Some(&row))?);
                    }
                }
            }
            rows.push(projected);
        }
        Ok(Some(ExecResult::Query { columns, rows }))
    }

    fn select(
        &mut self,
        items: &[SelectItem],
        table_name: &str,
        filter: Option<&Expr>,
        order_by: &[crate::sql::ast::OrderKey],
        limit: Option<usize>,
    ) -> DbResult<ExecResult> {
        let table = self.require_table(table_name)?;
        let items = exec::expand_items(items, &table);
        let mut columns = Vec::with_capacity(items.len());
        for item in &items {
            columns.push(exec::projection_name(item, &table)?);
        }

        if let Some(result) = self.try_vector_fast_path(&items, &table, filter, order_by, limit)? {
            return Ok(result);
        }

        struct OutRow {
            pk: i64,
            projected: Row,
            keys: Vec<Value>,
        }

        let mut out: Vec<OutRow> = Vec::new();
        let stop_early = order_by.is_empty() && limit.is_some();
        let limit_value = limit.unwrap_or(usize::MAX);
        let mut error: Option<DbError> = None;
        {
            let table_ref = &table;
            let items_ref = &items;
            let order_ref = order_by;
            self.storage.scan(table.id, &mut |pk, row| {
                let keep = match filter {
                    Some(f) => match exec::eval(f, table_ref, Some(row)) {
                        Ok(v) => exec::truthy(&v),
                        Err(e) => {
                            error = Some(e);
                            return false;
                        }
                    },
                    None => true,
                };
                if !keep {
                    return true;
                }
                let mut projected = Vec::with_capacity(items_ref.len());
                for item in items_ref {
                    match item {
                        SelectItem::Star => {}
                        SelectItem::Column { name } => match exec::resolve_column(table_ref, name) {
                            Ok(idx) => projected.push(row[idx].clone()),
                            Err(e) => {
                                error = Some(e);
                                return false;
                            }
                        },
                        SelectItem::Expr { expr, .. } => match exec::eval(expr, table_ref, Some(row)) {
                            Ok(v) => projected.push(v),
                            Err(e) => {
                                error = Some(e);
                                return false;
                            }
                        },
                    }
                }
                let mut keys = Vec::with_capacity(order_ref.len());
                for key in order_ref {
                    match exec::order_value(key, table_ref, row, items_ref, &projected) {
                        Ok(v) => keys.push(v),
                        Err(e) => {
                            error = Some(e);
                            return false;
                        }
                    }
                }
                out.push(OutRow { pk, projected, keys });
                if stop_early && out.len() >= limit_value {
                    return false;
                }
                true
            })?;
            if let Some(e) = error {
                return Err(e);
            }
        }

        if !order_by.is_empty() {
            out.sort_by(|a, b| {
                for (i, key) in order_by.iter().enumerate() {
                    let ord = exec::order_cmp(&a.keys[i], &b.keys[i]);
                    let ord = if key.desc { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                a.pk.cmp(&b.pk)
            });
        }
        if let Some(limit) = limit {
            out.truncate(limit);
        }
        let rows: Vec<Row> = out.into_iter().map(|r| r.projected).collect();
        Ok(ExecResult::Query { columns, rows })
    }
}