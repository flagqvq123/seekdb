//! Storage abstraction. MemStorage is a complete in-memory reference
//! implementation used for development; the durable engine implements the
//! same trait with a write-ahead log on disk.

use std::collections::{BTreeMap, HashMap};

use crate::types::{ColumnDef, DbError, DbResult, Row, TableDef};

pub mod disk;
pub use disk::DiskStorage;

pub trait Storage: Send {
    fn table(&self, name: &str) -> Option<TableDef>;
    fn table_by_id(&self, id: u32) -> Option<TableDef>;
    fn list_tables(&self) -> Vec<TableDef>;
    fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>, pk: usize) -> DbResult<TableDef>;
    fn create_vector_index(&mut self, table_id: u32, column: usize) -> DbResult<()>;
    fn vector_indexes(&self, table_id: u32) -> Vec<usize>;

    fn in_txn(&self) -> bool;
    fn begin(&mut self) -> DbResult<()>;
    fn commit(&mut self) -> DbResult<()>;
    fn rollback(&mut self) -> DbResult<()>;
    fn statement_mark(&mut self);
    fn rollback_statement(&mut self) -> DbResult<()>;

    fn get(&self, table_id: u32, pk: i64) -> DbResult<Option<Row>>;
    fn put(&mut self, table_id: u32, pk: i64, row: Row) -> DbResult<()>;
    fn delete(&mut self, table_id: u32, pk: i64) -> DbResult<bool>;
    fn scan(&self, table_id: u32, f: &mut dyn FnMut(i64, &Row) -> bool) -> DbResult<()>;

    fn flush(&mut self) -> DbResult<()>;
}

#[derive(Default)]
struct Txn {
    /// Latest write per row in this transaction; None is a delete.
    writes: HashMap<(u32, i64), Option<Row>>,
    /// Undo log for statement-level rollback: (table, pk, previous overlay value).
    undo: Vec<(u32, i64, Option<Option<Row>>)>,
    new_tables: Vec<TableDef>,
    new_indexes: Vec<(u32, usize)>,
    table_mark: usize,
    index_mark: usize,
    undo_mark: usize,
}

impl Txn {
    fn mark(&mut self) {
        self.table_mark = self.new_tables.len();
        self.index_mark = self.new_indexes.len();
        self.undo_mark = self.undo.len();
    }
}

#[derive(Default)]
pub struct MemStorage {
    tables: HashMap<u32, TableDef>,
    names: HashMap<String, u32>,
    rows: HashMap<u32, BTreeMap<i64, Row>>,
    indexes: HashMap<u32, Vec<usize>>,
    next_table_id: u32,
    txn: Option<Txn>,
}

impl MemStorage {
    pub fn new() -> Self {
        MemStorage::default()
    }

    fn allocate_table_id(&mut self) -> u32 {
        self.next_table_id += 1;
        self.next_table_id
    }
}

impl Storage for MemStorage {
    fn table(&self, name: &str) -> Option<TableDef> {
        if let Some(txn) = &self.txn {
            if let Some(def) = txn.new_tables.iter().rev().find(|t| t.name.eq_ignore_ascii_case(name)) {
                return Some(def.clone());
            }
        }
        self.names.get(&name.to_ascii_lowercase()).and_then(|id| self.tables.get(id).cloned())
    }

    fn table_by_id(&self, id: u32) -> Option<TableDef> {
        if let Some(txn) = &self.txn {
            if let Some(def) = txn.new_tables.iter().rev().find(|t| t.id == id) {
                return Some(def.clone());
            }
        }
        self.tables.get(&id).cloned()
    }

    fn list_tables(&self) -> Vec<TableDef> {
        let mut out: Vec<TableDef> = self.tables.values().cloned().collect();
        if let Some(txn) = &self.txn {
            out.extend(txn.new_tables.iter().cloned());
        }
        out.sort_by_key(|t| t.id);
        out
    }

    fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>, pk: usize) -> DbResult<TableDef> {
        let id = self.allocate_table_id();
        let def = TableDef { id, name: name.to_string(), columns, pk };
        if let Some(txn) = &mut self.txn {
            txn.new_tables.push(def.clone());
        } else {
            self.tables.insert(id, def.clone());
            self.names.insert(name.to_ascii_lowercase(), id);
            self.rows.entry(id).or_default();
        }
        Ok(def)
    }

    fn create_vector_index(&mut self, table_id: u32, column: usize) -> DbResult<()> {
        if let Some(txn) = &mut self.txn {
            txn.new_indexes.push((table_id, column));
        } else {
            let entry = self.indexes.entry(table_id).or_default();
            if !entry.contains(&column) {
                entry.push(column);
            }
        }
        Ok(())
    }

    fn vector_indexes(&self, table_id: u32) -> Vec<usize> {
        let mut out = self.indexes.get(&table_id).cloned().unwrap_or_default();
        if let Some(txn) = &self.txn {
            for (tid, col) in &txn.new_indexes {
                if *tid == table_id && !out.contains(col) {
                    out.push(*col);
                }
            }
        }
        out.sort_unstable();
        out
    }

    fn in_txn(&self) -> bool {
        self.txn.is_some()
    }

    fn begin(&mut self) -> DbResult<()> {
        if self.txn.is_some() {
            return Err(DbError::new(crate::types::ERR_TX_STATE, "transaction already active"));
        }
        let mut txn = Txn::default();
        txn.mark();
        self.txn = Some(txn);
        Ok(())
    }

    fn commit(&mut self) -> DbResult<()> {
        let txn = self
            .txn
            .take()
            .ok_or_else(|| DbError::new(crate::types::ERR_TX_STATE, "no active transaction"))?;
        for def in txn.new_tables {
            self.names.insert(def.name.to_ascii_lowercase(), def.id);
            self.rows.entry(def.id).or_default();
            self.tables.insert(def.id, def);
        }
        for (tid, col) in txn.new_indexes {
            let entry = self.indexes.entry(tid).or_default();
            if !entry.contains(&col) {
                entry.push(col);
            }
        }
        for ((tid, pk), value) in txn.writes {
            match value {
                Some(row) => {
                    self.rows.entry(tid).or_default().insert(pk, row);
                }
                None => {
                    if let Some(map) = self.rows.get_mut(&tid) {
                        map.remove(&pk);
                    }
                }
            }
        }
        Ok(())
    }

    fn rollback(&mut self) -> DbResult<()> {
        if self.txn.take().is_none() {
            return Err(DbError::new(crate::types::ERR_TX_STATE, "no active transaction"));
        }
        Ok(())
    }

    fn statement_mark(&mut self) {
        if let Some(txn) = &mut self.txn {
            txn.mark();
        }
    }

    fn rollback_statement(&mut self) -> DbResult<()> {
        let Some(txn) = &mut self.txn else { return Ok(()) };
        while txn.undo.len() > txn.undo_mark {
            let (tid, pk, prev) = txn.undo.pop().unwrap();
            match prev {
                None => {
                    txn.writes.remove(&(tid, pk));
                }
                Some(value) => {
                    txn.writes.insert((tid, pk), value);
                }
            }
        }
        txn.new_tables.truncate(txn.table_mark);
        txn.new_indexes.truncate(txn.index_mark);
        Ok(())
    }

    fn get(&self, table_id: u32, pk: i64) -> DbResult<Option<Row>> {
        if let Some(txn) = &self.txn {
            if let Some(value) = txn.writes.get(&(table_id, pk)) {
                return Ok(value.clone());
            }
        }
        Ok(self.rows.get(&table_id).and_then(|m| m.get(&pk)).cloned())
    }

    fn put(&mut self, table_id: u32, pk: i64, row: Row) -> DbResult<()> {
        if let Some(txn) = &mut self.txn {
            let key = (table_id, pk);
            let prev = txn.writes.get(&key).cloned();
            txn.undo.push((table_id, pk, prev));
            txn.writes.insert(key, Some(row));
        } else {
            self.rows.entry(table_id).or_default().insert(pk, row);
        }
        Ok(())
    }

    fn delete(&mut self, table_id: u32, pk: i64) -> DbResult<bool> {
        let existed = self.get(table_id, pk)?.is_some();
        if let Some(txn) = &mut self.txn {
            let key = (table_id, pk);
            let prev = txn.writes.get(&key).cloned();
            txn.undo.push((table_id, pk, prev));
            txn.writes.insert(key, None);
        } else if let Some(map) = self.rows.get_mut(&table_id) {
            map.remove(&pk);
        }
        Ok(existed)
    }

    fn scan(&self, table_id: u32, f: &mut dyn FnMut(i64, &Row) -> bool) -> DbResult<()> {
        let overlay: Vec<(i64, Option<Row>)> = match &self.txn {
            Some(txn) => {
                let mut items: Vec<(i64, Option<Row>)> = txn
                    .writes
                    .iter()
                    .filter(|((tid, _), _)| *tid == table_id)
                    .map(|((_, pk), v)| (*pk, v.clone()))
                    .collect();
                items.sort_by_key(|(pk, _)| *pk);
                items
            }
            None => Vec::new(),
        };
        let empty = BTreeMap::new();
        let base = self.rows.get(&table_id).unwrap_or(&empty);
        let mut base_iter = base.iter().peekable();
        let mut over_idx = 0usize;
        loop {
            let base_key = base_iter.peek().map(|(k, _)| **k);
            let over_key = overlay.get(over_idx).map(|(k, _)| *k);
            match (base_key, over_key) {
                (None, None) => break,
                (Some(_), None) => {
                    let (k, v) = base_iter.next().unwrap();
                    if !f(*k, v) {
                        break;
                    }
                }
                (None, Some(_)) => {
                    let (k, v) = overlay[over_idx].clone();
                    over_idx += 1;
                    if let Some(row) = v {
                        if !f(k, &row) {
                            break;
                        }
                    }
                }
                (Some(b), Some(o)) => {
                    if o < b {
                        let (k, v) = overlay[over_idx].clone();
                        over_idx += 1;
                        if let Some(row) = v {
                            if !f(k, &row) {
                                break;
                            }
                        }
                    } else if o > b {
                        let (k, v) = base_iter.next().unwrap();
                        if !f(*k, v) {
                            break;
                        }
                    } else {
                        let (k, v) = overlay[over_idx].clone();
                        over_idx += 1;
                        base_iter.next();
                        if let Some(row) = v {
                            if !f(k, &row) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> DbResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColType, Value};

    fn table_def() -> (Vec<ColumnDef>, usize) {
        (
            vec![
                ColumnDef { name: "id".into(), ty: ColType::BigInt, is_pk: true },
                ColumnDef { name: "v".into(), ty: ColType::Varchar(16), is_pk: false },
            ],
            0,
        )
    }

    #[test]
    fn autocommit_put_get_scan() {
        let mut s = MemStorage::new();
        let (cols, pk) = table_def();
        let def = s.create_table("t", cols, pk).unwrap();
        s.put(def.id, 2, vec![Value::Int(2), Value::Str("b".into())]).unwrap();
        s.put(def.id, 1, vec![Value::Int(1), Value::Str("a".into())]).unwrap();
        let mut seen = Vec::new();
        s.scan(def.id, &mut |k, _| {
            seen.push(k);
            true
        })
        .unwrap();
        assert_eq!(seen, vec![1, 2]);
    }

    #[test]
    fn txn_isolation_and_rollback() {
        let mut s = MemStorage::new();
        let (cols, pk) = table_def();
        let def = s.create_table("t", cols, pk).unwrap();
        s.put(def.id, 1, vec![Value::Int(1), Value::Str("a".into())]).unwrap();
        s.begin().unwrap();
        s.put(def.id, 1, vec![Value::Int(1), Value::Str("b".into())]).unwrap();
        assert_eq!(s.get(def.id, 1).unwrap().unwrap()[1], Value::Str("b".into()));
        s.rollback().unwrap();
        assert_eq!(s.get(def.id, 1).unwrap().unwrap()[1], Value::Str("a".into()));
    }

    #[test]
    fn statement_rollback_keeps_prior_txn_writes() {
        let mut s = MemStorage::new();
        let (cols, pk) = table_def();
        let def = s.create_table("t", cols, pk).unwrap();
        s.begin().unwrap();
        s.put(def.id, 1, vec![Value::Int(1), Value::Str("a".into())]).unwrap();
        s.statement_mark();
        s.put(def.id, 2, vec![Value::Int(2), Value::Str("x".into())]).unwrap();
        s.put(def.id, 1, vec![Value::Int(1), Value::Str("z".into())]).unwrap();
        s.rollback_statement().unwrap();
        assert!(s.get(def.id, 2).unwrap().is_none());
        assert_eq!(s.get(def.id, 1).unwrap().unwrap()[1], Value::Str("a".into()));
        s.commit().unwrap();
        assert_eq!(s.get(def.id, 1).unwrap().unwrap()[1], Value::Str("a".into()));
    }
}