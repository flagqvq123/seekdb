//! In-memory hot cache for one table: flat vector column, decoded rows and
//! equality dictionaries, used by the fast exact TopK path.
//!
//! Building the cache costs one sequential storage scan. It is invalidated by
//! mutations (see Engine.dirty_tables) and skipped when the table is too large
//! for the memory budget, in which case the engine falls back to the streaming
//! slow path.

use std::collections::HashMap;

use crate::storage::Storage;
use crate::types::{DbResult, Row, TableDef, Value};

/// Bytes of vector payload we are willing to keep resident for hot queries.
pub const DEFAULT_CACHE_BUDGET_BYTES: usize = 256 * 1024 * 1024;

#[derive(PartialEq, Eq, Hash)]
enum EqKey {
    Int(i64),
    Str(String),
}

/// Row-major snapshot of one table used by vector TopK queries.
pub struct TableCache {
    pub table_id: u32,
    pub vector_col: usize,
    pub dim: usize,
    pub ids: Vec<i64>,
    /// Row-major float32 vectors (row i spans [i*dim, (i+1)*dim)).
    pub flat: Vec<f32>,
    pub pk_to_idx: HashMap<i64, u32>,
    /// Decoded rows in the same order as ids; the vector column holds NULL to
    /// avoid duplicating the flat vector payload.
    pub rows: Vec<Row>,
    eq: HashMap<usize, HashMap<EqKey, Vec<u32>>>,
    pub bytes: usize,
}

impl TableCache {
    /// Build the cache when the table fits the budget; None means "too large".
    pub fn build(
        storage: &dyn Storage,
        table: &TableDef,
        vector_col: usize,
        budget: usize,
    ) -> DbResult<Option<TableCache>> {
        let dim = match table.columns[vector_col].ty.vector_dim() {
            Some(dim) => dim,
            None => return Ok(None),
        };
        let ncols = table.columns.len();
        let mut ids: Vec<i64> = Vec::new();
        let mut flat: Vec<f32> = Vec::new();
        let mut rows: Vec<Row> = Vec::new();
        let mut bytes = 0usize;
        let mut overflow = false;
        storage.scan(table.id, &mut |pk, row| {
            let mut cached = Vec::with_capacity(ncols);
            for (idx, value) in row.iter().enumerate() {
                if idx == vector_col {
                    match value {
                        Value::Vec(components) => flat.extend_from_slice(components),
                        _ => flat.extend(std::iter::repeat(0.0).take(dim)),
                    }
                    cached.push(Value::Null);
                } else {
                    bytes += value_bytes(value);
                    cached.push(value.clone());
                }
            }
            ids.push(pk);
            rows.push(cached);
            bytes += 32 + dim * 4;
            if bytes > budget {
                overflow = true;
                return false;
            }
            true
        })?;
        if overflow {
            return Ok(None);
        }
        let mut eq: HashMap<usize, HashMap<EqKey, Vec<u32>>> = HashMap::new();
        for (col_idx, _) in table.columns.iter().enumerate() {
            if col_idx == vector_col {
                continue;
            }
            let mut map: HashMap<EqKey, Vec<u32>> = HashMap::new();
            for (row_idx, row) in rows.iter().enumerate() {
                let key = match &row[col_idx] {
                    Value::Int(v) => EqKey::Int(*v),
                    Value::Str(s) => EqKey::Str(s.clone()),
                    _ => continue,
                };
                map.entry(key).or_default().push(row_idx as u32);
            }
            if !map.is_empty() {
                eq.insert(col_idx, map);
            }
        }
        let mut pk_to_idx = HashMap::with_capacity(ids.len());
        for (idx, pk) in ids.iter().enumerate() {
            pk_to_idx.insert(*pk, idx as u32);
        }
        Ok(Some(TableCache {
            table_id: table.id,
            vector_col,
            dim,
            ids,
            flat,
            pk_to_idx,
            rows,
            eq,
            bytes,
        }))
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Exact TopK over the cached vectors, optionally restricted to candidates.
    pub fn topk(&self, query: &[f32], k: usize, candidates: Option<&[u32]>) -> Vec<(i64, f64)> {
        if k == 0 || self.ids.is_empty() {
            return Vec::new();
        }
        match candidates {
            Some(cands) => crate::fastscan::scan_topk_indices_f64(
                &self.flat,
                &self.ids,
                self.dim,
                query,
                k,
                cands,
            ),
            None => crate::fastscan::scan_topk_f64(
                &self.flat,
                &self.ids,
                self.ids.len(),
                self.dim,
                query,
                k,
            ),
        }
    }

    /// Rows whose column equals the given literal, via the equality dictionary.
    /// None means the column/value combination is not indexed (caller falls back).
    pub fn lookup_eq(&self, column: usize, value: &Value) -> Option<Vec<u32>> {
        let map = self.eq.get(&column)?;
        let key = match value {
            Value::Int(v) => EqKey::Int(*v),
            Value::Str(s) => EqKey::Str(s.clone()),
            _ => return None,
        };
        Some(map.get(&key).cloned().unwrap_or_default())
    }
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 8,
        Value::Int(_) => 16,
        Value::Float(_) => 16,
        Value::Str(s) => 24 + s.len(),
        Value::Vec(v) => 24 + v.len() * 4,
    }
}
