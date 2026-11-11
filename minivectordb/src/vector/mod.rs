//! Vector value handling, distance functions and the ANN index interface.

use crate::types::{DbError, DbResult, ERR_DIMENSION_MISMATCH, ERR_INVALID_VECTOR};

/// Parse a vector literal such as "[1, 2.5, -3]" into f32 components.
pub fn parse_vector(text: &str) -> DbResult<Vec<f32>> {
    let trimmed = text.trim();
    let inner = match trimmed.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        Some(s) => s,
        None => return Err(DbError::new(ERR_INVALID_VECTOR, "vector literal must be bracketed with []")),
    };
    let inner = inner.trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for part in inner.split(',') {
        let piece = part.trim();
        if piece.is_empty() {
            return Err(DbError::new(ERR_INVALID_VECTOR, "empty vector component"));
        }
        let value: f64 = piece
            .parse()
            .map_err(|_| DbError::new(ERR_INVALID_VECTOR, format!("invalid vector component '{}'", piece)))?;
        if !value.is_finite() {
            return Err(DbError::new(ERR_INVALID_VECTOR, "vector components must be finite"));
        }
        let narrowed = value as f32;
        if !narrowed.is_finite() {
            return Err(DbError::new(ERR_INVALID_VECTOR, "vector component overflows float32"));
        }
        out.push(narrowed);
    }
    Ok(out)
}

/// Validate dimensionality of a stored/queried vector.
pub fn validate_dim(v: &[f32], dim: usize) -> DbResult<()> {
    if v.len() != dim {
        return Err(DbError::new(
            ERR_DIMENSION_MISMATCH,
            format!("expected dimension {}, got {}", dim, v.len()),
        ));
    }
    Ok(())
}

/// Plain Euclidean (L2) distance, computed in f64 over f32 components.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f64 {
    let mut sum = 0.0f64;
    let n = a.len().min(b.len());
    for i in 0..n {
        let d = (a[i] as f64) - (b[i] as f64);
        sum += d * d;
    }
    sum.sqrt()
}

/// An approximate nearest-neighbour index over float32 vectors.
pub trait AnnIndex: Send {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn add(&mut self, pk: i64, vector: &[f32]);
    fn remove(&mut self, pk: i64);
    fn clear(&mut self);
    /// Rebuild the whole index from (pk, vector) pairs.
    fn rebuild(&mut self, items: &[(i64, Vec<f32>)]);
    /// Return up to k nearest (pk, distance) pairs, ascending by distance then pk.
    fn search(&self, query: &[f32], k: usize) -> Vec<(i64, f64)>;
}

/// Exact brute-force index; always returns the true nearest neighbours.
#[derive(Default)]
pub struct FlatIndex {
    dim: usize,
    items: Vec<(i64, Vec<f32>)>,
}

impl FlatIndex {
    pub fn new(dim: usize) -> Self {
        FlatIndex { dim, items: Vec::new() }
    }
}

impl AnnIndex for FlatIndex {
    fn len(&self) -> usize {
        self.items.len()
    }

    fn add(&mut self, pk: i64, vector: &[f32]) {
        if self.dim == 0 {
            self.dim = vector.len();
        }
        if let Some(slot) = self.items.iter_mut().find(|(id, _)| *id == pk) {
            slot.1 = vector.to_vec();
        } else {
            self.items.push((pk, vector.to_vec()));
        }
    }

    fn remove(&mut self, pk: i64) {
        self.items.retain(|(id, _)| *id != pk);
    }

    fn clear(&mut self) {
        self.items.clear();
    }

    fn rebuild(&mut self, items: &[(i64, Vec<f32>)]) {
        self.items = items.to_vec();
        if let Some((_, v)) = items.first() {
            self.dim = v.len();
        }
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(i64, f64)> {
        let mut scored: Vec<(i64, f64)> = self
            .items
            .iter()
            .map(|(pk, v)| (*pk, l2_distance(v, query)))
            .collect();
        scored.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(k);
        scored
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn euclidean_basic() {
        assert!((l2_distance(&[3.0, 4.0], &[0.0, 0.0]) - 5.0).abs() < 1e-12);
    }

    #[test]
    fn float32_semantics() {
        let a = parse_vector("[16777217,0]").unwrap();
        let q = parse_vector("[16777216,0]").unwrap();
        // 16777217 is not representable in f32; it rounds to 16777216.
        assert_eq!(l2_distance(&a, &q), 0.0);
    }

    #[test]
    fn rejects_bad_vectors() {
        assert!(parse_vector("[1,]").is_err());
        assert!(parse_vector("1,2").is_err());
        assert!(parse_vector("[NaN,1]").is_err());
        assert!(parse_vector("[1e400]").is_err());
    }

    #[test]
    fn flat_search_ties_by_pk() {
        let mut idx = FlatIndex::new(2);
        idx.add(8, &[1.0, 0.0]);
        idx.add(3, &[-1.0, 0.0]);
        let r = idx.search(&[0.0, 0.0], 2);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, 3);
        assert_eq!(r[1].0, 8);
    }
}
