//! Fast exact top-k scan for L2 (Euclidean) vector search.
//!
//! The engine uses this module for the hot query path: given a flat row-major
//! vector buffer it returns the exact top-k rows by Euclidean distance. The
//! implementation is deliberately free of per-row allocation:
//!
//! * [`squared_l2`] widens f32 components to f64, subtracts in f64, squares and
//!   accumulates in f64 with eight independent partial sums (so the loop
//!   auto-vectorises), and never takes a square root.
//! * A bounded top-k collector keeps at most `k` candidates; the square root is
//!   taken only for the final `k` results.
//! * Rows are walked with `chunks_exact(dim)` so there is no bounds check or
//!   multiplication per row.
//! * Filtered scans run the same distance loop only over the candidate rows
//!   (either a `&[u32]` list of row indices or a [`BitSet`]).
//!
//! Distance semantics match [`crate::vector::l2_distance`]: f32 components are
//! widened to f64 before subtraction, accumulation is f64 and the square root is
//! taken last. Only the summation order differs (eight partial sums combined at
//! the end), which changes the result in the last f64 bits -- far below f32
//! precision -- so `sqrt(squared_l2(..))` matches `l2_distance(..)` to well
//! inside the 1e-5 comparison tolerance.
//!
//! Ordering is (distance ascending, id ascending), i.e. the same tie-break the
//! engine applies to `ORDER BY distance`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::vector::l2_distance;

/// Squared Euclidean distance between `a` and `b`.
///
/// Components are widened to f64 before subtraction, accumulated in f64 with
/// eight independent partial sums, and no square root is taken. For equal-length
/// slices this is `l2_distance(a, b)^2` up to f64 rounding of the summation
/// order; for unequal lengths the shorter length is used, matching
/// `l2_distance`.
#[inline(always)]
pub fn squared_l2(a: &[f32], b: &[f32]) -> f64 {
    let len = a.len().min(b.len());
    let mut sum0 = 0.0f64;
    let mut sum1 = 0.0f64;
    let mut sum2 = 0.0f64;
    let mut sum3 = 0.0f64;
    let mut sum4 = 0.0f64;
    let mut sum5 = 0.0f64;
    let mut sum6 = 0.0f64;
    let mut sum7 = 0.0f64;

    let mut left = a[..len].chunks_exact(8);
    let mut right = b[..len].chunks_exact(8);
    for (x, y) in left.by_ref().zip(right.by_ref()) {
        let d0 = x[0] as f64 - y[0] as f64;
        let d1 = x[1] as f64 - y[1] as f64;
        let d2 = x[2] as f64 - y[2] as f64;
        let d3 = x[3] as f64 - y[3] as f64;
        let d4 = x[4] as f64 - y[4] as f64;
        let d5 = x[5] as f64 - y[5] as f64;
        let d6 = x[6] as f64 - y[6] as f64;
        let d7 = x[7] as f64 - y[7] as f64;
        sum0 += d0 * d0;
        sum1 += d1 * d1;
        sum2 += d2 * d2;
        sum3 += d3 * d3;
        sum4 += d4 * d4;
        sum5 += d5 * d5;
        sum6 += d6 * d6;
        sum7 += d7 * d7;
    }

    let mut acc = ((sum0 + sum1) + (sum2 + sum3)) + ((sum4 + sum5) + (sum6 + sum7));
    let rest_left = left.remainder();
    let rest_right = right.remainder();
    for index in 0..rest_left.len() {
        let d = rest_left[index] as f64 - rest_right[index] as f64;
        acc += d * d;
    }
    acc
}

/// One candidate kept by the bounded top-k collector.
#[derive(Clone, Copy, Debug)]
struct Entry {
    dist_sq: f64,
    id: i64,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.dist_sq == other.dist_sq && self.id == other.id
    }
}

impl Eq for Entry {}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist_sq
            .partial_cmp(&other.dist_sq)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Bounded top-k collector: a max-heap holding at most `k` entries where the
/// root is the worst entry currently kept. The common case (row is worse than
/// the current threshold) is a single f64 comparison.
struct TopK {
    heap: BinaryHeap<Entry>,
    k: usize,
    worst_dist: f64,
    worst_id: i64,
}

impl TopK {
    #[inline]
    fn new(k: usize) -> Self {
        TopK {
            heap: BinaryHeap::with_capacity(k + 1),
            k,
            worst_dist: f64::INFINITY,
            worst_id: i64::MAX,
        }
    }

    #[inline(always)]
    fn offer(&mut self, dist_sq: f64, id: i64) {
        if self.heap.len() < self.k {
            self.heap.push(Entry { dist_sq, id });
            if self.heap.len() == self.k {
                let worst = self.heap.peek().expect("heap is non-empty");
                self.worst_dist = worst.dist_sq;
                self.worst_id = worst.id;
            }
            return;
        }
        if dist_sq > self.worst_dist || (dist_sq == self.worst_dist && id >= self.worst_id) {
            return;
        }
        self.heap.pop();
        self.heap.push(Entry { dist_sq, id });
        let worst = self.heap.peek().expect("heap is non-empty");
        self.worst_dist = worst.dist_sq;
        self.worst_id = worst.id;
    }

    /// Returns `(id, squared distance)` sorted by (distance, id) ascending.
    fn finish_sq(self) -> Vec<(i64, f64)> {
        let mut entries = self.heap.into_vec();
        entries.sort_unstable_by(|a, b| {
            a.dist_sq
                .partial_cmp(&b.dist_sq)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        entries.into_iter().map(|e| (e.id, e.dist_sq)).collect()
    }
}

#[inline]
fn finish_f32(scored: Vec<(i64, f64)>) -> Vec<(i64, f32)> {
    scored
        .into_iter()
        .map(|(id, dist_sq)| (id, dist_sq.sqrt() as f32))
        .collect()
}

#[inline]
fn finish_f64(scored: Vec<(i64, f64)>) -> Vec<(i64, f64)> {
    scored
        .into_iter()
        .map(|(id, dist_sq)| (id, dist_sq.sqrt()))
        .collect()
}

/// Exact top-k over every row, returning `(id, distance)` as f32.
///
/// * `vectors`: flat row-major buffer; row `i` is `vectors[i*dim..(i+1)*dim]`.
/// * `ids`: primary key of row `i`.
/// * `n`: number of valid rows (clamped to the buffer and `ids`).
/// * `query`: query vector; if its length differs from `dim`, the shorter length
///   is used, matching `l2_distance`.
///
/// Results are ordered by (distance ascending, id ascending). Returns an empty
/// vector for `k == 0`, `n == 0` or an empty buffer.
pub fn scan_topk(
    vectors: &[f32],
    ids: &[i64],
    n: usize,
    dim: usize,
    query: &[f32],
    k: usize,
) -> Vec<(i64, f32)> {
    finish_f32(scan_topk_sq(vectors, ids, n, dim, query, k))
}

/// Same as [`scan_topk`] but returns f64 distances.
///
/// Use this from the engine's exact mode when distances are compared or emitted
/// with f64 semantics (identical to the previous `l2_distance` path inside the
/// 1e-5 tolerance).
pub fn scan_topk_f64(
    vectors: &[f32],
    ids: &[i64],
    n: usize,
    dim: usize,
    query: &[f32],
    k: usize,
) -> Vec<(i64, f64)> {
    finish_f64(scan_topk_sq(vectors, ids, n, dim, query, k))
}

fn scan_topk_sq(
    vectors: &[f32],
    ids: &[i64],
    n: usize,
    dim: usize,
    query: &[f32],
    k: usize,
) -> Vec<(i64, f64)> {
    if k == 0 {
        return Vec::new();
    }
    let rows = n.min(ids.len());
    if dim == 0 {
        return zero_distance_topk(ids.iter().take(rows).copied(), k);
    }
    let rows = rows.min(vectors.len() / dim);
    if rows == 0 {
        return Vec::new();
    }
    let mut top = TopK::new(k);
    let body = &vectors[..rows * dim];
    for (row, &id) in body.chunks_exact(dim).zip(ids[..rows].iter()) {
        top.offer(squared_l2(row, query), id);
    }
    top.finish_sq()
}

/// Exact top-k restricted to the rows listed in `candidates` (row indices).
///
/// `candidates` is what the engine builds while applying a `WHERE` predicate;
/// only those rows are read and scored. Out-of-range indices are ignored.
pub fn scan_topk_indices(
    vectors: &[f32],
    ids: &[i64],
    dim: usize,
    query: &[f32],
    k: usize,
    candidates: &[u32],
) -> Vec<(i64, f32)> {
    finish_f32(scan_topk_indices_sq(vectors, ids, dim, query, k, candidates))
}

/// Same as [`scan_topk_indices`] but returns f64 distances.
pub fn scan_topk_indices_f64(
    vectors: &[f32],
    ids: &[i64],
    dim: usize,
    query: &[f32],
    k: usize,
    candidates: &[u32],
) -> Vec<(i64, f64)> {
    finish_f64(scan_topk_indices_sq(vectors, ids, dim, query, k, candidates))
}

fn scan_topk_indices_sq(
    vectors: &[f32],
    ids: &[i64],
    dim: usize,
    query: &[f32],
    k: usize,
    candidates: &[u32],
) -> Vec<(i64, f64)> {
    if k == 0 || candidates.is_empty() {
        return Vec::new();
    }
    if dim == 0 {
        return zero_distance_topk(
            candidates
                .iter()
                .filter_map(|&index| ids.get(index as usize).copied()),
            k,
        );
    }
    let mut top = TopK::new(k);
    for &index in candidates {
        let row = index as usize;
        if row >= ids.len() {
            continue;
        }
        let start = row * dim;
        let end = start + dim;
        if end > vectors.len() {
            continue;
        }
        top.offer(squared_l2(&vectors[start..end], query), ids[row]);
    }
    top.finish_sq()
}

/// Exact top-k restricted to the rows selected by a [`BitSet`].
pub fn scan_topk_masked(
    vectors: &[f32],
    ids: &[i64],
    dim: usize,
    query: &[f32],
    k: usize,
    mask: &BitSet,
) -> Vec<(i64, f32)> {
    finish_f32(scan_topk_masked_sq(vectors, ids, dim, query, k, mask))
}

/// Same as [`scan_topk_masked`] but returns f64 distances.
pub fn scan_topk_masked_f64(
    vectors: &[f32],
    ids: &[i64],
    dim: usize,
    query: &[f32],
    k: usize,
    mask: &BitSet,
) -> Vec<(i64, f64)> {
    finish_f64(scan_topk_masked_sq(vectors, ids, dim, query, k, mask))
}

fn scan_topk_masked_sq(
    vectors: &[f32],
    ids: &[i64],
    dim: usize,
    query: &[f32],
    k: usize,
    mask: &BitSet,
) -> Vec<(i64, f64)> {
    if k == 0 || mask.is_empty() {
        return Vec::new();
    }
    if dim == 0 {
        return zero_distance_topk(mask.iter().filter_map(|index| ids.get(index).copied()), k);
    }
    let mut top = TopK::new(k);
    for row in mask.iter() {
        if row >= ids.len() {
            continue;
        }
        let start = row * dim;
        let end = start + dim;
        if end > vectors.len() {
            continue;
        }
        top.offer(squared_l2(&vectors[start..end], query), ids[row]);
    }
    top.finish_sq()
}

/// `dim == 0` means every distance is 0.0, so the answer is simply the `k`
/// smallest ids.
fn zero_distance_topk<I: Iterator<Item = i64>>(ids: I, k: usize) -> Vec<(i64, f64)> {
    let mut selected: Vec<i64> = ids.collect();
    selected.sort_unstable();
    selected.truncate(k);
    selected.into_iter().map(|id| (id, 0.0)).collect()
}

/// Reference (slow) implementation of [`scan_topk`]: one [`l2_distance`] call per
/// row plus a full sort of all rows. Intended for differential testing of the
/// fast path; returns f64 distances.
pub fn naive_scan_topk(
    vectors: &[f32],
    ids: &[i64],
    n: usize,
    dim: usize,
    query: &[f32],
    k: usize,
) -> Vec<(i64, f64)> {
    if k == 0 {
        return Vec::new();
    }
    let rows = n.min(ids.len());
    if dim == 0 {
        return zero_distance_topk(ids.iter().take(rows).copied(), k);
    }
    let rows = rows.min(vectors.len() / dim);
    let mut scored: Vec<(i64, f64)> = Vec::with_capacity(rows);
    for row in 0..rows {
        let start = row * dim;
        scored.push((ids[row], l2_distance(&vectors[start..start + dim], query)));
    }
    sort_and_truncate(&mut scored, k);
    scored
}

/// Reference (slow) implementation of [`scan_topk_indices`], for differential
/// testing of the filtered fast path.
pub fn naive_scan_topk_indices(
    vectors: &[f32],
    ids: &[i64],
    dim: usize,
    query: &[f32],
    k: usize,
    candidates: &[u32],
) -> Vec<(i64, f64)> {
    if k == 0 || candidates.is_empty() {
        return Vec::new();
    }
    if dim == 0 {
        return zero_distance_topk(
            candidates
                .iter()
                .filter_map(|&index| ids.get(index as usize).copied()),
            k,
        );
    }
    let mut scored: Vec<(i64, f64)> = Vec::with_capacity(candidates.len());
    for &index in candidates {
        let row = index as usize;
        if row >= ids.len() {
            continue;
        }
        let start = row * dim;
        let end = start + dim;
        if end > vectors.len() {
            continue;
        }
        scored.push((ids[row], l2_distance(&vectors[start..end], query)));
    }
    sort_and_truncate(&mut scored, k);
    scored
}

fn sort_and_truncate(scored: &mut Vec<(i64, f64)>, k: usize) {
    scored.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scored.truncate(k);
}

/// Compact bitset over row indices, used to mark the rows that survive a
/// `WHERE` predicate.
///
/// The engine can call [`BitSet::set`] while it scans rows for a filter and then
/// hand the mask to [`scan_topk_masked`], avoiding an intermediate id list.
#[derive(Clone, Debug, Default)]
pub struct BitSet {
    words: Vec<u64>,
    len: usize,
}

impl BitSet {
    /// Creates an empty mask with room for `len` row indices.
    pub fn new(len: usize) -> Self {
        BitSet {
            words: vec![0u64; len.div_ceil(64)],
            len,
        }
    }

    /// Number of row indices the mask can represent.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clears every bit, keeping the allocation for reuse.
    pub fn clear(&mut self) {
        for word in &mut self.words {
            *word = 0;
        }
    }

    /// Marks `index`; out-of-range indices are ignored.
    pub fn set(&mut self, index: usize) {
        if index < self.len {
            self.words[index >> 6] |= 1u64 << (index & 63);
        }
    }

    /// Returns whether `index` is marked.
    pub fn get(&self, index: usize) -> bool {
        index < self.len && (self.words[index >> 6] >> (index & 63)) & 1 == 1
    }

    /// Number of marked indices.
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|word| word.count_ones() as usize).sum()
    }

    /// Iterates the marked indices in ascending order.
    pub fn iter(&self) -> BitIter<'_> {
        BitIter::new(&self.words)
    }
}

/// Iterator over the set bits of a [`BitSet`].
pub struct BitIter<'a> {
    words: &'a [u64],
    word: usize,
    bits: u64,
}

impl<'a> BitIter<'a> {
    fn new(words: &'a [u64]) -> Self {
        BitIter {
            words,
            word: 0,
            bits: words.first().copied().unwrap_or(0),
        }
    }
}

impl Iterator for BitIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        loop {
            if self.bits != 0 {
                let bit = self.bits.trailing_zeros() as usize;
                self.bits &= self.bits - 1;
                return Some(self.word * 64 + bit);
            }
            self.word += 1;
            if self.word >= self.words.len() {
                return None;
            }
            self.bits = self.words[self.word];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// Small deterministic xorshift64* RNG (no external crates).
    struct Lcg(u64);

    impl Lcg {
        fn new(seed: u64) -> Self {
            Lcg(seed | 1)
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        /// Uniform in [-1, 1).
        fn next_f32(&mut self) -> f32 {
            let bits = (self.next_u64() >> 40) as u32;
            (bits as f32 / (1u32 << 24) as f32) * 2.0 - 1.0
        }
    }

    fn random_dataset(seed: u64, n: usize, dim: usize) -> (Vec<f32>, Vec<i64>) {
        let mut rng = Lcg::new(seed);
        let mut vectors = Vec::with_capacity(n * dim);
        for _ in 0..n * dim {
            vectors.push(rng.next_f32());
        }
        let ids = (1..=n as i64).collect();
        (vectors, ids)
    }

    fn assert_same_ids(got: &[(i64, f32)], want: &[(i64, f64)]) {
        let got_ids: Vec<i64> = got.iter().map(|(id, _)| *id).collect();
        let want_ids: Vec<i64> = want.iter().map(|(id, _)| *id).collect();
        assert_eq!(got_ids, want_ids);
    }

    fn assert_distances_close(got: &[(i64, f32)], want: &[(i64, f64)]) {
        for ((_, got_distance), (_, want_distance)) in got.iter().zip(want.iter()) {
            let got = *got_distance as f64;
            let tolerance = 1e-5 * want_distance.abs().max(1.0);
            assert!(
                (got - want_distance).abs() <= tolerance,
                "distance mismatch: got {} want {}",
                got,
                want_distance
            );
        }
    }

    #[test]
    fn squared_l2_matches_l2_distance() {
        let mut rng = Lcg::new(0x5151_5151);
        for dim in [1usize, 2, 7, 8, 9, 64, 127, 128, 200] {
            let a: Vec<f32> = (0..dim).map(|_| rng.next_f32() * 100.0).collect();
            let b: Vec<f32> = (0..dim).map(|_| rng.next_f32() * 100.0).collect();
            let want = l2_distance(&a, &b);
            let got = squared_l2(&a, &b).sqrt();
            let tolerance = 1e-12 * want.abs().max(1.0);
            assert!(
                (got - want).abs() <= tolerance,
                "dim {}: squared_l2 sqrt {} vs l2_distance {}",
                dim,
                got,
                want
            );
        }
    }

    #[test]
    fn float32_semantics_for_large_coordinates() {
        // 16777217 is not representable in f32 and rounds to 16777216, exactly
        // like the stored vector in the public vector_float32 case.
        let stored: Vec<f32> = vec![16777217.0, 0.0];
        let query: Vec<f32> = vec![16777216.0, 0.0];
        assert_eq!(squared_l2(&stored, &query), 0.0);
        assert_eq!(squared_l2(&stored, &query).sqrt() as f32, 0.0);
    }

    #[test]
    fn ties_are_broken_by_id() {
        // Four points at distance 2 plus one closer point.
        let vectors: Vec<f32> = vec![
            2.0, 0.0, // id 5
            -2.0, 0.0, // id 1
            0.0, 2.0, // id 9
            0.0, -2.0, // id 3
            1.0, 0.0, // id 7
        ];
        let ids = vec![5i64, 1, 9, 3, 7];
        let got = scan_topk(&vectors, &ids, 5, 2, &[0.0, 0.0], 3);
        assert_eq!(got, vec![(7, 1.0), (1, 2.0), (3, 2.0)]);
        let all = scan_topk(&vectors, &ids, 5, 2, &[0.0, 0.0], 10);
        assert_eq!(all.len(), 5);
        assert_eq!(
            all,
            vec![(7, 1.0), (1, 2.0), (3, 2.0), (5, 2.0), (9, 2.0)]
        );
    }

    #[test]
    fn matches_naive_scan() {
        let (vectors, ids) = random_dataset(0x1234_9876, 400, 16);
        let mut rng = Lcg::new(0xCAFE_BABE);
        for _ in 0..20 {
            let query: Vec<f32> = (0..16).map(|_| rng.next_f32()).collect();
            let want = naive_scan_topk(&vectors, &ids, 400, 16, &query, 10);
            let got = scan_topk(&vectors, &ids, 400, 16, &query, 10);
            assert_eq!(got.len(), 10);
            assert_same_ids(&got, &want);
            assert_distances_close(&got, &want);

            let got_f64 = scan_topk_f64(&vectors, &ids, 400, 16, &query, 10);
            assert_same_ids(&got, &want);
            for ((_, got_distance), (_, want_distance)) in got_f64.iter().zip(want.iter()) {
                assert!((got_distance - want_distance).abs() <= 1e-12 * want_distance.max(1.0));
            }
        }
    }

    #[test]
    fn filtered_matches_naive_subset() {
        let (vectors, ids) = random_dataset(0xDEAD_1234, 300, 8);
        let candidates: Vec<u32> = (0..300u32).filter(|index| index % 3 != 1).collect();
        let mut rng = Lcg::new(0x7777_8888);
        for _ in 0..10 {
            let query: Vec<f32> = (0..8).map(|_| rng.next_f32()).collect();
            let want = naive_scan_topk_indices(&vectors, &ids, 8, &query, 10, &candidates);

            let got = scan_topk_indices(&vectors, &ids, 8, &query, 10, &candidates);
            assert_eq!(got.len(), 10);
            assert_same_ids(&got, &want);
            assert_distances_close(&got, &want);

            let got_f64 = scan_topk_indices_f64(&vectors, &ids, 8, &query, 10, &candidates);
            assert_same_ids(&got, &want);
        }
    }

    #[test]
    fn bitset_matches_index_list() {
        let (vectors, ids) = random_dataset(0x4242_4242, 250, 12);
        let mut mask = BitSet::new(250);
        for index in 0..250usize {
            if index % 5 != 2 {
                mask.set(index);
            }
        }
        let candidates: Vec<u32> = mask.iter().map(|index| index as u32).collect();
        assert_eq!(mask.count_ones(), candidates.len());

        let mut rng = Lcg::new(0x1111_2222);
        for _ in 0..10 {
            let query: Vec<f32> = (0..12).map(|_| rng.next_f32()).collect();
            let by_mask = scan_topk_masked(&vectors, &ids, 12, &query, 10, &mask);
            let by_indices = scan_topk_indices(&vectors, &ids, 12, &query, 10, &candidates);
            assert_eq!(by_mask, by_indices);
        }
    }

    #[test]
    fn edge_cases() {
        let (vectors, ids) = random_dataset(9, 20, 4);

        // k == 0
        assert!(scan_topk(&vectors, &ids, 20, 4, &[0.0; 4], 0).is_empty());
        // k larger than the dataset returns every row sorted.
        let all = scan_topk(&vectors, &ids, 20, 4, &[0.0; 4], 1000);
        assert_eq!(all.len(), 20);
        // empty dataset
        assert!(scan_topk(&vectors, &ids, 0, 4, &[0.0; 4], 10).is_empty());
        // n larger than the available rows is clamped
        assert_eq!(scan_topk(&vectors, &ids, 9999, 4, &[0.0; 4], 5).len(), 5);
        // dim == 0: every distance is zero, ties resolved by id
        let zero = scan_topk(&vectors, &ids, 20, 0, &[], 3);
        assert_eq!(zero, vec![(1, 0.0), (2, 0.0), (3, 0.0)]);
        // query shorter than dim: l2_distance truncates to the shorter length
        let short_query = [0.5f32, 0.25];
        let want = {
            let mut scored: Vec<(i64, f64)> = ids
                .iter()
                .enumerate()
                .map(|(row, &id)| {
                    let start = row * 4;
                    (id, l2_distance(&vectors[start..start + 4], &short_query))
                })
                .collect();
            sort_and_truncate(&mut scored, 5);
            scored
        };
        let got = scan_topk(&vectors, &ids, 20, 4, &short_query, 5);
        assert_same_ids(&got, &want);
        // filtered scan with no candidates
        assert!(scan_topk_indices(&vectors, &ids, 4, &[0.0; 4], 5, &[]).is_empty());
        assert!(scan_topk_masked(&vectors, &ids, 4, &[0.0; 4], 5, &BitSet::new(20)).is_empty());
        // out-of-range candidate rows are ignored
        let out_of_range = [0u32, 500, 3];
        assert_eq!(
            scan_topk_indices(&vectors, &ids, 4, &[0.0; 4], 5, &out_of_range),
            scan_topk_indices(&vectors, &ids, 4, &[0.0; 4], 5, &[0u32, 3])
        );
    }

    #[test]
    fn bitset_basics() {
        let mut mask = BitSet::new(130);
        assert_eq!(mask.len(), 130);
        assert_eq!(mask.count_ones(), 0);
        mask.set(0);
        mask.set(63);
        mask.set(64);
        mask.set(129);
        mask.set(500); // out of range: ignored
        assert!(mask.get(0));
        assert!(mask.get(63));
        assert!(mask.get(64));
        assert!(mask.get(129));
        assert!(!mask.get(65));
        assert!(!mask.get(500));
        assert_eq!(mask.count_ones(), 4);
        assert_eq!(mask.iter().collect::<Vec<_>>(), vec![0usize, 63, 64, 129]);
        mask.clear();
        assert_eq!(mask.count_ones(), 0);
        assert!(mask.iter().next().is_none());
    }

    /// 10k x 128, 1000 queries, K=10; unfiltered and 50%-filtered, compared
    /// against the naive `l2_distance` + full-sort reference.
    ///
    /// Run with:
    /// `cargo test --release -- --ignored --nocapture perf_10k_128`
    #[test]
    #[ignore = "performance benchmark (release build only)"]
    fn perf_10k_128_1000_queries() {
        const N: usize = 10_000;
        const DIM: usize = 128;
        const QUERIES: usize = 1_000;
        const K: usize = 10;
        const NAIVE_QUERIES: usize = 100; // naive reference is only timed on a subset

        let mut rng = Lcg::new(0x0BAD_F00D_1234_5678);
        let mut vectors = Vec::with_capacity(N * DIM);
        for _ in 0..N * DIM {
            vectors.push(rng.next_f32());
        }
        let ids: Vec<i64> = (1..=N as i64).collect();
        let queries: Vec<Vec<f32>> = (0..QUERIES)
            .map(|_| (0..DIM).map(|_| rng.next_f32()).collect())
            .collect();

        // ---- unfiltered: fastscan ----------------------------------------
        let start = Instant::now();
        let mut checksum = 0i64;
        for query in &queries {
            let got = scan_topk(&vectors, &ids, N, DIM, query, K);
            assert_eq!(got.len(), K);
            checksum = checksum.wrapping_add(got[0].0);
        }
        let fast_unfiltered = start.elapsed();

        // ---- unfiltered: f64 variant -------------------------------------
        let start = Instant::now();
        for query in &queries {
            let got = scan_topk_f64(&vectors, &ids, N, DIM, query, K);
            checksum = checksum.wrapping_add(got[0].0);
        }
        let fast_unfiltered_f64 = start.elapsed();

        // ---- unfiltered: naive reference ---------------------------------
        let start = Instant::now();
        for query in queries.iter().take(NAIVE_QUERIES) {
            let got = naive_scan_topk(&vectors, &ids, N, DIM, query, K);
            checksum = checksum.wrapping_add(got[0].0);
        }
        let naive_unfiltered_subset = start.elapsed();
        let naive_unfiltered =
            naive_unfiltered_subset.as_secs_f64() * QUERIES as f64 / NAIVE_QUERIES as f64;

        // ---- 50% filtered -------------------------------------------------
        let mut mask = BitSet::new(N);
        for index in (0..N).step_by(2) {
            mask.set(index);
        }
        assert_eq!(mask.count_ones(), N / 2);
        let candidates: Vec<u32> = mask.iter().map(|index| index as u32).collect();

        let start = Instant::now();
        for query in &queries {
            let got = scan_topk_masked(&vectors, &ids, DIM, query, K, &mask);
            assert_eq!(got.len(), K);
            checksum = checksum.wrapping_add(got[0].0);
        }
        let fast_filtered = start.elapsed();

        let start = Instant::now();
        for query in &queries {
            let got = scan_topk_indices(&vectors, &ids, DIM, query, K, &candidates);
            assert_eq!(got.len(), K);
            checksum = checksum.wrapping_add(got[0].0);
        }
        let fast_filtered_indices = start.elapsed();

        let start = Instant::now();
        for query in queries.iter().take(NAIVE_QUERIES) {
            let got = naive_scan_topk_indices(&vectors, &ids, DIM, query, K, &candidates);
            checksum = checksum.wrapping_add(got[0].0);
        }
        let naive_filtered_subset = start.elapsed();
        let naive_filtered =
            naive_filtered_subset.as_secs_f64() * QUERIES as f64 / NAIVE_QUERIES as f64;

        // ---- correctness spot check against the naive reference -----------
        for query in queries.iter().take(10) {
            let want = naive_scan_topk(&vectors, &ids, N, DIM, query, K);
            let got = scan_topk(&vectors, &ids, N, DIM, query, K);
            assert_same_ids(&got, &want);
            assert_distances_close(&got, &want);
            let want_filtered = naive_scan_topk_indices(&vectors, &ids, DIM, query, K, &candidates);
            let got_filtered = scan_topk_masked(&vectors, &ids, DIM, query, K, &mask);
            assert_same_ids(&got_filtered, &want_filtered);
            assert_distances_close(&got_filtered, &want_filtered);
        }
        assert_ne!(checksum, i64::MIN, "keep results observable");

        let per_query_us = |total: std::time::Duration| total.as_secs_f64() * 1e6 / QUERIES as f64;
        let bytes_per_query = (N * DIM * 4) as f64;

        println!(
            "== fastscan benchmark: 10k x 128, {} queries, K={} ==",
            QUERIES, K
        );
        println!(
            "unfiltered : total={:?}  per-query={:.1} us  ({:.1} GB/s effective)  \
             naive(100q)={:?} -> ~{:.0} ms/1000q  speedup={:.2}x",
            fast_unfiltered,
            per_query_us(fast_unfiltered),
            bytes_per_query * QUERIES as f64 / fast_unfiltered.as_secs_f64() / 1e9,
            naive_unfiltered_subset,
            naive_unfiltered * 1e3,
            naive_unfiltered / fast_unfiltered.as_secs_f64(),
        );
        println!(
            "unfiltered f64 variant: total={:?}  per-query={:.1} us",
            fast_unfiltered_f64,
            per_query_us(fast_unfiltered_f64),
        );
        println!(
            "filtered 50% (bitset)  : total={:?}  per-query={:.1} us  speedup vs naive={:.2}x",
            fast_filtered,
            per_query_us(fast_filtered),
            naive_filtered / fast_filtered.as_secs_f64(),
        );
        println!(
            "filtered 50% (indices) : total={:?}  per-query={:.1} us",
            fast_filtered_indices,
            per_query_us(fast_filtered_indices),
        );
        println!(
            "filtered naive(100q)   : {:?} -> ~{:.0} ms/1000q",
            naive_filtered_subset,
            naive_filtered * 1e3,
        );

        // Targets: exact results, single unfiltered query well under 300 us and
        // clearly faster than the naive reference.
        assert!(
            fast_unfiltered.as_secs_f64() < naive_unfiltered,
            "fastscan is not faster than naive"
        );
        assert!(
            per_query_us(fast_unfiltered) < 300.0,
            "unfiltered per-query latency too high: {:.1} us",
            per_query_us(fast_unfiltered)
        );
    }
}
