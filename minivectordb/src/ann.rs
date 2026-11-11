//! Approximate nearest-neighbour vector index (HNSW).
//!
//! The index implements [`crate::vector::AnnIndex`] with a Hierarchical
//! Navigable Small World graph and uses `std` only (no external crates, no
//! `unsafe`).
//!
//! Behaviour contract implemented here:
//! * Distances are plain Euclidean distances from [`crate::vector::l2_distance`]
//!   (f32 components accumulated in f64), so reported values match a
//!   brute-force scan.
//! * Results are ordered by (distance ascending, pk ascending) and never
//!   contain duplicate pks; deleted entries are never returned.
//! * While the live set is small (`HnswParams::exact_scan_limit`, default 4096)
//!   `search` performs an exact scan, which keeps the small public VECTOR(2)
//!   cases identical to brute force.
//! * `add` is incremental (never rebuilds the whole graph); `remove` tombstones
//!   the graph node so it can still be used for routing but is never returned.
//!   Re-adding an existing pk tombstones the old node first, so a pk is never
//!   returned twice. If tombstones come to dominate, the index compacts itself
//!   inside `add`.
//!
//! Vectors are kept in one flat `Vec<f32>` (row-major) so distance loops walk
//! contiguous memory; node ids index both `nodes` and `vectors`.

use std::cell::RefCell;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use crate::vector::{l2_distance, AnnIndex};

/// Hard cap on the random level of a graph node (sanity bound only).
const MAX_LEVEL: usize = 24;

/// Tunable HNSW parameters.
///
/// Defaults target the competition workload (50k x 64 dims): recall@10 well
/// above 0.95, build well under 10 s and single-query latency well under 5 ms.
#[derive(Clone, Copy, Debug)]
pub struct HnswParams {
    /// Maximum number of neighbours per node above level 0.
    pub m: usize,
    /// Maximum number of neighbours per node at level 0.
    pub m0: usize,
    /// Candidate list size used while building the graph.
    pub ef_construction: usize,
    /// Candidate list size used while answering a query.
    pub ef_search: usize,
    /// Live cardinality at or below which `search` runs an exact scan.
    pub exact_scan_limit: usize,
    /// Seed for the deterministic level RNG.
    pub seed: u64,
}

impl Default for HnswParams {
    fn default() -> Self {
        HnswParams {
            m: 16,
            m0: 32,
            ef_construction: 96,
            ef_search: 128,
            exact_scan_limit: 4096,
            seed: 0x9E37_79B9_7F4A_7C15,
        }
    }
}

impl HnswParams {
    fn sanitized(mut self) -> Self {
        self.m = self.m.clamp(2, 128);
        self.m0 = self.m0.max(self.m);
        self.ef_construction = self.ef_construction.max(self.m).max(1);
        self.ef_search = self.ef_search.max(1);
        if self.seed == 0 {
            self.seed = 0x9E37_79B9_7F4A_7C15;
        }
        self
    }
}

/// One graph node. `links[level]` holds node ids of neighbours at `level`.
struct Node {
    pk: i64,
    level: u8,
    deleted: bool,
    links: Vec<Vec<u32>>,
}

/// Deterministic xorshift64* RNG; used only for level generation.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    #[inline]
    fn next_f64(&mut self) -> f64 {
        // 53 random mantissa bits -> uniform in [0, 1).
        (self.next_u64() >> 11) as f64 * (1.0 / ((1u64 << 53) as f64))
    }
}

/// Reusable visited-set scratch space: an epoch-stamped array avoids clearing
/// a `HashSet` on every graph walk.
#[derive(Default)]
struct Scratch {
    visited: Vec<u32>,
    epoch: u32,
}

impl Scratch {
    fn begin(&mut self, nodes: usize) {
        if self.visited.len() < nodes {
            self.visited.resize(nodes, 0);
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            for slot in self.visited.iter_mut() {
                *slot = 0;
            }
            self.epoch = 1;
        }
    }

    /// Marks `id` visited for the current epoch; returns `true` the first time.
    #[inline]
    fn mark(&mut self, id: u32) -> bool {
        let slot = &mut self.visited[id as usize];
        if *slot == self.epoch {
            false
        } else {
            *slot = self.epoch;
            true
        }
    }
}

/// Distance with a node id, ordered by distance ascending then id ascending.
/// `BinaryHeap<Dist>` is therefore a max-heap by distance, and
/// `BinaryHeap<Reverse<Dist>>` a min-heap by distance.
#[derive(Clone, Copy, Debug)]
struct Dist(f64, u32);

impl PartialEq for Dist {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for Dist {}

impl Ord for Dist {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.1.cmp(&other.1))
    }
}

impl PartialOrd for Dist {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[inline]
fn dist_cmp(a: (f64, u32), b: (f64, u32)) -> Ordering {
    a.0.partial_cmp(&b.0)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.1.cmp(&b.1))
}

#[inline]
fn vector_slice(vectors: &[f32], dim: usize, id: u32) -> &[f32] {
    let start = id as usize * dim;
    &vectors[start..start + dim]
}

/// HNSW approximate nearest-neighbour index over f32 vectors.
pub struct HnswIndex {
    params: HnswParams,
    /// Vector dimensionality; fixed on first `rebuild`/`add`.
    dim: usize,
    /// Flat row-major vectors: node `id` occupies `[id * dim, (id + 1) * dim)`.
    vectors: Vec<f32>,
    nodes: Vec<Node>,
    entry: Option<u32>,
    max_level: u8,
    /// Maps a live pk to its graph node id (deleted nodes are not present).
    pk_to_node: HashMap<i64, u32>,
    /// Number of live (non-deleted) nodes.
    live: usize,
    rng: Rng,
    scratch: RefCell<Scratch>,
}

impl Default for HnswIndex {
    fn default() -> Self {
        HnswIndex::new(0)
    }
}

impl HnswIndex {
    /// Creates an index for `dim`-dimensional vectors. `dim == 0` means "take
    /// the dimensionality from the first vector added or rebuilt".
    pub fn new(dim: usize) -> Self {
        Self::with_params(dim, HnswParams::default())
    }

    /// Creates an index with explicit tuning parameters (useful for tests).
    pub fn with_params(dim: usize, params: HnswParams) -> Self {
        let params = params.sanitized();
        HnswIndex {
            params,
            dim,
            vectors: Vec::new(),
            nodes: Vec::new(),
            entry: None,
            max_level: 0,
            pk_to_node: HashMap::new(),
            live: 0,
            rng: Rng::new(params.seed),
            scratch: RefCell::new(Scratch::default()),
        }
    }

    /// Configured dimensionality (0 until the first vector is added).
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Number of graph nodes, including tombstones (diagnostics only).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    fn max_links(&self, level: usize) -> usize {
        if level == 0 {
            self.params.m0
        } else {
            self.params.m
        }
    }

    #[inline]
    fn dist_to(&self, query: &[f32], id: u32) -> f64 {
        l2_distance(vector_slice(&self.vectors, self.dim, id), query)
    }

    #[inline]
    fn dist_to_node(&self, a: u32, b: u32) -> f64 {
        l2_distance(
            vector_slice(&self.vectors, self.dim, a),
            vector_slice(&self.vectors, self.dim, b),
        )
    }

    fn random_level(&mut self) -> u8 {
        let ml = 1.0 / (self.params.m.max(2) as f64).ln();
        let u = self.rng.next_f64().max(1e-12);
        let level = (-u.ln() * ml).floor() as usize;
        level.min(MAX_LEVEL) as u8
    }

    /// Greedy descent from `from_level` down to `to_level` (inclusive); returns
    /// the closest node found and its distance to the query.
    fn greedy_search(
        &self,
        query: &[f32],
        entry: u32,
        from_level: usize,
        to_level: usize,
    ) -> (u32, f64) {
        let mut current = entry;
        let mut current_distance = self.dist_to(query, current);
        let mut level = from_level as isize;
        let stop = to_level as isize;
        while level >= stop {
            loop {
                let mut improved = false;
                let links = &self.nodes[current as usize].links[level as usize];
                for index in 0..links.len() {
                    let neighbour = links[index];
                    let distance = self.dist_to(query, neighbour);
                    if distance < current_distance {
                        current = neighbour;
                        current_distance = distance;
                        improved = true;
                    }
                }
                if !improved {
                    break;
                }
            }
            level -= 1;
        }
        (current, current_distance)
    }

    /// Beam search on one graph layer. Returns the `ef` closest
    /// `(distance, node id)` pairs found (unordered).
    fn search_layer(
        vectors: &[f32],
        nodes: &[Node],
        dim: usize,
        query: &[f32],
        entry: u32,
        ef: usize,
        level: usize,
        scratch: &mut Scratch,
    ) -> Vec<(f64, u32)> {
        scratch.begin(nodes.len());
        let mut candidates: BinaryHeap<Reverse<Dist>> = BinaryHeap::with_capacity(ef + 1);
        let mut results: BinaryHeap<Dist> = BinaryHeap::with_capacity(ef + 1);

        let entry_distance = l2_distance(vector_slice(vectors, dim, entry), query);
        scratch.mark(entry);
        candidates.push(Reverse(Dist(entry_distance, entry)));
        results.push(Dist(entry_distance, entry));

        while let Some(Reverse(Dist(distance, current))) = candidates.pop() {
            let worst = results.peek().map(|d| d.0).unwrap_or(f64::INFINITY);
            if results.len() >= ef && distance > worst {
                break;
            }
            let links = &nodes[current as usize].links[level];
            for &neighbour in links {
                if !scratch.mark(neighbour) {
                    continue;
                }
                let neighbour_distance = l2_distance(vector_slice(vectors, dim, neighbour), query);
                let worst = results.peek().map(|d| d.0).unwrap_or(f64::INFINITY);
                if results.len() < ef || neighbour_distance < worst {
                    candidates.push(Reverse(Dist(neighbour_distance, neighbour)));
                    results.push(Dist(neighbour_distance, neighbour));
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        results.into_vec().into_iter().map(|d| (d.0, d.1)).collect()
    }

    /// Neighbour selection heuristic (HNSW paper, Algorithm 4): keep a
    /// candidate only when it is closer to the query than to every neighbour
    /// already selected, then backfill with the closest discarded candidates.
    fn select_neighbors(&self, candidates: &[(f64, u32)], cap: usize) -> Vec<u32> {
        if candidates.is_empty() || cap == 0 {
            return Vec::new();
        }
        let mut sorted = candidates.to_vec();
        sorted.sort_by(|a, b| dist_cmp(*a, *b));
        if sorted.len() <= cap {
            return sorted.into_iter().map(|(_, id)| id).collect();
        }

        let mut selected: Vec<u32> = Vec::with_capacity(cap);
        let mut discarded: Vec<u32> = Vec::with_capacity(sorted.len() - cap);
        for &(distance, id) in &sorted {
            let mut keep = true;
            for &chosen in &selected {
                if self.dist_to_node(id, chosen) < distance {
                    keep = false;
                    break;
                }
            }
            if keep {
                selected.push(id);
                if selected.len() == cap {
                    return selected;
                }
            } else {
                discarded.push(id);
            }
        }
        for id in discarded {
            selected.push(id);
            if selected.len() == cap {
                break;
            }
        }
        selected
    }

    /// Adds `to` as a neighbour of `from` at `level`, pruning with the
    /// selection heuristic when the neighbour list overflows.
    fn connect(&mut self, from: u32, to: u32, level: usize) {
        let cap = self.max_links(level);
        let mut links = std::mem::take(&mut self.nodes[from as usize].links[level]);
        if links.contains(&to) {
            self.nodes[from as usize].links[level] = links;
            return;
        }
        links.push(to);
        if links.len() > cap {
            let mut candidates: Vec<(f64, u32)> = Vec::with_capacity(links.len());
            for &neighbour in &links {
                candidates.push((self.dist_to_node(from, neighbour), neighbour));
            }
            let selected = self.select_neighbors(&candidates, cap);
            self.nodes[from as usize].links[level] = selected;
        } else {
            self.nodes[from as usize].links[level] = links;
        }
    }

    /// Inserts a brand new node (the pk must not currently be live).
    fn insert_node(&mut self, pk: i64, vector: &[f32]) -> u32 {
        debug_assert_eq!(self.vectors.len(), self.nodes.len() * self.dim);
        let id = self.nodes.len() as u32;
        let level = self.random_level();

        self.vectors.extend_from_slice(vector);
        let mut links = Vec::with_capacity(level as usize + 1);
        for _ in 0..=level {
            links.push(Vec::new());
        }
        self.nodes.push(Node {
            pk,
            level,
            deleted: false,
            links,
        });

        let entry = match self.entry {
            Some(entry) => entry,
            None => {
                self.entry = Some(id);
                self.max_level = level;
                return id;
            }
        };

        // Phase 1: greedy descent from the top layer to the new node's top
        // layer (inclusive of `level + 1`).
        let (mut ep, _) = self.greedy_search(
            vector,
            entry,
            self.max_level as usize,
            level as usize + 1,
        );

        // Phase 2: link the new node into every layer it participates in.
        let top = level.min(self.max_level) as usize;
        for layer in (0..=top).rev() {
            let found = {
                let mut scratch = self.scratch.borrow_mut();
                Self::search_layer(
                    &self.vectors,
                    &self.nodes,
                    self.dim,
                    vector,
                    ep,
                    self.params.ef_construction,
                    layer,
                    &mut scratch,
                )
            };
            let cap = self.max_links(layer);
            let selected = self.select_neighbors(&found, cap);
            {
                let own_links = &mut self.nodes[id as usize].links[layer];
                own_links.clear();
                own_links.extend_from_slice(&selected);
            }
            for &neighbour in &selected {
                self.connect(neighbour, id, layer);
            }
            if let Some(&(_, nearest)) = found.iter().min_by(|a, b| dist_cmp(**a, **b)) {
                ep = nearest;
            }
        }

        if level > self.max_level {
            self.max_level = level;
            self.entry = Some(id);
        }
        id
    }

    /// Exact linear scan; used for small live sets and for `k >= live`.
    fn exact_search(&self, query: &[f32], k: usize) -> Vec<(i64, f64)> {
        let mut scored: Vec<(i64, f64)> = Vec::with_capacity(self.live);
        for (id, node) in self.nodes.iter().enumerate() {
            if node.deleted {
                continue;
            }
            scored.push((node.pk, self.dist_to(query, id as u32)));
        }
        scored.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(k);
        scored
    }

    /// Core insertion path used by both `add` and `rebuild`; never compacts.
    fn add_impl(&mut self, pk: i64, vector: &[f32]) {
        if vector.is_empty() {
            return;
        }
        if self.dim == 0 {
            self.dim = vector.len();
        }
        if vector.len() != self.dim {
            return;
        }
        if let Some(&old) = self.pk_to_node.get(&pk) {
            let node = &mut self.nodes[old as usize];
            if !node.deleted {
                node.deleted = true;
                self.live -= 1;
            }
        }
        let id = self.insert_node(pk, vector);
        self.pk_to_node.insert(pk, id);
        self.live += 1;
    }

    /// Rebuilds the graph from the live nodes only, dropping tombstones.
    fn compact(&mut self) {
        let dim = self.dim;
        if dim == 0 {
            return;
        }
        let mut items: Vec<(i64, Vec<f32>)> = Vec::with_capacity(self.live);
        for id in 0..self.nodes.len() {
            if self.nodes[id].deleted {
                continue;
            }
            let start = id * dim;
            items.push((self.nodes[id].pk, self.vectors[start..start + dim].to_vec()));
        }
        self.rebuild(&items);
    }
}

impl AnnIndex for HnswIndex {
    fn len(&self) -> usize {
        self.live
    }

    fn add(&mut self, pk: i64, vector: &[f32]) {
        self.add_impl(pk, vector);
        // Bound tombstone memory: once more than half of the nodes are dead and
        // the graph is non-trivial, rebuild from the live set.
        if self.nodes.len() > 8192 && self.nodes.len() - self.live > self.live {
            self.compact();
        }
    }

    fn remove(&mut self, pk: i64) {
        if let Some(id) = self.pk_to_node.remove(&pk) {
            let node = &mut self.nodes[id as usize];
            if !node.deleted {
                node.deleted = true;
                self.live -= 1;
            }
        }
    }

    fn clear(&mut self) {
        self.vectors.clear();
        self.nodes.clear();
        self.pk_to_node.clear();
        self.entry = None;
        self.max_level = 0;
        self.live = 0;
        self.rng = Rng::new(self.params.seed);
    }

    fn rebuild(&mut self, items: &[(i64, Vec<f32>)]) {
        self.clear();
        if let Some((_, first)) = items.first() {
            self.dim = first.len();
        }
        let dim = self.dim;
        if dim == 0 {
            return;
        }
        self.vectors.reserve(items.len() * dim);
        self.nodes.reserve(items.len());
        self.pk_to_node.reserve(items.len());
        for (pk, vector) in items {
            if vector.len() != dim {
                continue;
            }
            self.add_impl(*pk, vector);
        }
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(i64, f64)> {
        if k == 0 || self.live == 0 || self.dim == 0 || query.len() != self.dim {
            return Vec::new();
        }
        // Small collections (and requests asking for everything) are answered
        // exactly; this is what keeps the public VECTOR(2) ANN cases bit-for-bit
        // identical to brute force.
        if self.live <= self.params.exact_scan_limit || k >= self.live {
            return self.exact_search(query, k);
        }
        let entry = match self.entry {
            Some(entry) => entry,
            None => return Vec::new(),
        };
        let (start, _) = self.greedy_search(query, entry, self.max_level as usize, 1);
        let ef = self.params.ef_search.max(k).min(self.live);
        let found = {
            let mut scratch = self.scratch.borrow_mut();
            Self::search_layer(
                &self.vectors,
                &self.nodes,
                self.dim,
                query,
                start,
                ef,
                0,
                &mut scratch,
            )
        };

        let mut out: Vec<(i64, f64)> = Vec::with_capacity(found.len());
        for (distance, id) in found {
            let node = &self.nodes[id as usize];
            if node.deleted {
                continue;
            }
            out.push((node.pk, distance));
        }
        out.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        // Defensive: a pk can never be live twice, but never emit duplicates.
        out.dedup_by(|a, b| a.0 == b.0);
        out.truncate(k);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::FlatIndex;
    use std::collections::HashSet;
    use std::time::Instant;

    /// Small deterministic xorshift64* RNG for test data (no external crates).
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
            let bits = (self.next_u64() >> 40) as u32; // 24 random bits
            (bits as f32 / (1u32 << 24) as f32) * 2.0 - 1.0
        }

        fn next_usize(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }
    }

    fn random_items(seed: u64, n: usize, dim: usize) -> Vec<(i64, Vec<f32>)> {
        let mut rng = Lcg::new(seed);
        (0..n)
            .map(|i| (i as i64 + 1, (0..dim).map(|_| rng.next_f32()).collect()))
            .collect()
    }

    fn brute_force(items: &[(i64, Vec<f32>)], query: &[f32], k: usize) -> Vec<(i64, f64)> {
        let mut scored: Vec<(i64, f64)> = items
            .iter()
            .map(|(pk, vector)| (*pk, l2_distance(vector, query)))
            .collect();
        scored.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(k);
        scored
    }

    fn recall_at_k(
        index: &HnswIndex,
        items: &[(i64, Vec<f32>)],
        queries: &[Vec<f32>],
        k: usize,
    ) -> f64 {
        let mut hits = 0usize;
        for query in queries {
            let truth = brute_force(items, query, k);
            let got = index.search(query, k);
            let got_set: HashSet<i64> = got.iter().map(|(pk, _)| *pk).collect();
            assert_eq!(got_set.len(), got.len(), "search returned duplicate pks");
            for (pk, _) in &truth {
                if got_set.contains(pk) {
                    hits += 1;
                }
            }
        }
        hits as f64 / (queries.len() * k) as f64
    }

    fn assert_well_formed(results: &[(i64, f64)], k: usize) {
        assert!(results.len() <= k);
        let mut seen = HashSet::new();
        for (index, (pk, distance)) in results.iter().enumerate() {
            assert!(distance.is_finite());
            assert!(seen.insert(*pk), "duplicate pk {} in results", pk);
            if index > 0 {
                let (prev_pk, prev_distance) = results[index - 1];
                assert!(
                    prev_distance < *distance || (prev_distance == *distance && prev_pk < *pk),
                    "results not ordered by (distance, pk): {:?}",
                    results
                );
            }
        }
    }

    #[test]
    fn empty_index_returns_empty() {
        let index = HnswIndex::new(2);
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
        assert!(index.search(&[0.0, 0.0], 10).is_empty());
        assert_eq!(index.node_count(), 0);
    }

    #[test]
    fn k_zero_returns_empty() {
        let mut index = HnswIndex::new(2);
        index.add(1, &[0.0, 0.0]);
        assert!(index.search(&[0.0, 0.0], 0).is_empty());
    }

    #[test]
    fn small_index_is_exactly_brute_force() {
        // Default `exact_scan_limit` is 4096, so this exercises the exact path
        // used by the public VECTOR(2) ANN cases.
        let items = random_items(0x1234_5678, 200, 2);
        let mut index = HnswIndex::new(2);
        index.rebuild(&items);

        let mut rng = Lcg::new(0xFEED_FACE);
        for _ in 0..50 {
            let query: Vec<f32> = (0..2).map(|_| rng.next_f32()).collect();
            let expected = brute_force(&items, &query, 7);
            let got = index.search(&query, 7);
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn ties_are_broken_by_pk_like_brute_force() {
        let items: Vec<(i64, Vec<f32>)> = vec![
            (9, vec![0.0, 0.0]),
            (4, vec![1.0, 0.0]),
            (7, vec![-1.0, 0.0]),
            (2, vec![0.0, 1.0]),
            (5, vec![0.0, -1.0]),
            (1, vec![1.0, 1.0]),
        ];
        let mut index = HnswIndex::new(2);
        index.rebuild(&items);

        let mut flat = FlatIndex::new(2);
        flat.rebuild(&items);
        let query = [0.0f32, 0.0];
        for k in [1usize, 3, 5, 10] {
            let got = index.search(&query, k);
            assert_eq!(got, flat.search(&query, k));
        }
        assert_eq!(index.search(&query, 10)[0].0, 9);
    }

    #[test]
    fn remove_unknown_pk_is_noop_and_deleted_pks_never_returned() {
        let items = random_items(0xABCD_EF01, 300, 3);
        let mut index = HnswIndex::new(3);
        index.rebuild(&items);

        index.remove(12345); // unknown pk: no-op
        assert_eq!(index.len(), items.len());

        let mut removed = HashSet::new();
        for pk in [1i64, 5, 17, 42, 299] {
            index.remove(pk);
            removed.insert(pk);
        }
        assert_eq!(index.len(), items.len() - removed.len());

        let query = [0.1f32, -0.2, 0.3];
        let got = index.search(&query, 50);
        assert_well_formed(&got, 50);
        for (pk, _) in &got {
            assert!(!removed.contains(pk), "deleted pk {} was returned", pk);
        }

        // Removing the same pk twice is also a no-op.
        index.remove(5);
        assert_eq!(index.len(), items.len() - removed.len());
    }

    #[test]
    fn readd_replaces_vector_and_keeps_pk_unique() {
        let mut index = HnswIndex::new(2);
        index.add(1, &[0.0, 0.0]);
        index.add(2, &[10.0, 10.0]);
        index.add(1, &[100.0, 100.0]); // move pk 1 far away

        assert_eq!(index.len(), 2);
        let got = index.search(&[0.0, 0.0], 2);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, 2);
        assert_eq!(got[1].0, 1);
        // The reported distance must match the *new* vector of pk 1.
        let expected = l2_distance(&[100.0, 100.0], &[0.0, 0.0]);
        assert!((got[1].1 - expected).abs() < 1e-12);

        // Re-adding the same vector for an existing pk keeps exactly one copy.
        index.add(2, &[10.0, 10.0]);
        assert_eq!(index.len(), 2);
        let got = index.search(&[0.0, 0.0], 10);
        assert_well_formed(&got, 10);
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn clear_and_rebuild_reset_state() {
        let mut index = HnswIndex::new(4);
        index.rebuild(&random_items(7, 100, 4));
        assert_eq!(index.len(), 100);

        index.clear();
        assert_eq!(index.len(), 0);
        assert!(index.search(&[0.0; 4], 5).is_empty());

        let items = random_items(11, 60, 4);
        index.rebuild(&items);
        assert_eq!(index.len(), 60);
        let query = [0.3f32, -0.1, 0.0, 0.2];
        assert_eq!(index.search(&query, 5), brute_force(&items, &query, 5));

        // Rebuild from an empty slice empties the index.
        index.rebuild(&[]);
        assert_eq!(index.len(), 0);
        assert!(index.search(&query, 5).is_empty());
    }

    #[test]
    fn dimension_from_first_vector_and_mismatches_ignored() {
        let mut index = HnswIndex::new(0);
        index.add(1, &[1.0, 2.0, 3.0]);
        assert_eq!(index.dim(), 3);
        index.add(2, &[4.0, 5.0]); // wrong dimension: ignored
        assert_eq!(index.len(), 1);
        // Query with a wrong dimension returns nothing rather than panicking.
        assert!(index.search(&[1.0, 2.0], 1).is_empty());

        index.rebuild(&[(10, vec![0.0, 0.0]), (11, vec![0.0, 1.0])]);
        assert_eq!(index.dim(), 2);
        assert_eq!(index.len(), 2);
        let got = index.search(&[0.0, 0.0], 5);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, 10);
    }

    #[test]
    fn k_larger_than_len_returns_all_sorted() {
        let items = random_items(3, 25, 2);
        let mut index = HnswIndex::new(2);
        index.rebuild(&items);
        let query = [0.5f32, 0.5];
        let got = index.search(&query, 1000);
        assert_eq!(got.len(), items.len());
        assert_eq!(got, brute_force(&items, &query, items.len()));
        assert_well_formed(&got, items.len());
    }

    /// Exercises the graph path (not the exact fast path) on a small dataset.
    #[test]
    fn graph_path_recall_and_ordering() {
        let items = random_items(0x5EED_0001, 800, 12);
        let params = HnswParams {
            m: 8,
            m0: 16,
            ef_construction: 32,
            ef_search: 48,
            exact_scan_limit: 0, // force the HNSW code path
            seed: 0x1234_5678,
        };
        let mut index = HnswIndex::with_params(12, params);
        index.rebuild(&items);

        let mut rng = Lcg::new(0xC0FF_EE00);
        let queries: Vec<Vec<f32>> = (0..30)
            .map(|_| (0..12).map(|_| rng.next_f32()).collect())
            .collect();
        let recall = recall_at_k(&index, &items, &queries, 10);
        assert!(recall >= 0.95, "graph-path recall@10 too low: {}", recall);

        for query in &queries {
            let got = index.search(query, 10);
            assert_eq!(got.len(), 10);
            assert_well_formed(&got, 10);
            for (pk, distance) in &got {
                let vector = &items[(*pk - 1) as usize].1;
                let expected = l2_distance(vector, query);
                assert!((expected - distance).abs() < 1e-9);
            }
        }
    }

    /// Mixed incremental maintenance: adds, removals and updates while the
    /// graph path is active; results must always be live, unique and correctly
    /// scored.
    #[test]
    fn incremental_maintenance_is_consistent() {
        let mut rng = Lcg::new(0x7777_1111);
        let mut index = HnswIndex::with_params(
            8,
            HnswParams {
                m: 8,
                m0: 16,
                ef_construction: 32,
                ef_search: 48,
                exact_scan_limit: 0,
                seed: 0x99,
            },
        );
        let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
        for pk in 1..=400i64 {
            let vector: Vec<f32> = (0..8).map(|_| rng.next_f32()).collect();
            index.add(pk, &vector);
            live.insert(pk, vector);
        }
        for pk in [3i64, 40, 77, 123, 200, 399] {
            index.remove(pk);
            live.remove(&pk);
        }
        for pk in [7i64, 88] {
            let vector: Vec<f32> = (0..8).map(|_| rng.next_f32()).collect();
            index.add(pk, &vector);
            live.insert(pk, vector);
        }
        assert_eq!(index.len(), live.len());

        let mut items: Vec<(i64, Vec<f32>)> =
            live.iter().map(|(pk, v)| (*pk, v.clone())).collect();
        items.sort_by_key(|(pk, _)| *pk);

        for _ in 0..20 {
            let query: Vec<f32> = (0..8).map(|_| rng.next_f32()).collect();
            let got = index.search(&query, 10);
            assert_eq!(got.len(), 10);
            assert_well_formed(&got, 10);
            for (pk, distance) in &got {
                let vector = live.get(pk).expect("search returned a non-live pk");
                let expected = l2_distance(vector, &query);
                assert!((expected - distance).abs() < 1e-9);
            }
            // Recall of the approximate path should still be high after churn.
            let truth = brute_force(&items, &query, 10);
            let got_set: HashSet<i64> = got.iter().map(|(pk, _)| *pk).collect();
            let hits = truth.iter().filter(|(pk, _)| got_set.contains(pk)).count();
            assert!(hits >= 8, "recall after churn too low: {}/10", hits);
        }
    }

    #[test]
    fn tombstone_compaction_preserves_results() {
        // Force the internal compaction threshold by adding then deleting many
        // entries; the index must stay correct and bounded in size.
        let items = random_items(0x3131_4242, 9000, 4);
        let mut index = HnswIndex::new(4);
        index.rebuild(&items);
        assert_eq!(index.len(), 9000);
        for pk in 1..=6000i64 {
            index.remove(pk);
        }
        assert_eq!(index.len(), 3000);
        let query = [0.0f32, 0.0, 0.0, 0.0];
        let got = index.search(&query, 20);
        assert_well_formed(&got, 20);
        for (pk, _) in &got {
            assert!(*pk > 6000, "deleted pk {} returned after compaction", pk);
        }
        // Remaining live vectors must still be found exactly.
        let live: Vec<(i64, Vec<f32>)> =
            items.iter().filter(|(pk, _)| *pk > 6000).cloned().collect();
        assert_eq!(index.search(&query, 20), brute_force(&live, &query, 20));
    }

    /// Full-size benchmark: 50k x 64 build, query latency and recall@10.
    /// Run with:
    /// `cargo test --release -- --ignored --nocapture perf_50k_64d`
    #[test]
    #[ignore = "performance benchmark (release build only)"]
    fn perf_50k_64d() {
        const N: usize = 50_000;
        const DIM: usize = 64;
        const QUERIES: usize = 200;
        const K: usize = 10;

        let mut rng = Lcg::new(0xDEAD_BEEF_CAFE_F00D);
        let mut items: Vec<(i64, Vec<f32>)> = Vec::with_capacity(N);
        for i in 0..N {
            items.push((i as i64 + 1, (0..DIM).map(|_| rng.next_f32()).collect()));
        }
        let queries: Vec<Vec<f32>> = (0..QUERIES)
            .map(|_| (0..DIM).map(|_| rng.next_f32()).collect())
            .collect();

        let mut index = HnswIndex::new(DIM);
        let start = Instant::now();
        index.rebuild(&items);
        let build = start.elapsed();

        let start = Instant::now();
        let truth: Vec<Vec<(i64, f64)>> = queries
            .iter()
            .map(|query| brute_force(&items, query, K))
            .collect();
        let brute = start.elapsed();

        let mut latencies: Vec<f64> = Vec::with_capacity(QUERIES);
        let mut hits = 0usize;
        for (query, expected) in queries.iter().zip(truth.iter()) {
            let start = Instant::now();
            let got = index.search(query, K);
            latencies.push(start.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(got.len(), K);
            assert_well_formed(&got, K);
            let expected_pks: HashSet<i64> = expected.iter().map(|(pk, _)| *pk).collect();
            for (pk, distance) in &got {
                let vector = &items[(*pk - 1) as usize].1;
                let want = l2_distance(vector, query);
                assert!((want - distance).abs() <= 1e-9 * want.max(1.0));
                if expected_pks.contains(pk) {
                    hits += 1;
                }
            }
        }
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let recall = hits as f64 / (QUERIES * K) as f64;
        let p50 = latencies[latencies.len() / 2];
        let p99 = latencies[(latencies.len() * 99) / 100];

        println!(
            "50k x 64: build={:?} ({:.1} us/vector), brute-force ground truth={:?}, \
             query p50={:.3} ms, p99={:.3} ms, recall@10={:.4}",
            build,
            build.as_secs_f64() * 1e6 / N as f64,
            brute,
            p50,
            p99,
            recall
        );

        assert!(recall >= 0.95, "recall@10 {:.4} < 0.95", recall);
        assert!(build.as_secs_f64() < 10.0, "build took {:?}", build);
        assert!(p50 < 5.0, "p50 latency {:.3} ms", p50);
    }
}
