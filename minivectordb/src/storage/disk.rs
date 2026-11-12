//! Durable storage engine: immutable row segments plus a write-ahead log.
//!
//! Layout under the data directory:
//!
//!   MANIFEST               atomic snapshot: catalog, WAL generation, live
//!                          segment list (oldest -> newest) and next segment id
//!   seg-<id>.dat           row frames sorted by (table id, primary key)
//!   seg-<id>.idx           table id -> sorted (pk, frame offset, payload len)
//!                          plus sorted tombstone primary keys
//!   wal-<generation>.log   committed transactions not yet written to a segment
//!
//! A commit streams one self-describing, checksummed transaction record into the
//! current WAL and fsyncs it before returning. When the WAL grows past a
//! threshold the committed rows are written to a NEW segment and the manifest is
//! atomically switched (write MANIFEST.tmp, fsync, rename). Old segments are
//! never rewritten by a flush: a flush costs O(new rows), not O(table size), so
//! a single COMMIT can never trigger a full-table rewrite.
//!
//! Segment count is bounded by a size-tiered merge: when there are too many
//! segments, the oldest contiguous run whose total size fits a fixed budget
//! (a few hundred MB by default) is merged into one new segment. The merge is
//! bounded per commit and crash safe: the new segment and the new manifest are
//! fsynced before the manifest switch, and unreferenced files are garbage
//! collected on the next open.
//!
//! Reads merge all live segments (newest wins, tombstones hide older rows) with
//! the in-memory committed overlay and the current transaction. Only the
//! primary-key -> file-position index (20 bytes per row) and a bounded FIFO row
//! cache are resident, so tables larger than memory can still be scanned in
//! primary-key order with a streaming, buffered read per segment.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::format::{self, Slot, WalOp};
use super::Storage;
use crate::types::{ColumnDef, DbError, DbResult, Row, TableDef, ERR_INTERNAL, ERR_TX_STATE};

const MANIFEST_FILE: &str = "MANIFEST";
const MANIFEST_TEMP: &str = "MANIFEST.tmp";

/// WAL bytes tolerated before the committed overlay is written to a segment.
const DEFAULT_WAL_BYTES: u64 = 64 * 1024 * 1024;
/// Committed rows tolerated in memory before forcing a segment flush.
const DEFAULT_FLUSH_ROWS: usize = 512 * 1024;
/// Maximum bytes one merge operation may rewrite (per commit).
const DEFAULT_MERGE_BYTES: u64 = 128 * 1024 * 1024;
/// Segment count above which one bounded merge is performed per flush.
const DEFAULT_TARGET_SEGMENTS: usize = 16;
/// Default row-cache budget (bytes of decoded rows kept resident).
const DEFAULT_CACHE_BYTES: usize = 128 * 1024 * 1024;
const READ_BUFFER: usize = 1 << 20;
const SEGMENT_READ_BUFFER: usize = 1 << 16;
const WRITE_BUFFER: usize = 1 << 20;

/// Crash-safe storage engine.
pub struct DiskStorage {
    dir: PathBuf,
    generation: u64,
    next_segment_id: u64,
    catalog: Catalog,
    /// Live segments, oldest first; later segments shadow earlier ones.
    segments: Vec<Segment>,
    /// Committed but not yet written to a segment. None is a tombstone.
    committed: HashMap<u32, BTreeMap<i64, Option<Row>>>,
    committed_rows: usize,
    wal: Wal,
    cache: RefCell<RowCache>,
    txn: Option<Txn>,
    wal_bytes: u64,
    flush_rows: usize,
    merge_bytes: u64,
    target_segments: usize,
}

/// One immutable segment: rows plus tombstones for every table.
struct Segment {
    id: u64,
    data_bytes: u64,
    data_path: PathBuf,
    data_file: RefCell<File>,
    slots: HashMap<u32, Vec<Slot>>,
    tombstones: HashMap<u32, Vec<i64>>,
}

impl Segment {
    fn open(dir: &Path, id: u64) -> DbResult<Segment> {
        let data_path = segment_path(dir, id, "dat");
        let data_file = File::open(&data_path)?;
        let data_len = data_file.metadata()?.len();
        let index = read_index_file(&segment_path(dir, id, "idx"))?;
        let mut slots: HashMap<u32, Vec<Slot>> = HashMap::new();
        let mut tombstones: HashMap<u32, Vec<i64>> = HashMap::new();
        for (table_id, table_slots, table_tombstones) in index {
            let mut previous: Option<i64> = None;
            for slot in &table_slots {
                let end = slot.off.saturating_add(format::frame_total_len(slot.len as u64));
                if end > data_len {
                    return Err(format::corrupt("segment data file is shorter than its index"));
                }
                if let Some(previous) = previous {
                    if slot.pk <= previous {
                        return Err(format::corrupt("segment index is not sorted by primary key"));
                    }
                }
                previous = Some(slot.pk);
            }
            let mut previous: Option<i64> = None;
            for pk in &table_tombstones {
                if let Some(previous) = previous {
                    if *pk <= previous {
                        return Err(format::corrupt("segment tombstone list is not sorted"));
                    }
                }
                previous = Some(*pk);
            }
            if !table_slots.is_empty() {
                slots.insert(table_id, table_slots);
            }
            if !table_tombstones.is_empty() {
                tombstones.insert(table_id, table_tombstones);
            }
        }
        Ok(Segment {
            id,
            data_bytes: data_len,
            data_path,
            data_file: RefCell::new(data_file),
            slots,
            tombstones,
        })
    }

    fn has_table(&self, table_id: u32) -> bool {
        self.slots.contains_key(&table_id) || self.tombstones.contains_key(&table_id)
    }

    fn read_row(&self, slot: Slot) -> DbResult<Row> {
        let mut file = self.data_file.borrow_mut();
        file.seek(SeekFrom::Start(slot.off))?;
        let (kind, payload) = format::read_frame(&mut *file, slot.len)?;
        if kind != format::KIND_ROW {
            return Err(format::corrupt("unexpected record kind in segment data file"));
        }
        format::decode_row(&payload)
    }

    fn lookup(&self, table_id: u32, pk: i64) -> Option<Lookup> {
        if let Some(tombstones) = self.tombstones.get(&table_id) {
            if tombstones.binary_search(&pk).is_ok() {
                return Some(Lookup::Deleted);
            }
        }
        if let Some(slots) = self.slots.get(&table_id) {
            if let Ok(index) = slots.binary_search_by_key(&pk, |slot| slot.pk) {
                return Some(Lookup::Row(slots[index]));
            }
        }
        None
    }
}

enum Lookup {
    Row(Slot),
    Deleted,
}

impl DiskStorage {
    /// Open (or create) a database directory, replaying the WAL before return.
    pub fn open(dir: &Path) -> DbResult<Self> {
        fs::create_dir_all(dir)?;
        let mut storage = match read_manifest(dir)? {
            Some(manifest) => DiskStorage::from_manifest(dir, manifest)?,
            None => DiskStorage::initialize(dir)?,
        };
        storage.recover()?;
        storage.remove_orphans()?;
        Ok(storage)
    }

    /// Fresh data directory: wipe leftovers and write an empty generation 1.
    fn initialize(dir: &Path) -> DbResult<Self> {
        remove_all_owned_files(dir)?;
        let generation = 1u64;
        let next_segment_id = 1u64;
        File::create(wal_path(dir, generation))?.sync_all()?;
        write_manifest(dir, generation, next_segment_id, &format::CatalogImage::default(), &[])?;
        let wal = Wal::open(&wal_path(dir, generation), 0)?;
        Ok(DiskStorage {
            dir: dir.to_path_buf(),
            generation,
            next_segment_id,
            catalog: Catalog::default(),
            segments: Vec::new(),
            committed: HashMap::new(),
            committed_rows: 0,
            wal,
            cache: RefCell::new(RowCache::new(cache_budget_from_env())),
            txn: None,
            wal_bytes: wal_bytes_from_env(),
            flush_rows: DEFAULT_FLUSH_ROWS,
            merge_bytes: merge_bytes_from_env(),
            target_segments: target_segments_from_env(),
        })
    }

    fn from_manifest(dir: &Path, manifest: format::ManifestBody) -> DbResult<Self> {
        let catalog = Catalog::from_image(manifest.catalog);
        let mut segments = Vec::with_capacity(manifest.segments.len());
        for (id, _bytes) in &manifest.segments {
            segments.push(Segment::open(dir, *id)?);
        }
        let wal_path = wal_path(dir, manifest.generation);
        let wal_len = match fs::metadata(&wal_path) {
            Ok(metadata) => metadata.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                File::create(&wal_path)?.sync_all()?;
                0
            }
            Err(err) => return Err(err.into()),
        };
        let wal = Wal::open(&wal_path, wal_len)?;
        Ok(DiskStorage {
            dir: dir.to_path_buf(),
            generation: manifest.generation,
            next_segment_id: manifest.next_segment_id,
            catalog,
            segments,
            committed: HashMap::new(),
            committed_rows: 0,
            wal,
            cache: RefCell::new(RowCache::new(cache_budget_from_env())),
            txn: None,
            wal_bytes: wal_bytes_from_env(),
            flush_rows: DEFAULT_FLUSH_ROWS,
            merge_bytes: merge_bytes_from_env(),
            target_segments: target_segments_from_env(),
        })
    }

    /// Replay the current WAL, truncating a torn tail, then reopen it for append.
    fn recover(&mut self) -> DbResult<()> {
        let path = self.wal.path.clone();
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let file_len = file.metadata()?.len();
        let mut position = 0u64;
        {
            let mut reader = BufReader::with_capacity(READ_BUFFER, &mut file);
            loop {
                let remaining = file_len.saturating_sub(position);
                if remaining < format::HEADER_LEN as u64 {
                    break;
                }
                let mut header_bytes = [0u8; format::HEADER_LEN];
                if reader.read_exact(&mut header_bytes).is_err() {
                    break;
                }
                let Some(header) = format::parse_frame_header(&header_bytes) else {
                    break;
                };
                if header.kind != format::KIND_TXN {
                    break;
                }
                let payload_len = header.len as u64;
                if payload_len > format::MAX_WAL_RECORD {
                    break;
                }
                if remaining < format::frame_total_len(payload_len) {
                    break;
                }
                let mut crc = format::Crc32::new();
                crc.update(&header.kind.to_le_bytes());
                let mut payload_reader = format::CrcReader::with_crc(&mut reader, payload_len, crc);
                let decoded = format::read_txn(&mut payload_reader);
                let leftover = payload_reader.remaining();
                let (_, payload_crc) = payload_reader.finish();
                let Ok((ops, next_table_id)) = decoded else {
                    break;
                };
                if leftover != 0 {
                    break;
                }
                let mut trailer = [0u8; format::TRAILER_LEN];
                if reader.read_exact(&mut trailer).is_err() {
                    break;
                }
                if u32::from_le_bytes(trailer) != payload_crc {
                    break;
                }
                self.apply_ops(&ops, next_table_id)?;
                position += format::frame_total_len(payload_len);
            }
        }
        if position < file_len {
            file.set_len(position)?;
            file.sync_all()?;
        }
        self.wal = Wal::open(&path, position)?;
        Ok(())
    }

    /// Delete files that are neither referenced by the manifest nor the WAL.
    fn remove_orphans(&mut self) -> DbResult<()> {
        let mut keep: Vec<String> = Vec::with_capacity(self.segments.len() * 2 + 2);
        keep.push(MANIFEST_FILE.to_string());
        keep.push(file_name(&wal_path(&self.dir, self.generation)));
        for segment in &self.segments {
            keep.push(file_name(&segment_path(&self.dir, segment.id, "dat")));
            keep.push(file_name(&segment_path(&self.dir, segment.id, "idx")));
        }
        let mut stale = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if keep.iter().any(|kept| *kept == name) {
                continue;
            }
            let ours = name.starts_with("seg-")
                || name.starts_with("wal-")
                || name.starts_with("ckpt-")
                || name.starts_with("MANIFEST.")
                || name == "CURRENT"
                || name.starts_with("CURRENT.")
                || name.ends_with(".tmp");
            if ours {
                stale.push(entry.path());
            }
        }
        for path in stale {
            let _ = fs::remove_file(path);
        }
        Ok(())
    }

/// Apply already-committed operations to memory (used by commit and replay).
    fn apply_ops(&mut self, ops: &[WalOp], next_table_id: u32) -> DbResult<()> {
        for op in ops {
            match op {
                WalOp::CreateTable(def) => self.catalog.insert_table(def.clone()),
                WalOp::CreateIndex { table_id, column } => self.catalog.add_index(*table_id, *column),
                WalOp::Put { table_id, pk, row } => {
                    self.committed.entry(*table_id).or_default().insert(*pk, Some(row.clone()));
                    self.committed_rows += 1;
                }
                WalOp::Delete { table_id, pk } => {
                    self.committed.entry(*table_id).or_default().insert(*pk, None);
                    self.committed_rows += 1;
                }
            }
        }
        if next_table_id > self.catalog.next_table_id {
            self.catalog.next_table_id = next_table_id;
        }
        Ok(())
    }

    /// Apply a committed transaction held in memory (moves rows, no cloning).
    fn apply_txn(&mut self, txn: Txn) {
        for def in txn.new_tables {
            self.catalog.insert_table(def);
        }
        for (table_id, column) in txn.new_indexes {
            self.catalog.add_index(table_id, column as u32);
        }
        for ((table_id, pk), value) in txn.writes {
            self.committed.entry(table_id).or_default().insert(pk, value);
            self.committed_rows += 1;
        }
    }

    /// Exact WAL payload size for a transaction, used to stream the record.
    fn txn_payload_len(txn: &Txn) -> u64 {
        let mut len = 4u64;
        for def in &txn.new_tables {
            len += format::wal_create_table_len(def);
        }
        for _ in &txn.new_indexes {
            len += format::WAL_OP_CREATE_INDEX_LEN as u64;
        }
        for value in txn.writes.values() {
            len += match value {
                Some(row) => format::wal_put_len(row),
                None => format::WAL_OP_DELETE_LEN as u64,
            };
        }
        len += 4;
        len
    }

    /// Stream one committed transaction record into the WAL and fsync it.
    fn write_txn_record(&mut self, txn: &Txn) -> DbResult<()> {
        let payload_len = Self::txn_payload_len(txn);
        if payload_len > format::MAX_WAL_RECORD {
            return Err(DbError::new(ERR_INTERNAL, "transaction is too large to commit"));
        }
        let next_table_id = self.catalog.next_table_id;
        let op_count = (txn.new_tables.len() + txn.new_indexes.len() + txn.writes.len()) as u32;
        self.wal.append_stream(format::KIND_TXN, payload_len, |writer| {
            format::write_txn_header(writer, op_count)?;
            let mut stream = format::TxnWriter::new(writer);
            for def in &txn.new_tables {
                stream = stream.create_table(def)?;
            }
            for (table_id, column) in &txn.new_indexes {
                stream = stream.create_index(*table_id, *column as u32)?;
            }
            for ((table_id, pk), value) in &txn.writes {
                stream = match value {
                    Some(row) => stream.put(*table_id, *pk, row)?,
                    None => stream.delete(*table_id, *pk)?,
                };
            }
            stream.finish(next_table_id)?;
            Ok(())
        })
    }

    /// Auto-commit path used when the trait is driven without an explicit txn.
    fn auto_commit_ops(&mut self, ops: &[WalOp]) -> DbResult<()> {
        let payload = format::encode_txn(ops, self.catalog.next_table_id);
        if payload.len() as u64 > format::MAX_WAL_RECORD {
            return Err(DbError::new(ERR_INTERNAL, "transaction is too large to commit"));
        }
        self.wal.append_stream(format::KIND_TXN, payload.len() as u64, |writer| {
            writer.write_all(&payload)?;
            Ok(())
        })?;
        self.apply_ops(ops, self.catalog.next_table_id)?;
        self.maybe_flush();
        Ok(())
    }

    /// Flush the committed overlay to a new segment once the WAL or the overlay
    /// crosses its threshold. The flush cost is proportional to the new rows
    /// only, plus at most one bounded merge.
    fn maybe_flush(&mut self) {
        if self.txn.is_some() {
            return;
        }
        if self.wal.len < self.wal_bytes && self.committed_rows < self.flush_rows {
            return;
        }
        if let Err(err) = self.flush_and_compact() {
            eprintln!("minivectordb: segment flush failed: {}", err);
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction state
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Catalog (tables + vector index metadata)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Catalog {
    tables: Vec<TableDef>,
    names: HashMap<String, u32>,
    indexes: Vec<(u32, u32)>,
    next_table_id: u32,
}

impl Catalog {
    fn from_image(image: format::CatalogImage) -> Catalog {
        let mut catalog = Catalog {
            tables: Vec::new(),
            names: HashMap::new(),
            indexes: image.indexes,
            next_table_id: image.next_table_id,
        };
        for def in image.tables {
            catalog.insert_table(def);
        }
        catalog
    }

    fn image(&self) -> format::CatalogImage {
        format::CatalogImage {
            tables: self.tables.clone(),
            indexes: self.indexes.clone(),
            next_table_id: self.next_table_id,
        }
    }

    fn table_by_id(&self, id: u32) -> Option<&TableDef> {
        match self.tables.binary_search_by_key(&id, |def| def.id) {
            Ok(index) => Some(&self.tables[index]),
            Err(_) => None,
        }
    }

    fn table(&self, name: &str) -> Option<&TableDef> {
        self.names.get(&name.to_ascii_lowercase()).and_then(|id| self.table_by_id(*id))
    }

    fn insert_table(&mut self, def: TableDef) {
        if self.table_by_id(def.id).is_some() {
            return;
        }
        let key = def.name.to_ascii_lowercase();
        if self.names.contains_key(&key) {
            return;
        }
        self.names.insert(key, def.id);
        if def.id > self.next_table_id {
            self.next_table_id = def.id;
        }
        let position = self
            .tables
            .binary_search_by_key(&def.id, |table| table.id)
            .unwrap_or_else(|position| position);
        self.tables.insert(position, def);
    }

    fn add_index(&mut self, table_id: u32, column: u32) {
        if !self.indexes.contains(&(table_id, column)) {
            self.indexes.push((table_id, column));
        }
    }
}

// ---------------------------------------------------------------------------
// Segment flush and bounded merge
// ---------------------------------------------------------------------------

impl DiskStorage {

    /// Write the committed overlay into a new segment, then optionally merge one
    /// bounded run of old segments. Only new rows plus the merge budget are
    /// written, so the worst-case commit latency is bounded and independent of
    /// the total table size.
    fn flush_and_compact(&mut self) -> DbResult<()> {
        if self.txn.is_some() {
            return Ok(());
        }
        let need_flush = !self.committed.is_empty();
        if !need_flush && self.wal.len > 0 {
            // Never rotate a WAL that still carries data not present in a segment.
            return Ok(());
        }
        let segment_count = self.segments.len() + usize::from(need_flush);
        let merge_run = if segment_count > self.target_segments {
            self.select_merge_run()
        } else {
            None
        };
        if !need_flush && merge_run.is_none() {
            return Ok(());
        }
        let mut next_segment_id = self.next_segment_id;
        let flushed = if need_flush {
            let segment = self.write_overlay_segment(next_segment_id)?;
            next_segment_id += 1;
            Some(segment)
        } else {
            None
        };
        let merged = match merge_run {
            Some((start, end)) => {
                let segment = self.merge_segment_range(start, end, next_segment_id)?;
                next_segment_id += 1;
                Some((start, end, segment))
            }
            None => None,
        };
        let old_segments = std::mem::take(&mut self.segments);
        let mut old_slots: Vec<Option<Segment>> = old_segments.into_iter().map(Some).collect();
        let merge_range = merged.as_ref().map(|(start, end, _)| (*start, *end));
        let mut merged_segment = merged.map(|(_, _, segment)| segment);
        let mut removed: Vec<Segment> = Vec::new();
        let mut final_segments: Vec<Segment> = Vec::with_capacity(old_slots.len() + 2);
        let mut index = 0usize;
        while index < old_slots.len() {
            if let Some((start, end)) = merge_range {
                if index == start {
                    for slot in old_slots.iter_mut().take(end).skip(start) {
                        if let Some(segment) = slot.take() {
                            removed.push(segment);
                        }
                    }
                    if let Some(segment) = merged_segment.take() {
                        final_segments.push(segment);
                    }
                    index = end;
                    continue;
                }
            }
            if let Some(segment) = old_slots[index].take() {
                final_segments.push(segment);
            }
            index += 1;
        }
        if let Some(segment) = flushed {
            final_segments.push(segment);
        }
        self.publish(next_segment_id, final_segments, removed, need_flush)
    }

    /// Pick the oldest contiguous run whose size fits the merge budget.
    fn select_merge_run(&self) -> Option<(usize, usize)> {
        let count = self.segments.len();
        if count < 2 {
            return None;
        }
        for start in 0..count {
            let mut total = self.segments[start].data_bytes;
            if total > self.merge_bytes {
                continue;
            }
            let mut end = start + 1;
            while end < count {
                let next = self.segments[end].data_bytes;
                if total.saturating_add(next) > self.merge_bytes {
                    break;
                }
                total += next;
                end += 1;
            }
            if end - start >= 2 {
                return Some((start, end));
            }
        }
        None
    }

    fn write_overlay_segment(&mut self, id: u64) -> DbResult<Segment> {
        let mut builder = SegmentBuilder::new(&self.dir, id)?;
        for table in &self.catalog.tables {
            let table_id = table.id;
            let Some(overlay) = self.committed.get(&table_id) else {
                continue;
            };
            for (pk, value) in overlay {
                match value {
                    Some(row) => builder.push_row(table_id, *pk, row)?,
                    None => builder.push_tombstone(table_id, *pk),
                }
            }
        }
        builder.finish()
    }

    /// Merge the contiguous run [start, end) of live segments into one segment.
    /// Newer inputs shadow older ones and tombstones are dropped.
    fn merge_segment_range(&self, start: usize, end: usize, id: u64) -> DbResult<Segment> {
        let mut builder = SegmentBuilder::new(&self.dir, id)?;
        // A tombstone may only be dropped when no older segment survives the
        // merge, otherwise it must be carried forward to keep hiding old rows.
        let keep_tombstones = start > 0;
        let inputs = &self.segments[start..end];
        for table in &self.catalog.tables {
            let table_id = table.id;
            let mut cursors: Vec<SegmentCursor> = Vec::new();
            for segment in inputs {
                if !segment.has_table(table_id) {
                    continue;
                }
                cursors.push(SegmentCursor::new(segment, table_id)?);
            }
            if cursors.is_empty() {
                continue;
            }
            loop {
                let mut min_pk: Option<i64> = None;
                for cursor in &cursors {
                    if let Some(pk) = cursor.peek() {
                        min_pk = Some(match min_pk {
                            Some(current) => current.min(pk),
                            None => pk,
                        });
                    }
                }
                let Some(pk) = min_pk else {
                    break;
                };
                let mut chosen_cursor: Option<usize> = None;
                let mut chosen_slot: Option<Option<Slot>> = None;
                for cursor_index in (0..cursors.len()).rev() {
                    if cursors[cursor_index].peek() == Some(pk) {
                        let entry = cursors[cursor_index].advance();
                        if chosen_cursor.is_none() {
                            chosen_cursor = Some(cursor_index);
                            chosen_slot = Some(entry);
                        }
                    }
                }
                if let (Some(cursor_index), Some(Some(slot))) = (chosen_cursor, chosen_slot) {
                    let row = cursors[cursor_index].read_row(slot)?;
                    builder.push_row(table_id, pk, &row)?;
                } else if keep_tombstones && chosen_cursor.is_some() {
                    builder.push_tombstone(table_id, pk);
                }
            }
        }
        builder.finish()
    }

    /// Atomically publish a new segment list: new files and a new empty WAL are
    /// fsynced first, then the manifest rename is the single commit point.
    fn publish(
        &mut self,
        next_segment_id: u64,
        segments: Vec<Segment>,
        removed: Vec<Segment>,
        flushed: bool,
    ) -> DbResult<()> {
        let new_generation = self.generation + 1;
        let new_wal_path = wal_path(&self.dir, new_generation);
        File::create(&new_wal_path)?.sync_all()?;
        let new_wal = Wal::open(&new_wal_path, 0)?;
        let catalog_image = self.catalog.image();
        let manifest_segments: Vec<(u64, u64)> =
            segments.iter().map(|segment| (segment.id, segment.data_bytes)).collect();
        write_manifest(&self.dir, new_generation, next_segment_id, &catalog_image, &manifest_segments)?;

        // ---- single atomic switch point: only infallible work below ----
        let old_wal_path = self.wal.path.clone();
        let merged = !removed.is_empty();
        self.generation = new_generation;
        self.next_segment_id = next_segment_id;
        self.segments = segments;
        self.wal = new_wal;
        if flushed {
            self.committed.clear();
            self.committed_rows = 0;
        }
        if merged {
            self.cache.borrow_mut().clear();
        }
        for segment in removed {
            let id = segment.id;
            drop(segment);
            let _ = fs::remove_file(segment_path(&self.dir, id, "dat"));
            let _ = fs::remove_file(segment_path(&self.dir, id, "idx"));
        }
        let _ = fs::remove_file(old_wal_path);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Segment writer and cross-segment cursor
// ---------------------------------------------------------------------------

/// Streams rows and tombstones into a new segment file, then fsyncs and renames
/// both the data and the index into place.
struct SegmentBuilder {
    id: u64,
    dir: PathBuf,
    data_temp: PathBuf,
    data: Option<BufWriter<File>>,
    position: u64,
    slots: HashMap<u32, Vec<Slot>>,
    tombstones: HashMap<u32, Vec<i64>>,
    scratch: Vec<u8>,
}

impl SegmentBuilder {
    fn new(dir: &Path, id: u64) -> DbResult<SegmentBuilder> {
        let data_temp = segment_temp_path(dir, id, "dat");
        let file = File::create(&data_temp)?;
        Ok(SegmentBuilder {
            id,
            dir: dir.to_path_buf(),
            data_temp,
            data: Some(BufWriter::with_capacity(WRITE_BUFFER, file)),
            position: 0,
            slots: HashMap::new(),
            tombstones: HashMap::new(),
            scratch: Vec::new(),
        })
    }

    fn push_row(&mut self, table_id: u32, pk: i64, row: &Row) -> DbResult<()> {
        self.scratch.clear();
        format::encode_row_into(&mut self.scratch, row);
        let payload_len = self.scratch.len() as u32;
        let writer = self.data.as_mut().ok_or_else(|| DbError::internal("segment already finished"))?;
        format::write_frame(writer, format::KIND_ROW, &self.scratch)?;
        let offset = self.position;
        self.position += format::frame_total_len(payload_len as u64);
        self.slots.entry(table_id).or_default().push(Slot { pk, off: offset, len: payload_len });
        Ok(())
    }

    fn push_tombstone(&mut self, table_id: u32, pk: i64) {
        self.tombstones.entry(table_id).or_default().push(pk);
    }

    fn finish(mut self) -> DbResult<Segment> {
        let data = self.data.take().ok_or_else(|| DbError::internal("segment already finished"))?;
        let file = data.into_inner().map_err(|err| DbError::from(err.into_error()))?;
        file.sync_all()?;
        drop(file);
        let data_final = segment_path(&self.dir, self.id, "dat");
        fs::rename(&self.data_temp, &data_final)?;
        let index_temp = segment_temp_path(&self.dir, self.id, "idx");
        write_index_file(&index_temp, &self.slots, &self.tombstones)?;
        let index_final = segment_path(&self.dir, self.id, "idx");
        fs::rename(&index_temp, &index_final)?;
        sync_directory(&self.dir);
        let data_file = File::open(&data_final)?;
        Ok(Segment {
            id: self.id,
            data_bytes: self.position,
            data_path: data_final,
            data_file: RefCell::new(data_file),
            slots: self.slots,
            tombstones: self.tombstones,
        })
    }
}

/// Sorted cursor over one segment for a single table: merges the row slots and
/// the tombstone list so callers see one ascending primary-key stream.
struct SegmentCursor<'a> {
    segment_id: u64,
    reader: RecordReader,
    slots: &'a [Slot],
    tombstones: &'a [i64],
    slot_index: usize,
    tombstone_index: usize,
}

impl<'a> SegmentCursor<'a> {
    fn new(segment: &'a Segment, table_id: u32) -> DbResult<SegmentCursor<'a>> {
        Ok(SegmentCursor {
            segment_id: segment.id,
            reader: RecordReader::open(&segment.data_path)?,
            slots: segment.slots.get(&table_id).map(|slots| slots.as_slice()).unwrap_or(&[]),
            tombstones: segment
                .tombstones
                .get(&table_id)
                .map(|tombstones| tombstones.as_slice())
                .unwrap_or(&[]),
            slot_index: 0,
            tombstone_index: 0,
        })
    }

    fn peek(&self) -> Option<i64> {
        let slot_pk = self.slots.get(self.slot_index).map(|slot| slot.pk);
        let tombstone_pk = self.tombstones.get(self.tombstone_index).copied();
        match (slot_pk, tombstone_pk) {
            (Some(row), Some(tombstone)) => Some(row.min(tombstone)),
            (Some(row), None) => Some(row),
            (None, Some(tombstone)) => Some(tombstone),
            (None, None) => None,
        }
    }

    fn advance(&mut self) -> Option<Slot> {
        let slot_pk = self.slots.get(self.slot_index).map(|slot| slot.pk);
        let tombstone_pk = self.tombstones.get(self.tombstone_index).copied();
        match (slot_pk, tombstone_pk) {
            (Some(row), Some(tombstone)) => {
                if row <= tombstone {
                    let slot = self.slots[self.slot_index];
                    self.slot_index += 1;
                    Some(slot)
                } else {
                    self.tombstone_index += 1;
                    None
                }
            }
            (Some(_), None) => {
                let slot = self.slots[self.slot_index];
                self.slot_index += 1;
                Some(slot)
            }
            (None, Some(_)) => {
                self.tombstone_index += 1;
                None
            }
            (None, None) => None,
        }
    }

    fn read_row(&mut self, slot: Slot) -> DbResult<Row> {
        self.reader.read_row(slot)
    }

    fn segment_id(&self) -> u64 {
        self.segment_id
    }
}

// ---------------------------------------------------------------------------
// WAL file
// ---------------------------------------------------------------------------

struct Wal {
    path: PathBuf,
    file: File,
    len: u64,
}

impl Wal {
    fn open(path: &Path, len: u64) -> DbResult<Wal> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        Ok(Wal { path: path.to_path_buf(), file, len })
    }

    /// Append one record, streaming the payload, then fsync. On failure the
    /// partially written bytes are truncated away so the WAL stays replayable.
    fn append_stream<F>(&mut self, kind: u32, payload_len: u64, body: F) -> DbResult<()>
    where
        F: FnOnce(&mut format::CrcWriter<&mut File>) -> DbResult<()>,
    {
        let start = self.len;
        let result = self.append_stream_inner(kind, payload_len, body);
        if result.is_err() {
            let _ = self.file.set_len(start);
            let _ = self.file.seek(SeekFrom::End(0));
            self.len = start;
        }
        result
    }

    fn append_stream_inner<F>(&mut self, kind: u32, payload_len: u64, body: F) -> DbResult<()>
    where
        F: FnOnce(&mut format::CrcWriter<&mut File>) -> DbResult<()>,
    {
        if payload_len > format::MAX_WAL_RECORD {
            return Err(DbError::new(ERR_INTERNAL, "WAL record is too large"));
        }
        let mut header = [0u8; format::HEADER_LEN];
        header[0..4].copy_from_slice(&format::RECORD_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&(payload_len as u32).to_le_bytes());
        header[8..12].copy_from_slice(&kind.to_le_bytes());
        self.file.write_all(&header)?;
        let mut crc = format::Crc32::new();
        crc.update(&kind.to_le_bytes());
        let mut writer = format::CrcWriter::with_crc(&mut self.file, crc);
        body(&mut writer)?;
        let (file, written, checksum) = writer.finish();
        if written != payload_len {
            return Err(DbError::internal("WAL record length mismatch"));
        }
        file.write_all(&checksum.to_le_bytes())?;
        file.sync_data()?;
        self.len += format::frame_total_len(payload_len);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Bounded FIFO row cache (keyed by segment id so merges never return stale rows)
// ---------------------------------------------------------------------------

struct RowCache {
    budget: usize,
    used: usize,
    rows: HashMap<(u64, u32, i64), (Arc<Row>, usize)>,
    order: VecDeque<(u64, u32, i64)>,
}

impl RowCache {
    fn new(budget: usize) -> RowCache {
        RowCache { budget, used: 0, rows: HashMap::new(), order: VecDeque::new() }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.rows.len()
    }

    fn get(&self, key: (u64, u32, i64)) -> Option<Arc<Row>> {
        self.rows.get(&key).map(|(row, _)| Arc::clone(row))
    }

    fn insert(&mut self, key: (u64, u32, i64), row: Arc<Row>, size: usize) {
        if self.budget == 0 || size > self.budget {
            return;
        }
        if let Some((_, previous)) = self.rows.insert(key, (row, size)) {
            self.used = self.used.saturating_sub(previous);
        }
        self.used += size;
        self.order.push_back(key);
        while self.used > self.budget {
            let Some(victim) = self.order.pop_front() else {
                break;
            };
            if let Some((_, size)) = self.rows.remove(&victim) {
                self.used = self.used.saturating_sub(size);
            }
        }
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.order.clear();
        self.used = 0;
    }
}

// ---------------------------------------------------------------------------
// Buffered sequential reader for segment data files
// ---------------------------------------------------------------------------

struct RecordReader {
    reader: BufReader<File>,
    position: u64,
}

impl RecordReader {
    fn open(path: &Path) -> DbResult<RecordReader> {
        Ok(RecordReader {
            reader: BufReader::with_capacity(SEGMENT_READ_BUFFER, File::open(path)?),
            position: 0,
        })
    }

    fn read_row(&mut self, slot: Slot) -> DbResult<Row> {
        if self.position != slot.off {
            self.reader.seek(SeekFrom::Start(slot.off))?;
            self.position = slot.off;
        }
        let (kind, payload) = format::read_frame(&mut self.reader, slot.len)?;
        if kind != format::KIND_ROW {
            return Err(format::corrupt("unexpected record kind in segment data file"));
        }
        self.position += format::frame_total_len(slot.len as u64);
        format::decode_row(&payload)
    }
}

// ---------------------------------------------------------------------------
// File naming, manifest and index files
// ---------------------------------------------------------------------------

fn segment_path(dir: &Path, id: u64, extension: &str) -> PathBuf {
    dir.join(format!("seg-{}.{}", id, extension))
}

fn segment_temp_path(dir: &Path, id: u64, extension: &str) -> PathBuf {
    dir.join(format!("seg-{}.{}.tmp", id, extension))
}

fn wal_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("wal-{}.log", generation))
}

fn file_name(path: &Path) -> String {
    path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default()
}

fn cache_budget_from_env() -> usize {
    match std::env::var("MINIVECTORDB_ROW_CACHE_MIB") {
        Ok(text) => match text.trim().parse::<u64>() {
            Ok(mib) => (mib as usize).saturating_mul(1024 * 1024),
            Err(_) => DEFAULT_CACHE_BYTES,
        },
        Err(_) => DEFAULT_CACHE_BYTES,
    }
}

fn wal_bytes_from_env() -> u64 {
    match std::env::var("MINIVECTORDB_FLUSH_MIB") {
        Ok(text) => match text.trim().parse::<u64>() {
            Ok(mib) if mib > 0 => mib.saturating_mul(1024 * 1024),
            _ => DEFAULT_WAL_BYTES,
        },
        Err(_) => DEFAULT_WAL_BYTES,
    }
}

fn merge_bytes_from_env() -> u64 {
    match std::env::var("MINIVECTORDB_MERGE_MIB") {
        Ok(text) => match text.trim().parse::<u64>() {
            Ok(mib) if mib > 0 => mib.saturating_mul(1024 * 1024),
            _ => DEFAULT_MERGE_BYTES,
        },
        Err(_) => DEFAULT_MERGE_BYTES,
    }
}

fn target_segments_from_env() -> usize {
    match std::env::var("MINIVECTORDB_SEGMENTS") {
        Ok(text) => match text.trim().parse::<usize>() {
            Ok(count) if count >= 2 => count,
            _ => DEFAULT_TARGET_SEGMENTS,
        },
        Err(_) => DEFAULT_TARGET_SEGMENTS,
    }
}

fn read_manifest(dir: &Path) -> DbResult<Option<format::ManifestBody>> {
    if !dir.join(MANIFEST_FILE).exists() {
        return Ok(None);
    }
    let body = read_container_file(&dir.join(MANIFEST_FILE), format::MANIFEST_MAGIC)?;
    Ok(Some(format::decode_manifest_body(&body)?))
}

fn write_manifest(
    dir: &Path,
    generation: u64,
    next_segment_id: u64,
    catalog: &format::CatalogImage,
    segments: &[(u64, u64)],
) -> DbResult<()> {
    let body = format::encode_manifest_body(generation, next_segment_id, catalog, segments);
    let temp = dir.join(MANIFEST_TEMP);
    write_container_file(&temp, format::MANIFEST_MAGIC, &body)?;
    fs::rename(&temp, dir.join(MANIFEST_FILE))?;
    sync_directory(dir);
    Ok(())
}

/// Remove every file this engine may have created (used only for a directory
/// without a manifest, i.e. a fresh database or an interrupted initialization).
fn remove_all_owned_files(dir: &Path) -> DbResult<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let ours = name.starts_with("seg-")
            || name.starts_with("wal-")
            || name.starts_with("ckpt-")
            || name.starts_with("MANIFEST")
            || name == "CURRENT"
            || name.starts_with("CURRENT.")
            || name.ends_with(".tmp");
        if ours {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) {
    #[cfg(unix)]
    {
        if let Ok(directory) = File::open(path) {
            let _ = directory.sync_all();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

fn write_container_file(path: &Path, magic: u32, body: &[u8]) -> DbResult<()> {
    let mut file = File::create(path)?;
    let mut crc = format::Crc32::new();
    crc.update(body);
    file.write_all(&format::container_header(magic, body.len() as u64, crc.finish()))?;
    file.write_all(body)?;
    file.sync_all()?;
    Ok(())
}

fn read_container_file(path: &Path, magic: u32) -> DbResult<Vec<u8>> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut header_bytes = [0u8; format::CONTAINER_HEADER_LEN];
    file.read_exact(&mut header_bytes)?;
    let header = format::parse_container_header(&header_bytes, magic)
        .ok_or_else(|| format::corrupt("bad container magic"))?;
    if header.version != format::CONTAINER_VERSION {
        return Err(format::corrupt("unsupported container version"));
    }
    if header.body_len > format::MAX_CATALOG_BODY {
        return Err(format::corrupt("container body is implausibly large"));
    }
    if header.body_len > file_len.saturating_sub(format::CONTAINER_HEADER_LEN as u64) {
        return Err(format::corrupt("container body is truncated"));
    }
    let mut body = vec![0u8; header.body_len as usize];
    file.read_exact(&mut body)?;
    if format::crc32(&body) != header.crc {
        return Err(format::corrupt("container body checksum mismatch"));
    }
    Ok(body)
}

fn write_index_file(
    path: &Path,
    slots: &HashMap<u32, Vec<Slot>>,
    tombstones: &HashMap<u32, Vec<i64>>,
) -> DbResult<()> {
    let mut file = File::create(path)?;
    file.write_all(&[0u8; format::CONTAINER_HEADER_LEN])?;
    let (body_len, body_crc) = {
        let mut table_ids: Vec<u32> = slots.keys().copied().collect();
        for table_id in tombstones.keys() {
            if !table_ids.contains(table_id) {
                table_ids.push(*table_id);
            }
        }
        table_ids.sort_unstable();
        let empty_slots: &[Slot] = &[];
        let empty_tombstones: &[i64] = &[];
        let tables: Vec<(u32, &[Slot], &[i64])> = table_ids
            .iter()
            .map(|table_id| {
                (
                    *table_id,
                    slots.get(table_id).map(|list| list.as_slice()).unwrap_or(empty_slots),
                    tombstones.get(table_id).map(|list| list.as_slice()).unwrap_or(empty_tombstones),
                )
            })
            .collect();
        let mut writer = format::CrcWriter::new(BufWriter::with_capacity(WRITE_BUFFER, &mut file));
        format::write_index_body(&mut writer, &tables)?;
        let (mut buffered, written, checksum) = writer.finish();
        buffered.flush()?;
        (written, checksum)
    };
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&format::container_header(format::INDEX_MAGIC, body_len, body_crc))?;
    file.sync_all()?;
    Ok(())
}

#[allow(clippy::type_complexity)]
fn read_index_file(path: &Path) -> DbResult<Vec<(u32, Vec<Slot>, Vec<i64>)>> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut header_bytes = [0u8; format::CONTAINER_HEADER_LEN];
    file.read_exact(&mut header_bytes)?;
    let header = format::parse_container_header(&header_bytes, format::INDEX_MAGIC)
        .ok_or_else(|| format::corrupt("bad segment index magic"))?;
    if header.version != format::CONTAINER_VERSION {
        return Err(format::corrupt("unsupported segment index version"));
    }
    if header.body_len > file_len.saturating_sub(format::CONTAINER_HEADER_LEN as u64) {
        return Err(format::corrupt("segment index is truncated"));
    }
    let tables = {
        let reader = BufReader::with_capacity(READ_BUFFER, &mut file);
        let mut crc_reader = format::CrcReader::new(reader, header.body_len);
        let tables = format::read_index_body(&mut crc_reader)?;
        let leftover = crc_reader.remaining();
        let (_, checksum) = crc_reader.finish();
        if leftover != 0 || checksum != header.crc {
            return Err(format::corrupt("segment index checksum mismatch"));
        }
        tables
    };
    Ok(tables)
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

impl DiskStorage {
    fn cached_segment_row(&self, segment: &Segment, table_id: u32, slot: Slot) -> DbResult<Arc<Row>> {
        let key = (segment.id, table_id, slot.pk);
        let cached = self.cache.borrow().get(key);
        if let Some(row) = cached {
            return Ok(row);
        }
        let row = Arc::new(segment.read_row(slot)?);
        let size = format::encoded_row_len(&row) + 48;
        self.cache.borrow_mut().insert(key, Arc::clone(&row), size);
        Ok(row)
    }

    fn cached_cursor_row(
        &self,
        cursor: &mut SegmentCursor,
        table_id: u32,
        slot: Slot,
    ) -> DbResult<Arc<Row>> {
        let key = (cursor.segment_id(), table_id, slot.pk);
        let cached = self.cache.borrow().get(key);
        if let Some(row) = cached {
            return Ok(row);
        }
        let row = Arc::new(cursor.read_row(slot)?);
        let size = format::encoded_row_len(&row) + 48;
        self.cache.borrow_mut().insert(key, Arc::clone(&row), size);
        Ok(row)
    }

    /// Newest segment wins; a tombstone in a newer segment hides older rows.
    fn lookup_row(&self, table_id: u32, pk: i64) -> DbResult<Option<Row>> {
        for segment in self.segments.iter().rev() {
            match segment.lookup(table_id, pk) {
                Some(Lookup::Row(slot)) => {
                    return Ok(Some((*self.cached_segment_row(segment, table_id, slot)?).clone()))
                }
                Some(Lookup::Deleted) => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }

    /// Streaming merge of the transaction overlay, the committed overlay and all
    /// live segments, in ascending primary-key order.
    fn scan_table(&self, table_id: u32, f: &mut dyn FnMut(i64, &Row) -> bool) -> DbResult<()> {
        let overlay: Vec<(i64, &Option<Row>)> = match &self.txn {
            Some(txn) => {
                let mut items: Vec<(i64, &Option<Row>)> = txn
                    .writes
                    .iter()
                    .filter(|((table, _), _)| *table == table_id)
                    .map(|((_, pk), value)| (*pk, value))
                    .collect();
                items.sort_by_key(|(pk, _)| *pk);
                items
            }
            None => Vec::new(),
        };
        let committed = self.committed.get(&table_id);
        let mut cursors: Vec<SegmentCursor> = Vec::new();
        for segment in self.segments.iter().rev() {
            if segment.has_table(table_id) {
                cursors.push(SegmentCursor::new(segment, table_id)?);
            }
        }
        if committed.is_none() && overlay.is_empty() && cursors.is_empty() {
            return Ok(());
        }
        let mut committed_iter = committed.map(|map| map.iter());
        let mut committed_next: Option<(i64, &Option<Row>)> = None;
        let mut overlay_index = 0usize;
        loop {
            if committed_next.is_none() {
                committed_next = committed_iter
                    .as_mut()
                    .and_then(|iter| iter.next().map(|(pk, value)| (*pk, value)));
            }
            let overlay_pk = overlay.get(overlay_index).map(|(pk, _)| *pk);
            let committed_pk = committed_next.map(|(pk, _)| pk);
            let mut min_pk = overlay_pk;
            if let Some(pk) = committed_pk {
                min_pk = Some(match min_pk {
                    Some(current) => current.min(pk),
                    None => pk,
                });
            }
            for cursor in &cursors {
                if let Some(pk) = cursor.peek() {
                    min_pk = Some(match min_pk {
                        Some(current) => current.min(pk),
                        None => pk,
                    });
                }
            }
            let Some(pk) = min_pk else {
                break;
            };
            let mut decided = false;
            let mut borrowed: Option<&Row> = None;
            let mut chosen_cursor: Option<usize> = None;
            let mut chosen_slot: Option<Option<Slot>> = None;
            if overlay_pk == Some(pk) {
                decided = true;
                borrowed = overlay[overlay_index].1.as_ref();
                overlay_index += 1;
            }
            if committed_pk == Some(pk) {
                if !decided {
                    decided = true;
                    borrowed = committed_next.and_then(|(_, value)| value.as_ref());
                }
                committed_next = None;
            }
            for index in 0..cursors.len() {
                if cursors[index].peek() == Some(pk) {
                    let entry = cursors[index].advance();
                    if !decided {
                        decided = true;
                        chosen_cursor = Some(index);
                        chosen_slot = Some(entry);
                    }
                }
            }
            let mut shared: Option<Arc<Row>> = None;
            if let (Some(index), Some(Some(slot))) = (chosen_cursor, chosen_slot) {
                shared = Some(self.cached_cursor_row(&mut cursors[index], table_id, slot)?);
            }
            if let Some(row) = borrowed {
                if !f(pk, row) {
                    break;
                }
            } else if let Some(row) = shared {
                if !f(pk, row.as_ref()) {
                    break;
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn generation(&self) -> u64 {
        self.generation
    }
    #[cfg(test)]
    fn wal_len(&self) -> u64 {
        self.wal.len
    }
    #[cfg(test)]
    fn committed_row_count(&self) -> usize {
        self.committed_rows
    }
    #[cfg(test)]
    fn cached_row_count(&self) -> usize {
        self.cache.borrow().len()
    }
    #[cfg(test)]
    fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

// ---------------------------------------------------------------------------
// Storage trait implementation
// ---------------------------------------------------------------------------

impl Storage for DiskStorage {
    fn table(&self, name: &str) -> Option<TableDef> {
        if let Some(txn) = &self.txn {
            if let Some(def) = txn.new_tables.iter().rev().find(|table| table.name.eq_ignore_ascii_case(name)) {
                return Some(def.clone());
            }
        }
        self.catalog.table(name).cloned()
    }

    fn table_by_id(&self, id: u32) -> Option<TableDef> {
        if let Some(txn) = &self.txn {
            if let Some(def) = txn.new_tables.iter().rev().find(|table| table.id == id) {
                return Some(def.clone());
            }
        }
        self.catalog.table_by_id(id).cloned()
    }

    fn list_tables(&self) -> Vec<TableDef> {
        let mut tables = self.catalog.tables.clone();
        if let Some(txn) = &self.txn {
            tables.extend(txn.new_tables.iter().cloned());
        }
        tables.sort_by_key(|table| table.id);
        tables
    }

    fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>, pk: usize) -> DbResult<TableDef> {
        self.catalog.next_table_id += 1;
        let def = TableDef { id: self.catalog.next_table_id, name: name.to_string(), columns, pk };
        if let Some(txn) = &mut self.txn {
            txn.new_tables.push(def.clone());
        } else {
            self.auto_commit_ops(&[WalOp::CreateTable(def.clone())])?;
        }
        Ok(def)
    }

    fn create_vector_index(&mut self, table_id: u32, column: usize) -> DbResult<()> {
        if let Some(txn) = &mut self.txn {
            txn.new_indexes.push((table_id, column));
            Ok(())
        } else {
            self.auto_commit_ops(&[WalOp::CreateIndex { table_id, column: column as u32 }])
        }
    }

    fn vector_indexes(&self, table_id: u32) -> Vec<usize> {
        let mut columns: Vec<usize> = self
            .catalog
            .indexes
            .iter()
            .filter(|(table, _)| *table == table_id)
            .map(|(_, column)| *column as usize)
            .collect();
        if let Some(txn) = &self.txn {
            for (table, column) in &txn.new_indexes {
                if *table == table_id && !columns.contains(column) {
                    columns.push(*column);
                }
            }
        }
        columns.sort_unstable();
        columns.dedup();
        columns
    }

    fn in_txn(&self) -> bool {
        self.txn.is_some()
    }

    fn begin(&mut self) -> DbResult<()> {
        if self.txn.is_some() {
            return Err(DbError::new(ERR_TX_STATE, "transaction already active"));
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
            .ok_or_else(|| DbError::new(ERR_TX_STATE, "no active transaction"))?;
        if txn.writes.is_empty() && txn.new_tables.is_empty() && txn.new_indexes.is_empty() {
            return Ok(());
        }
        self.write_txn_record(&txn)?;
        self.apply_txn(txn);
        self.maybe_flush();
        Ok(())
    }

    fn rollback(&mut self) -> DbResult<()> {
        if self.txn.take().is_none() {
            return Err(DbError::new(ERR_TX_STATE, "no active transaction"));
        }
        Ok(())
    }

    fn statement_mark(&mut self) {
        if let Some(txn) = &mut self.txn {
            txn.mark();
        }
    }

    fn rollback_statement(&mut self) -> DbResult<()> {
        let Some(txn) = &mut self.txn else {
            return Ok(());
        };
        while txn.undo.len() > txn.undo_mark {
            let (table_id, pk, previous) = txn.undo.pop().unwrap();
            match previous {
                None => {
                    txn.writes.remove(&(table_id, pk));
                }
                Some(value) => {
                    txn.writes.insert((table_id, pk), value);
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
        if let Some(map) = self.committed.get(&table_id) {
            if let Some(value) = map.get(&pk) {
                return Ok(value.clone());
            }
        }
        self.lookup_row(table_id, pk)
    }

    fn put(&mut self, table_id: u32, pk: i64, row: Row) -> DbResult<()> {
        if let Some(txn) = &mut self.txn {
            let key = (table_id, pk);
            let previous = txn.writes.get(&key).cloned();
            txn.undo.push((table_id, pk, previous));
            txn.writes.insert(key, Some(row));
            return Ok(());
        }
        self.auto_commit_ops(&[WalOp::Put { table_id, pk, row }])
    }

    fn delete(&mut self, table_id: u32, pk: i64) -> DbResult<bool> {
        let existed = self.get(table_id, pk)?.is_some();
        if let Some(txn) = &mut self.txn {
            let key = (table_id, pk);
            let previous = txn.writes.get(&key).cloned();
            txn.undo.push((table_id, pk, previous));
            txn.writes.insert(key, None);
            return Ok(existed);
        }
        self.auto_commit_ops(&[WalOp::Delete { table_id, pk }])?;
        Ok(existed)
    }

    fn scan(&self, table_id: u32, f: &mut dyn FnMut(i64, &Row) -> bool) -> DbResult<()> {
        self.scan_table(table_id, f)
    }

    fn flush(&mut self) -> DbResult<()> {
        if self.txn.is_none() {
            self.flush_and_compact()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColType, Value};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> TempDir {
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("target");
            path.push("disk-storage-tests");
            path.push(format!("mvd-seg-{}-{}-{}-{}", tag, std::process::id(), nanos, unique));
            fs::create_dir_all(&path).unwrap();
            TempDir { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn table_spec() -> (Vec<ColumnDef>, usize) {
        (
            vec![
                ColumnDef { name: "id".to_string(), ty: ColType::BigInt, is_pk: true },
                ColumnDef { name: "content".to_string(), ty: ColType::Varchar(32), is_pk: false },
                ColumnDef { name: "embedding".to_string(), ty: ColType::Vector(2), is_pk: false },
            ],
            0,
        )
    }

    fn make_row(pk: i64) -> Row {
        vec![
            Value::Int(pk),
            Value::Str(format!("row-{}", pk)),
            Value::Vec(vec![pk as f32, -(pk as f32)]),
        ]
    }

    fn pks(storage: &DiskStorage, table_id: u32) -> Vec<i64> {
        let mut out = Vec::new();
        storage
            .scan(table_id, &mut |pk, _| {
                out.push(pk);
                true
            })
            .unwrap();
        out
    }

    /// Force a flush on every commit and a small merge budget, so tests exercise
    /// many segments and bounded merges quickly.
    fn enable_eager_flushing(storage: &mut DiskStorage, wal_bytes: u64, merge_bytes: u64, target: usize) {
        storage.wal_bytes = wal_bytes;
        storage.flush_rows = 1_000_000_000;
        storage.merge_bytes = merge_bytes;
        storage.target_segments = target;
    }

    #[test]
    fn fresh_directory_opens_empty() {
        let dir = TempDir::new("fresh");
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert!(storage.list_tables().is_empty());
        assert_eq!(storage.generation(), 1);
        assert_eq!(storage.segment_count(), 0);
        assert!(dir.path.join(MANIFEST_FILE).exists());
        assert_eq!(storage.get(1, 1).unwrap(), None);
        assert_eq!(pks(&storage, 1), Vec::<i64>::new());
    }

    #[test]
    fn reopen_restores_catalog_rows_and_index_metadata() {
        let dir = TempDir::new("reopen");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            let def = storage.create_table("docs", columns, pk).unwrap();
            table_id = def.id;
            storage.put(table_id, 2, make_row(2)).unwrap();
            storage.put(table_id, 1, make_row(1)).unwrap();
            storage.create_vector_index(table_id, 2).unwrap();
            storage.flush().unwrap();
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        let def = storage.table("docs").expect("catalog must be restored");
        assert_eq!(def.id, table_id);
        assert_eq!(def.columns.len(), 3);
        assert_eq!(def.columns[2].ty, ColType::Vector(2));
        assert_eq!(pks(&storage, table_id), vec![1, 2]);
        assert_eq!(storage.get(table_id, 1).unwrap().unwrap()[1], Value::Str("row-1".to_string()));
        assert_eq!(storage.vector_indexes(table_id), vec![2]);
        assert_eq!(storage.list_tables().len(), 1);
        assert_eq!(storage.segment_count(), 1);
    }

    #[test]
    fn commit_without_flush_survives_simulated_kill() {
        let dir = TempDir::new("kill");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            storage.begin().unwrap();
            for pk in 0..100i64 {
                storage.put(table_id, pk, make_row(pk)).unwrap();
            }
            storage.commit().unwrap();
            // No flush: drop simulates a hard kill right after the commit ack.
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(pks(&storage, table_id), (0..100).collect::<Vec<_>>());
        assert_eq!(storage.committed_row_count(), 100);
        assert_eq!(storage.segment_count(), 0);
    }

    #[test]
    fn transaction_rollback_never_reaches_disk() {
        let dir = TempDir::new("rollback");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            storage.put(table_id, 1, make_row(1)).unwrap();
            storage.begin().unwrap();
            storage.put(table_id, 1, make_row(10)).unwrap();
            storage.put(table_id, 2, make_row(2)).unwrap();
            assert!(storage.in_txn());
            assert_eq!(storage.get(table_id, 2).unwrap().unwrap()[0], Value::Int(2));
            storage.rollback().unwrap();
            assert_eq!(storage.get(table_id, 1).unwrap().unwrap()[0], Value::Int(1));
            assert_eq!(storage.get(table_id, 2).unwrap(), None);
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(pks(&storage, table_id), vec![1]);
        assert_eq!(storage.get(table_id, 1).unwrap().unwrap()[0], Value::Int(1));
    }

    #[test]
    fn statement_rollback_keeps_earlier_transaction_writes() {
        let dir = TempDir::new("stmt");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            storage.begin().unwrap();
            storage.put(table_id, 1, make_row(1)).unwrap();
            storage.statement_mark();
            storage.put(table_id, 2, make_row(2)).unwrap();
            storage.put(table_id, 1, make_row(100)).unwrap();
            storage.delete(table_id, 1).unwrap();
            storage.rollback_statement().unwrap();
            assert_eq!(storage.get(table_id, 1).unwrap().unwrap()[0], Value::Int(1));
            assert_eq!(storage.get(table_id, 2).unwrap(), None);
            storage.statement_mark();
            storage.put(table_id, 3, make_row(3)).unwrap();
            storage.commit().unwrap();
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(pks(&storage, table_id), vec![1, 3]);
        assert_eq!(storage.get(table_id, 1).unwrap().unwrap()[1], Value::Str("row-1".to_string()));
    }

    #[test]
    fn torn_wal_tail_is_truncated_and_later_commits_survive() {
        let dir = TempDir::new("torn");
        let table_id;
        let generation;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            storage.put(table_id, 1, make_row(1)).unwrap();
            storage.flush().unwrap();
            generation = storage.generation();
        }
        let wal = wal_path(&dir.path, generation);
        {
            let mut file = OpenOptions::new().append(true).open(&wal).unwrap();
            let mut header = [0u8; format::HEADER_LEN];
            header[0..4].copy_from_slice(&format::RECORD_MAGIC.to_le_bytes());
            header[4..8].copy_from_slice(&100u32.to_le_bytes());
            header[8..12].copy_from_slice(&format::KIND_TXN.to_le_bytes());
            file.write_all(&header).unwrap();
            file.write_all(b"partial").unwrap();
        }
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            assert_eq!(storage.get(table_id, 1).unwrap().unwrap()[0], Value::Int(1));
            assert_eq!(storage.wal_len(), 0);
            storage.put(table_id, 2, make_row(2)).unwrap();
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(pks(&storage, table_id), vec![1, 2]);
    }

    #[test]
    fn multiple_segments_merge_and_recover() {
        let dir = TempDir::new("multiseg");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            enable_eager_flushing(&mut storage, 2048, 64 * 1024, 4);
            for batch in 0..20i64 {
                storage.begin().unwrap();
                for offset in 0..20i64 {
                    let pk = batch * 20 + offset;
                    storage.put(table_id, pk, make_row(pk)).unwrap();
                }
                storage.commit().unwrap();
            }
            // Update some rows that now live in older segments, delete others.
            storage.put(table_id, 5, make_row(5000)).unwrap();
            storage.delete(table_id, 6).unwrap();
            storage.flush().unwrap();
            assert!(storage.generation() > 10);
            assert!(
                storage.segment_count() <= 6,
                "segment count must stay bounded, got {}",
                storage.segment_count()
            );
            let keys = pks(&storage, table_id);
            assert_eq!(keys.len(), 399);
            assert_eq!(storage.get(table_id, 5).unwrap().unwrap()[0], Value::Int(5000));
            assert_eq!(storage.get(table_id, 6).unwrap(), None);
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        let keys = pks(&storage, table_id);
        assert_eq!(keys.len(), 399);
        assert_eq!(storage.get(table_id, 5).unwrap().unwrap()[0], Value::Int(5000));
        assert_eq!(storage.get(table_id, 6).unwrap(), None);
        assert_eq!(storage.get(table_id, 399).unwrap().unwrap()[0], Value::Int(399));
    }

    #[test]
    fn deletes_and_updates_shadow_older_segments() {
        let dir = TempDir::new("shadow");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            enable_eager_flushing(&mut storage, 512, 256 * 1024, 8);
            for pk in 0..10i64 {
                storage.put(table_id, pk, make_row(pk)).unwrap();
            }
            storage.flush().unwrap();
            // Newer segment: overwrite half, delete the other half.
            for pk in 0..10i64 {
                if pk % 2 == 0 {
                    storage.put(table_id, pk, make_row(1000 + pk)).unwrap();
                } else {
                    storage.delete(table_id, pk).unwrap();
                }
            }
            storage.flush().unwrap();
            assert_eq!(pks(&storage, table_id), vec![0, 2, 4, 6, 8]);
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(pks(&storage, table_id), vec![0, 2, 4, 6, 8]);
        assert_eq!(storage.get(table_id, 0).unwrap().unwrap()[0], Value::Int(1000));
        assert_eq!(storage.get(table_id, 1).unwrap(), None);
        assert_eq!(storage.get(table_id, 8).unwrap().unwrap()[0], Value::Int(1008));
    }

    #[test]
    fn middle_run_merge_preserves_tombstones_over_older_segment() {
        let dir = TempDir::new("midmerge");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            // Flush every commit, merge only runs that fit 4 KiB. The first
            // segment is much larger than the budget, so merges start after it.
            storage.wal_bytes = 1;
            storage.flush_rows = 1_000_000_000;
            storage.merge_bytes = 4096;
            storage.target_segments = 2;
            storage.begin().unwrap();
            for pk in 0..400i64 {
                storage.put(table_id, pk, make_row(pk)).unwrap();
            }
            storage.commit().unwrap();
            storage.flush().unwrap();
            assert_eq!(storage.segment_count(), 1);
            // Delete rows that only exist in that first segment; each delete
            // lands in its own tiny segment, which then gets merged.
            for pk in 0..20i64 {
                storage.delete(table_id, pk).unwrap();
            }
            storage.flush().unwrap();
            assert!(storage.segment_count() >= 2);
            for pk in 0..20i64 {
                assert_eq!(storage.get(table_id, pk).unwrap(), None, "row {} resurrected", pk);
            }
            let keys = pks(&storage, table_id);
            assert_eq!(keys.len(), 380);
            assert_eq!(keys[0], 20);
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        for pk in 0..20i64 {
            assert_eq!(storage.get(table_id, pk).unwrap(), None, "row {} resurrected after reopen", pk);
        }
        assert_eq!(pks(&storage, table_id).len(), 380);
    }

    #[test]
    fn segment_count_stays_bounded_under_repeated_flushes() {
        let dir = TempDir::new("bounded");
        let mut storage = DiskStorage::open(&dir.path).unwrap();
        let (columns, pk) = table_spec();
        let table_id = storage.create_table("t", columns, pk).unwrap().id;
        enable_eager_flushing(&mut storage, 4096, 128 * 1024, 4);
        let mut worst = Duration::from_millis(0);
        for pk in 0..600i64 {
            storage.begin().unwrap();
            storage.put(table_id, pk, make_row(pk)).unwrap();
            let started = Instant::now();
            storage.commit().unwrap();
            let elapsed = started.elapsed();
            if elapsed > worst {
                worst = elapsed;
            }
            assert!(
                storage.segment_count() <= 6,
                "segment count grew to {}",
                storage.segment_count()
            );
        }
        assert_eq!(pks(&storage, table_id), (0..600).collect::<Vec<_>>());
        assert!(
            worst < Duration::from_secs(5),
            "single commit took {:?}, suspiciously slow for a bounded flush",
            worst
        );
    }

    #[test]
    fn row_cache_does_not_alias_tables_with_the_same_primary_key() {
        let dir = TempDir::new("alias");
        let (left, right);
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            left = storage.create_table("a", columns, pk).unwrap().id;
            let columns = vec![
                ColumnDef { name: "id".to_string(), ty: ColType::BigInt, is_pk: true },
                ColumnDef { name: "value".to_string(), ty: ColType::Varchar(32), is_pk: false },
            ];
            right = storage.create_table("b", columns, 0).unwrap().id;
            storage
                .put(left, 1, vec![Value::Int(1), Value::Str("left".to_string()), Value::Null])
                .unwrap();
            storage
                .put(right, 1, vec![Value::Int(1), Value::Str("right".to_string())])
                .unwrap();
            storage.flush().unwrap();
            // Warm the cache through one table, then read the other.
            assert_eq!(storage.get(left, 1).unwrap().unwrap()[1], Value::Str("left".to_string()));
            assert_eq!(storage.get(right, 1).unwrap().unwrap()[1], Value::Str("right".to_string()));
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(storage.get(left, 1).unwrap().unwrap()[1], Value::Str("left".to_string()));
        assert_eq!(storage.get(right, 1).unwrap().unwrap()[1], Value::Str("right".to_string()));
        let mut right_rows = Vec::new();
        storage
            .scan(right, &mut |_, row| {
                right_rows.push(row.clone());
                true
            })
            .unwrap();
        assert_eq!(right_rows.len(), 1);
        assert_eq!(right_rows[0][1], Value::Str("right".to_string()));
    }

    #[test]
    fn orphaned_segment_and_temp_files_are_ignored() {
        let dir = TempDir::new("orphans");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            storage.put(table_id, 1, make_row(1)).unwrap();
            storage.flush().unwrap();
        }
        // A crash after writing the merged segment but before the manifest switch
        // leaves an unreferenced segment plus a temp manifest behind.
        fs::write(dir.path.join("seg-999.dat"), b"garbage").unwrap();
        fs::write(dir.path.join("seg-999.idx"), b"garbage").unwrap();
        fs::write(dir.path.join("MANIFEST.tmp"), b"garbage").unwrap();
        fs::write(dir.path.join("wal-999.log"), b"garbage").unwrap();
        for _ in 0..2 {
            let storage = DiskStorage::open(&dir.path).unwrap();
            assert_eq!(pks(&storage, table_id), vec![1]);
        }
        for entry in fs::read_dir(&dir.path).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            assert!(!name.contains("999"), "orphan survived: {}", name);
            assert!(!name.ends_with(".tmp"), "temp file survived: {}", name);
        }
    }

    #[test]
    fn get_and_scan_see_own_transaction_writes() {
        let dir = TempDir::new("readwrite");
        let mut storage = DiskStorage::open(&dir.path).unwrap();
        let (columns, pk) = table_spec();
        let table_id = storage.create_table("t", columns, pk).unwrap().id;
        storage.put(table_id, 5, make_row(5)).unwrap();
        storage.begin().unwrap();
        storage.put(table_id, 3, make_row(3)).unwrap();
        storage.delete(table_id, 5).unwrap();
        assert_eq!(pks(&storage, table_id), vec![3]);
        assert_eq!(storage.get(table_id, 3).unwrap().unwrap()[0], Value::Int(3));
        assert_eq!(storage.get(table_id, 5).unwrap(), None);
        storage.rollback().unwrap();
        assert_eq!(pks(&storage, table_id), vec![5]);
    }

    #[test]
    fn ddl_inside_transaction_rolls_back() {
        let dir = TempDir::new("ddl");
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            storage.begin().unwrap();
            let (columns, pk) = table_spec();
            assert!(storage.create_table("ghost", columns, pk).is_ok());
            assert!(storage.table("ghost").is_some());
            storage.rollback().unwrap();
            assert!(storage.table("ghost").is_none());
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert!(storage.table("ghost").is_none());
        assert!(storage.list_tables().is_empty());
    }

    #[test]
    fn multiple_tables_persist_independently() {
        let dir = TempDir::new("tables");
        let (docs, tags);
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            docs = storage.create_table("docs", columns, pk).unwrap().id;
            let (columns, pk) = table_spec();
            tags = storage.create_table("tags", columns, pk).unwrap().id;
            storage.put(docs, 3, make_row(3)).unwrap();
            storage.put(tags, 1, make_row(1)).unwrap();
            storage.put(tags, 2, make_row(2)).unwrap();
            storage.flush().unwrap();
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(storage.list_tables().len(), 2);
        assert_eq!(pks(&storage, docs), vec![3]);
        assert_eq!(pks(&storage, tags), vec![1, 2]);
    }

    #[test]
    fn repeated_scans_use_the_row_cache() {
        let dir = TempDir::new("cache");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            storage.begin().unwrap();
            for pk in 0..200i64 {
                storage.put(table_id, pk, make_row(pk)).unwrap();
            }
            storage.commit().unwrap();
            storage.flush().unwrap();
            assert_eq!(storage.cached_row_count(), 0);
            assert_eq!(pks(&storage, table_id).len(), 200);
            assert_eq!(storage.cached_row_count(), 200);
            let mut seen = 0;
            storage
                .scan(table_id, &mut |_, _| {
                    seen += 1;
                    true
                })
                .unwrap();
            assert_eq!(seen, 200);
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(pks(&storage, table_id).len(), 200);
    }

    #[test]
    fn large_table_write_scan_and_reopen() {
        let dir = TempDir::new("large");
        let count = 200_000i64;
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("big", columns, pk).unwrap().id;
            storage.begin().unwrap();
            for pk in (0..count).rev() {
                storage.put(table_id, pk, make_row(pk)).unwrap();
            }
            storage.commit().unwrap();
            assert_eq!(pks(&storage, table_id), (0..count).collect::<Vec<_>>());
            storage.flush().unwrap();
            assert_eq!(storage.committed_row_count(), 0, "flush must empty the memory overlay");
            assert_eq!(storage.get(table_id, count / 2).unwrap().unwrap()[0], Value::Int(count / 2));
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        let mut seen = 0i64;
        let mut previous = i64::MIN;
        let mut ordered = true;
        storage
            .scan(table_id, &mut |pk, _| {
                if pk <= previous {
                    ordered = false;
                }
                previous = pk;
                seen += 1;
                true
            })
            .unwrap();
        assert!(ordered);
        assert_eq!(seen, count);
    }
}
