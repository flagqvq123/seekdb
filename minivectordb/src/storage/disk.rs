//! Durable storage engine: generation-based checkpoints plus a write-ahead log.
//!
//! Layout under the data directory:
//!
//!   CURRENT                u64 generation number of the live checkpoint
//!   ckpt-<gen>.dat         row frames sorted by (table id, primary key)
//!   ckpt-<gen>.idx         table id -> sorted (pk, frame offset, payload len)
//!   ckpt-<gen>.cat         catalog snapshot (tables, vector index metadata)
//!   wal-<gen>.log          committed transactions not yet folded into a ckpt
//!
//! A commit appends one self-describing, checksummed transaction record to the
//! current WAL and fsyncs it before returning, so an acknowledged commit
//! survives an immediate kill. A checkpoint merges ckpt + WAL into
//! ckpt-<gen+1>.*, fsyncs every file, creates an empty wal-<gen+1>.log and only
//! then atomically switches CURRENT (write CURRENT.tmp, fsync, rename). A crash
//! anywhere before the switch leaves the previous generation (plus its WAL)
//! intact; a crash after it leaves the new generation intact and the old files
//! are garbage collected on the next open. Recovery truncates a torn WAL tail at
//! the first bad frame and is therefore idempotent.
//!
//! Reads are served by a bounded FIFO row cache; cache misses fall back to a
//! buffered, strictly sequential read of the checkpoint data file. Only the
//! primary-key -> file-position index and the cache are resident, so tables
//! larger than memory can still be scanned in primary-key order.

use std::cell::RefCell;
use std::collections::btree_map;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::format::{self, Slot, WalOp};
use super::Storage;
use crate::types::{ColumnDef, DbError, DbResult, Row, TableDef, ERR_INTERNAL, ERR_TX_STATE};

const CURRENT_FILE: &str = "CURRENT";
const CURRENT_TEMP: &str = "CURRENT.tmp";

/// Bytes of WAL tolerated before a checkpoint is forced.
const DEFAULT_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;
/// Committed rows tolerated in memory before a checkpoint is forced.
const DEFAULT_CHECKPOINT_ROWS: usize = 512 * 1024;
/// Default row-cache budget (bytes of decoded rows kept resident).
const DEFAULT_CACHE_BYTES: usize = 128 * 1024 * 1024;
const READ_BUFFER: usize = 1 << 20;
const WRITE_BUFFER: usize = 1 << 20;

/// Crash-safe storage engine.
pub struct DiskStorage {
    dir: PathBuf,
    generation: u64,
    catalog: Catalog,
    /// Checkpoint rows: table id -> slots sorted by primary key.
    base: HashMap<u32, Vec<Slot>>,
    /// Committed but not yet checkpointed rows. None is a tombstone.
    committed: HashMap<u32, BTreeMap<i64, Option<Row>>>,
    committed_rows: usize,
    wal: Wal,
    data_path: PathBuf,
    data_file: RefCell<File>,
    cache: RefCell<RowCache>,
    txn: Option<Txn>,
    checkpoint_bytes: u64,
    checkpoint_rows: usize,
}

impl DiskStorage {
    /// Open (or create) a database directory, replaying the WAL before return.
    pub fn open(dir: &Path) -> DbResult<Self> {
        fs::create_dir_all(dir)?;
        let generation = match read_current(dir)? {
            Some(generation) => generation,
            None => {
                initialize_generation(dir)?;
                1
            }
        };
        let mut storage = DiskStorage::load(dir, generation)?;
        storage.recover()?;
        storage.remove_stale_files()?;
        Ok(storage)
    }

    /// Build the on-disk/in-memory state for one generation (no WAL replay).
    fn load(dir: &Path, generation: u64) -> DbResult<Self> {
        let catalog_body =
            read_container_file(&checkpoint_path(dir, generation, "cat"), format::CATALOG_MAGIC)?;
        let catalog = Catalog::from_image(format::decode_catalog(&catalog_body)?);
        let base = read_index_file(&checkpoint_path(dir, generation, "idx"))?;
        let data_path = checkpoint_path(dir, generation, "dat");
        let data_file = File::open(&data_path)?;
        let data_len = data_file.metadata()?.len();
        for slots in base.values() {
            let mut previous: Option<i64> = None;
            for slot in slots {
                let end = slot.off.saturating_add(format::frame_total_len(slot.len as u64));
                if end > data_len {
                    return Err(format::corrupt("checkpoint data file is shorter than its index"));
                }
                if let Some(previous) = previous {
                    if slot.pk <= previous {
                        return Err(format::corrupt("checkpoint index is not sorted by primary key"));
                    }
                }
                previous = Some(slot.pk);
            }
        }
        let wal_path = wal_path(dir, generation);
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
            generation,
            catalog,
            base,
            committed: HashMap::new(),
            committed_rows: 0,
            wal,
            data_path,
            data_file: RefCell::new(data_file),
            cache: RefCell::new(RowCache::new(cache_budget_from_env())),
            txn: None,
            checkpoint_bytes: checkpoint_bytes_from_env(),
            checkpoint_rows: DEFAULT_CHECKPOINT_ROWS,
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
            // Torn tail: drop everything after the last durable record.
            file.set_len(position)?;
            file.sync_all()?;
        }
        self.wal = Wal::open(&path, position)?;
        Ok(())
    }

    /// Remove files that do not belong to the live generation.
    fn remove_stale_files(&mut self) -> DbResult<()> {
        let keep = [
            CURRENT_FILE.to_string(),
            format!("ckpt-{}.dat", self.generation),
            format!("ckpt-{}.idx", self.generation),
            format!("ckpt-{}.cat", self.generation),
            format!("wal-{}.log", self.generation),
        ];
        let mut stale = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if keep.iter().any(|kept| *kept == name) {
                continue;
            }
            let ours = name.starts_with("ckpt-")
                || name.starts_with("wal-")
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
                    self.cache.borrow_mut().remove((*table_id, *pk));
                }
                WalOp::Delete { table_id, pk } => {
                    self.committed.entry(*table_id).or_default().insert(*pk, None);
                    self.committed_rows += 1;
                    self.cache.borrow_mut().remove((*table_id, *pk));
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
            self.cache.borrow_mut().remove((table_id, pk));
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
        self.maybe_checkpoint();
        Ok(())
    }

    fn maybe_checkpoint(&mut self) {
        if self.txn.is_some() {
            return;
        }
        if self.wal.len < self.checkpoint_bytes && self.committed_rows < self.checkpoint_rows {
            return;
        }
        if let Err(err) = self.checkpoint() {
            eprintln!("minivectordb: checkpoint failed: {}", err);
        }
    }

/// Fold the checkpoint plus committed WAL state into generation + 1.
    fn checkpoint(&mut self) -> DbResult<()> {
        if self.txn.is_some() {
            return Ok(());
        }
        if self.committed.is_empty() && self.wal.len == 0 {
            return Ok(());
        }
        let new_generation = self.generation + 1;
        let dat_final = checkpoint_path(&self.dir, new_generation, "dat");
        let idx_final = checkpoint_path(&self.dir, new_generation, "idx");
        let cat_final = checkpoint_path(&self.dir, new_generation, "cat");
        let dat_temp = checkpoint_path(&self.dir, new_generation, "dat.tmp");
        let idx_temp = checkpoint_path(&self.dir, new_generation, "idx.tmp");
        let cat_temp = checkpoint_path(&self.dir, new_generation, "cat.tmp");

        let mut new_base: HashMap<u32, Vec<Slot>> = HashMap::with_capacity(self.base.len());
        {
            let data_path = self.data_path.clone();
            let mut reader = RecordReader::open(&data_path)?;
            let mut scratch: Vec<u8> = Vec::new();
            let mut data_position = 0u64;
            let file = File::create(&dat_temp)?;
            let mut writer = BufWriter::with_capacity(WRITE_BUFFER, file);
            for def in &self.catalog.tables {
                let table_id = def.id;
                let base_slots: &[Slot] =
                    self.base.get(&table_id).map(|slots| slots.as_slice()).unwrap_or(&[]);
                let overlay: Option<&BTreeMap<i64, Option<Row>>> = self.committed.get(&table_id);
                let mut slots: Vec<Slot> = Vec::with_capacity(base_slots.len());
                let mut overlay_iter = overlay.map(|map| map.iter().peekable());
                let mut base_index = 0usize;
                loop {
                    let base_pk = base_slots.get(base_index).map(|slot| slot.pk);
                    let overlay_pk =
                        overlay_iter.as_mut().and_then(|iter| iter.peek().map(|(pk, _)| **pk));
                    let pk = match (base_pk, overlay_pk) {
                        (None, None) => break,
                        (Some(pk), None) => pk,
                        (None, Some(pk)) => pk,
                        (Some(base), Some(overlay)) => base.min(overlay),
                    };
                    let value: Option<Row> = if overlay_pk == Some(pk) {
                        let entry = overlay_iter.as_mut().unwrap().next().unwrap();
                        let value = entry.1.clone();
                        if base_pk == Some(pk) {
                            base_index += 1;
                        }
                        value
                    } else {
                        let slot = base_slots[base_index];
                        base_index += 1;
                        Some(reader.read_row(slot)?)
                    };
                    if let Some(row) = &value {
                        scratch.clear();
                        format::encode_row_into(&mut scratch, row);
                        format::write_frame(&mut writer, format::KIND_ROW, &scratch)?;
                        slots.push(Slot { pk, off: data_position, len: scratch.len() as u32 });
                        data_position += format::frame_total_len(scratch.len() as u64);
                    }
                }
                if !slots.is_empty() {
                    new_base.insert(table_id, slots);
                }
            }
            let file = writer.into_inner().map_err(|err| DbError::from(err.into_error()))?;
            file.sync_all()?;
        }

        write_index_file(&idx_temp, &new_base)?;
        let catalog_body = format::encode_catalog(&format::CatalogImage {
            tables: self.catalog.tables.clone(),
            indexes: self.catalog.indexes.clone(),
            next_table_id: self.catalog.next_table_id,
        });
        write_container_file(&cat_temp, format::CATALOG_MAGIC, &catalog_body)?;

        fs::rename(&dat_temp, &dat_final)?;
        fs::rename(&idx_temp, &idx_final)?;
        fs::rename(&cat_temp, &cat_final)?;
        sync_directory(&self.dir);

        let new_wal_path = wal_path(&self.dir, new_generation);
        File::create(&new_wal_path)?.sync_all()?;

        // Everything fallible happens before the single atomic switch point.
        let new_data_file = File::open(&dat_final)?;
        let new_wal = Wal::open(&new_wal_path, 0)?;
        write_current(&self.dir, new_generation)?;

        let old_generation = self.generation;
        self.generation = new_generation;
        self.base = new_base;
        self.committed.clear();
        self.committed_rows = 0;
        self.data_path = dat_final;
        self.data_file = RefCell::new(new_data_file);
        self.cache.borrow_mut().clear();
        self.wal = new_wal;

        for extension in ["dat", "idx", "cat"] {
            let _ = fs::remove_file(checkpoint_path(&self.dir, old_generation, extension));
        }
        let _ = fs::remove_file(wal_path(&self.dir, old_generation));
        Ok(())
    }

    fn read_slot(&self, slot: Slot) -> DbResult<Row> {
        let mut file = self.data_file.borrow_mut();
        file.seek(SeekFrom::Start(slot.off))?;
        let (kind, payload) = format::read_frame(&mut *file, slot.len)?;
        if kind != format::KIND_ROW {
            return Err(format::corrupt("unexpected record kind in checkpoint data file"));
        }
        format::decode_row(&payload)
    }

    fn cached_row(&self, table_id: u32, slot: Slot) -> DbResult<Arc<Row>> {
        let key = (table_id, slot.pk);
        let cached = self.cache.borrow().get(key);
        if let Some(row) = cached {
            return Ok(row);
        }
        let row = Arc::new(self.read_slot(slot)?);
        let size = format::encoded_row_len(&row) + 48;
        self.cache.borrow_mut().insert(key, Arc::clone(&row), size);
        Ok(row)
    }

    fn base_lookup(&self, table_id: u32, pk: i64) -> Option<Slot> {
        let slots = self.base.get(&table_id)?;
        match slots.binary_search_by_key(&pk, |slot| slot.pk) {
            Ok(index) => Some(slots[index]),
            Err(_) => None,
        }
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
// Bounded FIFO row cache
// ---------------------------------------------------------------------------

struct RowCache {
    budget: usize,
    used: usize,
    rows: HashMap<(u32, i64), (Arc<Row>, usize)>,
    order: VecDeque<(u32, i64)>,
}

impl RowCache {
    fn new(budget: usize) -> RowCache {
        RowCache { budget, used: 0, rows: HashMap::new(), order: VecDeque::new() }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.rows.len()
    }

    fn get(&self, key: (u32, i64)) -> Option<Arc<Row>> {
        self.rows.get(&key).map(|(row, _)| Arc::clone(row))
    }

    fn insert(&mut self, key: (u32, i64), row: Arc<Row>, size: usize) {
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

    fn remove(&mut self, key: (u32, i64)) {
        if let Some((_, size)) = self.rows.remove(&key) {
            self.used = self.used.saturating_sub(size);
        }
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.order.clear();
        self.used = 0;
    }
}

// ---------------------------------------------------------------------------
// Buffered sequential reader for the checkpoint data file
// ---------------------------------------------------------------------------

struct RecordReader {
    reader: BufReader<File>,
    position: u64,
}

impl RecordReader {
    fn open(path: &Path) -> DbResult<RecordReader> {
        Ok(RecordReader {
            reader: BufReader::with_capacity(READ_BUFFER, File::open(path)?),
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
            return Err(format::corrupt("unexpected record kind in checkpoint data file"));
        }
        self.position += format::frame_total_len(slot.len as u64);
        format::decode_row(&payload)
    }
}

// ---------------------------------------------------------------------------
// File naming and durable directory helpers
// ---------------------------------------------------------------------------

fn checkpoint_path(dir: &Path, generation: u64, extension: &str) -> PathBuf {
    dir.join(format!("ckpt-{}.{}", generation, extension))
}

fn wal_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("wal-{}.log", generation))
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

fn checkpoint_bytes_from_env() -> u64 {
    match std::env::var("MINIVECTORDB_CHECKPOINT_MIB") {
        Ok(text) => match text.trim().parse::<u64>() {
            Ok(mib) if mib > 0 => mib.saturating_mul(1024 * 1024),
            _ => DEFAULT_CHECKPOINT_BYTES,
        },
        Err(_) => DEFAULT_CHECKPOINT_BYTES,
    }
}

fn read_current(dir: &Path) -> DbResult<Option<u64>> {
    match fs::read_to_string(dir.join(CURRENT_FILE)) {
        Ok(text) => text
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format::corrupt("CURRENT file is corrupt")),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn initialize_generation(dir: &Path) -> DbResult<()> {
    let generation = 1u64;
    File::create(checkpoint_path(dir, generation, "dat"))?.sync_all()?;
    let catalog_body = format::encode_catalog(&format::CatalogImage::default());
    write_container_file(&checkpoint_path(dir, generation, "cat"), format::CATALOG_MAGIC, &catalog_body)?;
    write_index_file(&checkpoint_path(dir, generation, "idx"), &HashMap::new())?;
    File::create(wal_path(dir, generation))?.sync_all()?;
    write_current(dir, generation)
}

fn write_current(dir: &Path, generation: u64) -> DbResult<()> {
    let temp = dir.join(CURRENT_TEMP);
    {
        let mut file = File::create(&temp)?;
        file.write_all(format!("{}\n", generation).as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&temp, dir.join(CURRENT_FILE))?;
    sync_directory(dir);
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
        .ok_or_else(|| format::corrupt("bad checkpoint file magic"))?;
    if header.version != format::CONTAINER_VERSION {
        return Err(format::corrupt("unsupported checkpoint version"));
    }
    if header.body_len > format::MAX_CATALOG_BODY {
        return Err(format::corrupt("checkpoint body is implausibly large"));
    }
    if header.body_len > file_len.saturating_sub(format::CONTAINER_HEADER_LEN as u64) {
        return Err(format::corrupt("checkpoint body is truncated"));
    }
    let mut body = vec![0u8; header.body_len as usize];
    file.read_exact(&mut body)?;
    if format::crc32(&body) != header.crc {
        return Err(format::corrupt("checkpoint body checksum mismatch"));
    }
    Ok(body)
}

fn write_index_file(path: &Path, base: &HashMap<u32, Vec<Slot>>) -> DbResult<()> {
    let mut file = File::create(path)?;
    file.write_all(&[0u8; format::CONTAINER_HEADER_LEN])?;
    let (body_len, body_crc) = {
        let mut tables: Vec<(&u32, &Vec<Slot>)> =
            base.iter().filter(|(_, slots)| !slots.is_empty()).collect();
        tables.sort_by_key(|(table_id, _)| **table_id);
        let slices: Vec<(u32, &[Slot])> =
            tables.iter().map(|(table_id, slots)| (**table_id, slots.as_slice())).collect();
        let mut writer = format::CrcWriter::new(BufWriter::with_capacity(WRITE_BUFFER, &mut file));
        format::write_index_body(&mut writer, &slices)?;
        let (mut buffered, written, checksum) = writer.finish();
        buffered.flush()?;
        (written, checksum)
    };
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&format::container_header(format::INDEX_MAGIC, body_len, body_crc))?;
    file.sync_all()?;
    Ok(())
}

fn read_index_file(path: &Path) -> DbResult<HashMap<u32, Vec<Slot>>> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut header_bytes = [0u8; format::CONTAINER_HEADER_LEN];
    file.read_exact(&mut header_bytes)?;
    let header = format::parse_container_header(&header_bytes, format::INDEX_MAGIC)
        .ok_or_else(|| format::corrupt("bad index magic"))?;
    if header.version != format::CONTAINER_VERSION {
        return Err(format::corrupt("unsupported index version"));
    }
    if header.body_len > file_len.saturating_sub(format::CONTAINER_HEADER_LEN as u64) {
        return Err(format::corrupt("checkpoint index is truncated"));
    }
    let mut base = HashMap::new();
    {
        let reader = BufReader::with_capacity(READ_BUFFER, &mut file);
        let mut crc_reader = format::CrcReader::new(reader, header.body_len);
        let tables = format::read_index_body(&mut crc_reader)?;
        let leftover = crc_reader.remaining();
        let (_, checksum) = crc_reader.finish();
        if leftover != 0 || checksum != header.crc {
            return Err(format::corrupt("checkpoint index checksum mismatch"));
        }
        for (table_id, slots) in tables {
            base.insert(table_id, slots);
        }
    }
    Ok(base)
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
        self.maybe_checkpoint();
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
        match self.base_lookup(table_id, pk) {
            Some(slot) => Ok(Some((*self.cached_row(table_id, slot)?).clone())),
            None => Ok(None),
        }
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
        let base_slots: &[Slot] = match self.base.get(&table_id) {
            Some(slots) => slots.as_slice(),
            None => &[],
        };
        let committed = self.committed.get(&table_id);
        if base_slots.is_empty() && committed.is_none() && overlay.is_empty() {
            return Ok(());
        }
        let mut base_reader = if base_slots.is_empty() {
            None
        } else {
            Some(RecordReader::open(&self.data_path)?)
        };
        let mut committed_iter: Option<btree_map::Iter<i64, Option<Row>>> = committed.map(|map| map.iter());
        let mut committed_next: Option<(i64, &Option<Row>)> = None;
        let mut base_index = 0usize;
        let mut overlay_index = 0usize;
        loop {
            if committed_next.is_none() {
                committed_next = committed_iter
                    .as_mut()
                    .and_then(|iter| iter.next().map(|(pk, value)| (*pk, value)));
            }
            let base_pk = base_slots.get(base_index).map(|slot| slot.pk);
            let committed_pk = committed_next.map(|(pk, _)| pk);
            let overlay_pk = overlay.get(overlay_index).map(|(pk, _)| *pk);
            let pk = match [base_pk, committed_pk, overlay_pk].into_iter().flatten().min() {
                Some(pk) => pk,
                None => break,
            };
            let mut decided = false;
            let mut borrowed: Option<&Row> = None;
            let mut shared: Option<Arc<Row>> = None;
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
            if base_pk == Some(pk) {
                let slot = base_slots[base_index];
                base_index += 1;
                if !decided {
                    let cached = self.cache.borrow().get((table_id, slot.pk));
                    if let Some(cached) = cached {
                        shared = Some(cached);
                    } else {
                        let reader = base_reader
                            .as_mut()
                            .ok_or_else(|| format::corrupt("missing checkpoint reader"))?;
                        let row = Arc::new(reader.read_row(slot)?);
                        let size = format::encoded_row_len(&row) + 48;
                        self.cache
                            .borrow_mut()
                            .insert((table_id, slot.pk), Arc::clone(&row), size);
                        shared = Some(row);
                    }
                }
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

    fn row_count_hint(&self, table_id: u32) -> Option<usize> {
        let base = self.base.get(&table_id).map(|slots| slots.len()).unwrap_or(0);
        let overlay = self.committed.get(&table_id).map(|rows| rows.len()).unwrap_or(0);
        Some(base + overlay)
    }

    fn flush(&mut self) -> DbResult<()> {
        if self.txn.is_none() {
            self.checkpoint()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColType, Value};
    use std::sync::atomic::{AtomicU64, Ordering};

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
            path.push(format!("mvd-disk-{}-{}-{}-{}", tag, std::process::id(), nanos, unique));
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

    #[test]
    fn fresh_directory_opens_empty() {
        let dir = TempDir::new("fresh");
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert!(storage.list_tables().is_empty());
        assert_eq!(storage.generation(), 1);
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
    fn multiple_checkpoints_recover_with_updates_and_deletes() {
        let dir = TempDir::new("ckpt");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            for batch in 0..4i64 {
                storage.begin().unwrap();
                for offset in 0..50i64 {
                    let pk = batch * 50 + offset;
                    storage.put(table_id, pk, make_row(pk)).unwrap();
                }
                storage.commit().unwrap();
                storage.flush().unwrap();
            }
            storage.put(table_id, 0, make_row(1000)).unwrap();
            storage.delete(table_id, 199).unwrap();
            storage.flush().unwrap();
            assert!(storage.generation() >= 5);
        }
        let storage = DiskStorage::open(&dir.path).unwrap();
        let keys = pks(&storage, table_id);
        assert_eq!(keys.len(), 199);
        assert_eq!(keys[0], 0);
        assert_eq!(storage.get(table_id, 0).unwrap().unwrap()[0], Value::Int(1000));
        assert_eq!(storage.get(table_id, 199).unwrap(), None);
        assert_eq!(storage.get(table_id, 150).unwrap().unwrap()[0], Value::Int(150));
    }

    #[test]
    fn checkpoint_bounds_wal_growth() {
        let dir = TempDir::new("bound");
        let mut storage = DiskStorage::open(&dir.path).unwrap();
        storage.checkpoint_bytes = 4096;
        storage.checkpoint_rows = 100;
        let (columns, pk) = table_spec();
        let table_id = storage.create_table("t", columns, pk).unwrap().id;
        for pk in 0..500i64 {
            storage.begin().unwrap();
            storage.put(table_id, pk, make_row(pk)).unwrap();
            storage.commit().unwrap();
        }
        assert!(storage.generation() > 2, "checkpoint should have been triggered");
        assert!(storage.wal_len() <= 8192, "WAL must stay bounded, got {}", storage.wal_len());
        assert_eq!(pks(&storage, table_id), (0..500).collect::<Vec<_>>());
        drop(storage);
        let storage = DiskStorage::open(&dir.path).unwrap();
        assert_eq!(pks(&storage, table_id), (0..500).collect::<Vec<_>>());
    }

    #[test]
    fn stale_files_from_interrupted_checkpoint_are_ignored() {
        let dir = TempDir::new("stale");
        let table_id;
        {
            let mut storage = DiskStorage::open(&dir.path).unwrap();
            let (columns, pk) = table_spec();
            table_id = storage.create_table("t", columns, pk).unwrap().id;
            storage.put(table_id, 1, make_row(1)).unwrap();
            storage.flush().unwrap();
        }
        for name in ["ckpt-99.dat", "ckpt-99.idx", "ckpt-99.cat", "wal-99.log", "ckpt-3.dat.tmp", "CURRENT.tmp"] {
            fs::write(dir.path.join(name), b"garbage").unwrap();
        }
        for _ in 0..2 {
            let storage = DiskStorage::open(&dir.path).unwrap();
            assert_eq!(pks(&storage, table_id), vec![1]);
        }
        for entry in fs::read_dir(&dir.path).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            assert!(!name.contains("99"), "stale file survived: {}", name);
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
            assert_eq!(storage.committed_row_count(), 0, "checkpoint must empty the memory overlay");
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




