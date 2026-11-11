# DiskStorage WIP: durable WAL + checkpoint storage (task-1)

Status as of 2026-11-11 20:10 (session cut short by shutdown).

- src/storage/format.rs is COMPLETE and self-contained (CRC32, record framing,
  row/catalog/index/WAL encoders + decoders, unit tests). It is not yet declared
  in src/storage/mod.rs, so cargo does not compile it yet. It is committed so the
  next session can wire it in immediately.
- src/storage/disk.rs is still the old delegate stub (crate compiles, cargo check
  EXIT=0). The full implementation was designed and partially written but the
  file write was rejected by the tooling at the end of the session, so nothing
  half-finished was left on disk.
- Next step (first thing next session): add 'mod format;' to src/storage/mod.rs
  (needs Lead approval, mod.rs is Lead-owned), then implement disk.rs per below
  and run cargo test.

## Why this design (driven by COMPETITION_SPEC.md)

Performance benchmark: 10,000 rows x 128-dim vectors, 100 batches of 100 rows,
one COMMIT per batch (100 fsyncs total), then 1,000 exact TopK queries (K=10,
half with an equality filter). Metrics: wall time T, /data physical bytes B,
container peak memory M (page cache included).

Design consequences:

1. One commit must write exactly one WAL record and call one fsync. Do not
   fsync or syscall per row. A 100-row batch becomes a single streamed record.
2. Queries must not re-read the whole table from disk. A bounded in-memory row
   cache (default 128 MiB, configurable) keeps recently scanned rows resident;
   the official data set (about 5 MB of vectors) fits entirely, so only the
   first scan pays disk cost.
3. Memory must stay bounded for data larger than RAM: base rows live in a
   checkpoint file; only pk -> file position index (20 bytes per row) plus the
   cache are resident; scans stream sequentially from the data file.
4. Checkpoint at shutdown and whenever WAL exceeds a threshold so that WAL
   growth stays bounded and restart replay stays cheap.

## Data directory layout (/data)

    CURRENT              decimal u64 generation number of the live checkpoint
    ckpt-G.dat           row frames sorted by (table id, primary key)
    ckpt-G.idx           table id -> sorted (pk, frame offset, payload len)
    ckpt-G.cat           catalog snapshot: tables, vector index metadata,
                         next_table_id
    wal-G.log            committed transactions not yet folded into a checkpoint

One generation owns exactly these four files. Stale ckpt-* / wal-* / *.tmp
files are deleted on open (only names with our prefixes, never user files).

## Record framing (shared by WAL and checkpoint data files)

    header  12 bytes: magic u32 (0x4D564452), payload len u32, kind u32
    payload len bytes
    trailer  4 bytes: CRC32(kind_le_bytes || payload)

Magic/len/kind let replay detect a tail that is missing or overruns the file;
the trailer checksum lets it detect a torn or corrupted final record. Replay
stops at the first bad frame and truncates the file to the last good offset.
Truncation is idempotent, so recovery may itself be killed and retried.

Trailer instead of an inline checksum is deliberate: the payload length is
known up front (every WAL op has a deterministic encoded size), so a large
transaction can be streamed straight to disk while the CRC is computed; the
checksum is written last. Nothing large is buffered in memory on the commit
path.

Kind values: 0 = committed transaction record (WAL), 1 = row record
(checkpoint data file).

## Encodings (src/storage/format.rs)

Row payload: u32 column count, then per value one tag byte:
  0 NULL; 1 INT + i64; 2 FLOAT + f64; 3 STRING + u32 len + utf8;
  4 VECTOR + u32 dim + dim x f32 (little-endian floats, f32 storage semantics).

Table definition: u32 id, u32 pk index, string name, u32 column count, then
per column: string name, u8 type tag, u32 type param. Type tags: 0 BIGINT,
1 VARCHAR(n), 2 VECTOR(d).

WAL transaction payload: u32 op count, ops, u32 next_table_id.
Ops: 1 PUT (u32 table, i64 pk, u32 row len, row payload); 2 DELETE (u32 table,
i64 pk); 3 CREATE TABLE (table definition); 4 CREATE INDEX (u32 table,
u32 column).

Catalog body: u32 table count, table definitions, u32 index count,
(u32 table, u32 column) pairs, u32 next_table_id.

Checkpoint index body: u32 table count; per table: u32 table id, u64 row
count, then count times (i64 pk, u64 frame offset, u32 payload len) = 20 bytes
per row, strictly ascending by pk (validated on load).

Catalog and index files are containers: 24-byte header
(magic u32, version u32, body len u64, body CRC32 u32, reserved u32) followed
by the body. The header is written last for the index file (placeholder header,
stream body, seek back to 0, write real header) so the body is never buffered
in memory.

## Checkpoint protocol (atomic switch)

1. Merge base slots plus the committed in-memory overlay table by table,
   primary key by primary key; drop tombstones and superseded rows.
2. Stream row frames into ckpt-(G+1).dat.tmp, recording slots in memory.
3. Write ckpt-(G+1).idx.tmp from those slots, then ckpt-(G+1).cat.tmp.
4. fsync all three files, rename each .tmp to its final name, fsync directory.
5. Create an empty wal-(G+1).log and fsync it.
6. Open the new data file and new WAL handle.
7. Write CURRENT.tmp with G+1, fsync it, rename over CURRENT, fsync directory.
   This rename is the single atomic switch point.
8. Only now update in-memory state (generation/base/cache/WAL) and delete the
   old generation files (best effort).

Crash before step 7: old generation plus its WAL replay to the committed state;
new generation leftovers are stale and deleted on next open.
Crash after step 7: new generation is complete and durable; the empty new WAL
is already present, old files are stale and deleted on next open.
All fallible work happens before the switch; after it only assignments and
best-effort deletes.

## Recovery (before the ready handshake)

1. Read CURRENT; if absent, initialize generation 1 (empty data, catalog,
   index, WAL) and fsync each file before writing CURRENT.
2. Load ckpt-G.cat, validate magic/version/body CRC; rebuild the catalog.
3. Load ckpt-G.idx, validate header CRC while streaming; build
   table id -> Vec of slots (sorted by pk, checked).
4. Open ckpt-G.dat; validate every slot end offset is within the file.
5. Replay wal-G.log sequentially: parse header, wrap the payload in a
   CRC-checked reader, decode the transaction, read and verify the trailer CRC.
   Apply each good record to the in-memory overlay. On the first bad/short/
   mismatching frame stop and truncate the WAL to the last good offset, then
   fsync. Reopen the WAL in append mode at that offset.
6. Delete stale files. Only then print the ready line (main.rs already does
   this after Engine::open returns).

## Transactions

Same semantics as the MemStorage reference: overlay map (table, pk) -> optional
row in the transaction, statement undo log, DDL marks. Commit serializes DDL
first, then row writes, streams one WAL record and fsyncs it; only then applies
the overlay to the committed in-memory map and clears the transaction. If the
WAL append fails, the partially written bytes are truncated back to the previous
length and the commit reports an error without applying anything.
Rollback and statement rollback never touch the WAL. Statements run inside
begin/commit/rollback from the engine, so a failed statement cannot leave a
partial WAL record.

## Reads

Visibility order: transaction overlay > committed overlay > checkpoint base.
get: point lookup in the two overlays, then binary search of the pk index, then
read the row frame and decode it (row cache first).
scan: three-way merge over the sorted base slots, the sorted committed
BTreeMap and a sorted list of transaction overlay entries. Base rows are read
with a buffered sequential reader (no seek while offsets advance), cached as
Arc<Row>, and passed to the callback as &Row so no row is cloned. Deleted rows
are suppressed by the merge, never by materialising the table.

## Tunables

- MINIVECTORDB_ROW_CACHE_MIB: row cache budget in MiB (default 128, 0 disables).
- MINIVECTORDB_CHECKPOINT_MIB: WAL size that forces a checkpoint (default 64).
  Committed row count threshold default 512k rows.

## Known trade-offs / remaining work

- Checkpoint rewrites the whole base. For the official 5-10 MB workload this is
  irrelevant; for a multi-hundred-MB data set written in one process it causes
  write amplification (B metric). A per-table or LSM-style segment scheme was
  considered but deliberately deferred: correctness first.
- The row cache is FIFO, not LRU. Repeated full scans stay hot; pathological
  access patterns can evict useful rows.
- No asynchronous checkpointing, no per-table drop (SQL does not support DROP
  TABLE), no multi-process locking (single process by contest definition).
- Unit tests still to be written in disk.rs: reopen restores catalog/rows/index
  metadata; transaction and statement rollback across reopen; torn WAL tail
  truncated (partial header and partial payload) and later commits still work;
  several checkpoints with updates and deletes; WAL growth bounded by the
  threshold; stale/interrupted-checkpoint files ignored on double reopen;
  read-your-own-writes in a transaction; 200k-row write/scan/reopen.

## Immediate resume checklist

1. Add 'mod format;' to src/storage/mod.rs (Lead-owned, ask first).
2. cargo test to confirm format.rs tests pass.
3. Implement src/storage/disk.rs exactly as described above.
4. cargo test (all crate tests, including the 200k-row test).
5. Report WAL layout, durability window and env tunables to the Lead.
