# DiskStorage: segmented durable storage engine (task-1, v3)

Status: IMPLEMENTED and validated (2026-11-12). Replaces the previous
full-rewrite checkpoint engine because that design rewrote the whole data set
inside a COMMIT and could exceed the 60 s single-request timeout on large data.

## Why segments

The old engine rewrote ckpt + committed WAL into a new data file on every
checkpoint, and the checkpoint ran synchronously inside a COMMIT request. With
2 GB of data one commit could write 2 GB, which is exactly the kind of request
that trips the 60 s evaluator timeout. The new engine only ever writes (a) the
new committed rows and (b) one size-bounded merge of old segments per commit,
so COMMIT latency is bounded and independent of the total table size.

## Data directory layout (/data)

    MANIFEST               atomic snapshot: generation, next segment id,
                           catalog (tables + vector index metadata),
                           ordered live segment list (oldest -> newest)
    seg-<id>.dat           immutable row frames sorted by (table id, pk)
    seg-<id>.idx           per table: sorted row slots (pk, frame offset,
                           payload len) and a sorted tombstone pk list
    wal-<generation>.log   committed transactions not yet in a segment

MANIFEST is small (catalog plus segment ids) and is the only switch point. Old
segments are immutable; a flush never touches them.

## Commit path

1. Build the transaction payload size exactly (every WAL op has a deterministic
   encoded size), then stream one record into wal-<gen>.log:
   12-byte header (magic, payload len, kind) + payload + 4-byte CRC trailer over
   kind ++ payload. Nothing large is buffered; a 2000-row batch is one record.
2. fsync (fdatasync) the WAL, only then return success and apply the overlay in
   memory.
3. If the WAL exceeds MINIVECTORDB_FLUSH_MIB (default 64 MiB) or the overlay
   exceeds 512k rows, run one flush+merge (below). A crash anywhere leaves
   either no record (no effect) or a complete checksummed record (full effect).

## Flush and bounded merge (single atomic switch per commit)

When a flush is due:

1. Stream the committed overlay into seg-<new>.dat/.idx as .tmp files, fsync
   both, rename into place. Cost is O(new rows), never O(table size).
2. If the live segment count exceeds MINIVECTORDB_SEGMENTS (default 16), pick
   the oldest contiguous run whose total data size fits MINIVECTORDB_MERGE_MIB
   (default 128 MiB) and merge it into seg-<new2>.dat/.idx. Newer inputs shadow
   older ones; rows shadowed inside the run are dropped. Tombstones are dropped
   only when the run starts at the oldest segment, otherwise they are carried
   forward (a tombstone merged from a middle run still has to hide older rows).
   Merge cost is bounded by the budget, so worst-case commit work is
   segment_write (<= WAL threshold) + merge_write (<= merge budget).
3. Create an empty wal-<gen+1>.log and fsync it.
4. Write MANIFEST.tmp, fsync, rename over MANIFEST, fsync the directory. This
   rename is the single atomic switch point.
5. Only after the switch: update in-memory state, then delete the old WAL file
   and the segments consumed by the merge (best effort).

Crash windows: before step 4 the old manifest plus the old WAL replay to the
committed state and the new files are orphans; after step 4 the new generation
is complete and durable. Orphan files are deleted on the next open.

## Read path

Visibility order: transaction overlay, committed overlay, then segments from
newest to oldest. A tombstone in a newer layer hides older rows, so deletes and
updates across segments stay correct.

get: two overlay lookups, then for each segment from newest to oldest check its
tombstone list and binary search its pk index; the first hit wins.

scan: k-way merge of the transaction overlay (sorted), the committed BTreeMap
and one cursor per live segment for that table (row slots and tombstones merged
into one pk-ordered stream). Each segment is read with its own 64 KiB buffered
reader in pk order, so a scan is streaming and never materialises a table.
Rows are passed to the callback as &Row; the only resident structures are the
20-byte-per-row pk -> file-position index and a bounded FIFO row cache
(MINIVECTORDB_ROW_CACHE_MIB, default 128 MiB) keyed by (segment id, table id,
pk). The table id is part of the key because different tables share a segment
and may share the same primary key.

## Transactions

Unchanged semantics: overlay map (table, pk) -> optional row inside the
transaction, statement undo log with an undo mark, DDL marks. COMMIT writes one
WAL record and fsyncs; only then the overlay becomes the committed in-memory
overlay. ROLLBACK and statement rollback never touch disk. Statement-level
rollback unwinds only the undo entries recorded after the last statement_mark.

## Recovery (must finish before the ready handshake)

1. Read MANIFEST; if absent, initialize a fresh generation 1 (any leftover
   files with our prefixes are removed first).
2. Load the catalog, then load every segment index, validating sorting and that
   every slot's frame end lies inside its data file.
3. Replay wal-<gen>.log: parse the header, stream the payload through a
   CRC-checked reader, verify the trailer CRC, apply the transaction. Stop at
   the first short or mismatching frame, truncate the WAL to the last good
   offset, fsync. Truncation is idempotent, so recovery may itself be killed and
   retried.
4. Open the WAL for append and delete orphan files (unreferenced segments, old
   WAL generations, temp files).

Recovery cost is: manifest + segment index files (20 bytes per row) + at most
one WAL threshold of replay. It never rewrites data.

## Tunables (environment)

    MINIVECTORDB_FLUSH_MIB      WAL bytes that trigger a segment flush (64)
    MINIVECTORDB_MERGE_MIB      max bytes one merge may rewrite (128)
    MINIVECTORDB_SEGMENTS       segment-count target that triggers merges (16)
    MINIVECTORDB_ROW_CACHE_MIB  decoded-row cache budget (128, 0 disables)

## Measured (WSL Ubuntu, release build, /data on ext4)

Official workload (10k rows x 128 dims, 100 batches of 100 rows, 100 commits;
then 200 filtered/unfiltered exact TopK K=10):

- write: 0.84 s total, average 5.6 ms per commit, max 9 ms;
- query: average 0.41 ms, max 9 ms (engine keeps the small data set hot);
- /data 6 MB; restart ready 0.002 s.

Large data (500k rows x 128 dims, 2000 rows per batch/commit, 250 commits):

- write: 33.0 s total; per commit average 58 ms, p99 416 ms, MAX 447 ms
  (bounded merge of at most 128 MiB; never a full rewrite);
- peak process RSS during writes: 111.7 MB; /data 272.5 MB;
- segments during the run: 4 x <= 64.2 MB; after the shutdown flush 5 segments;
- restart to ready: 0.03 s; recovery returns the correct first rows;
- exact TopK over the whole table: about 3.2 s per query, peak RSS 513 MB --
  this cost is the engine's full exact scan plus result materialisation, not
  storage reads.

Very large data (1.28 M rows x 128 dims = 698 MB on disk, 4000 rows per commit):

- write: 82.1 s total; per commit average 108 ms, p99 460 ms, max 474 ms;
- peak RSS during writes 154 MB; 10 segments of <= 65 MB (no merge needed:
  segment count stayed below the target);
- restart to ready 0.08 s; post-restart point lookups correct; post-restart
  peak RSS 226 MB.

Forced-merge scale test (16 MiB flush threshold, 128 MiB merge budget, target 4
segments, 400k rows): per commit average 63 ms, p99 735 ms, max 972 ms
(i.e. one 16 MB segment write plus at most one 128 MB merge), segment count
stayed at 4 with the largest segment 118 MB, RSS 52 MB. Hard kill after acks:
restart 0.06 s, all acked rows present; a second restart also correct.

Resource case from tools/verify (200k rows x 128 dims): bulk insert 35.8 s,
recovery_open 0.377 s, exact TopK 0.874 s, ANN 0.008 s, point lookup 72.8 ms.

## Validation

- cargo test: 58 passed, 0 failed, 2 ignored (includes 16 disk tests: reopen,
  torn WAL tail, drop-without-flush crash simulation, transaction and statement
  rollback, multi-segment merge recovery, tombstones across merges, cache
  aliasing, orphan/temp cleanup, bounded segment count, 200k-row streaming
  scan).
- official public cases (protocol_check): 20 / 20 PASS.
- team edge suite (tools/verify/run_all.py): 45 / 45 PASS, durability 15/15.
- official-like admission suite (tools/verify/official_like_cases.py): 56/61.
  Failures are outside storage: the certain one is the SQL parser rejecting
  the -9223372036854775808 literal; the four uncertain ones are affected_rows
  changed-vs-matched semantics, backslash escapes and CREATE TABLE IF NOT
  EXISTS.

## Known trade-offs / risks

- Merge rewrites up to MINIVECTORDB_MERGE_MIB per commit (128 MiB default,
  measured worst commit 447 ms). Lower it for a tighter per-request bound at
  the cost of keeping more segments live.
- Segment count is bounded only while merges fit the budget: when two segments
  are both larger than the merge budget the count can exceed the target. For
  the 1-2 GB resource range the measured count stays at 4-8.
- Query memory is dominated by the engine materialising full exact-scan results
  (513 MB peak for 500k x 128 dims); the storage layer itself stays at
  index + 128 MiB cache.
- No multi-process locking (single process per the contest definition).
