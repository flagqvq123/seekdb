//! On-disk encodings for the durable storage engine.
//!
//! Every byte that has to survive a crash is framed with a self-describing
//! record: a 12-byte header (magic, payload length, record kind), the payload,
//! and a 4-byte CRC-32 trailer computed over kind ++ payload.  Recovery scans
//! frames until the first one whose header is missing, whose length overruns
//! the file or whose checksum fails, and then truncates the file at that
//! boundary.  That is what makes torn writes safe and recovery idempotent.
//!
//! The trailer (instead of an inline checksum) lets the writer stream a very
//! large transaction straight to the WAL without materialising the whole
//! record in memory: the payload length is known up front (each operation has
//! a deterministic encoded size) while the checksum is emitted last.

use std::io::{self, Read, Write};

use crate::types::{ColType, ColumnDef, DbError, DbResult, Row, TableDef, Value, ERR_IO};

/// Magic that prefixes every record frame ("MVDR").
pub const RECORD_MAGIC: u32 = 0x4D56_4452;
/// Bytes in the record header: magic (u32), payload length (u32), kind (u32).
pub const HEADER_LEN: usize = 12;
/// Bytes in the record trailer: CRC-32 of kind ++ payload.
pub const TRAILER_LEN: usize = 4;
/// Record kind of a committed transaction record in the WAL.
pub const KIND_TXN: u32 = 0;
/// Record kind of one row stored in a checkpoint data file.
pub const KIND_ROW: u32 = 1;
/// Hard cap for one WAL record; keeps recovery away from absurd allocations.
pub const MAX_WAL_RECORD: u64 = 1024 * 1024 * 1024;

/// Magic of the checkpoint catalog file ("MVCA").
pub const CATALOG_MAGIC: u32 = 0x4D56_4341;
/// Magic of the checkpoint index file ("MVID").
pub const INDEX_MAGIC: u32 = 0x4D56_4944;
pub const CONTAINER_VERSION: u32 = 1;
/// Bytes in a checkpoint container header.
pub const CONTAINER_HEADER_LEN: usize = 24;
/// Upper bound for the in-memory catalog body (defensive, catalogs are tiny).
pub const MAX_CATALOG_BODY: u64 = 64 * 1024 * 1024;

/// Bytes stored per row in the on-disk/in-memory checkpoint index.
#[allow(dead_code)]
pub const SLOT_LEN: usize = 20;

// Value tags used by the row encoding.
const TAG_NULL: u8 = 0;
const TAG_INT: u8 = 1;
const TAG_FLOAT: u8 = 2;
const TAG_STR: u8 = 3;
const TAG_VEC: u8 = 4;

// Column type tags used by the catalog encoding.
const TYPE_BIGINT: u8 = 0;
const TYPE_VARCHAR: u8 = 1;
const TYPE_VECTOR: u8 = 2;

// WAL operation tags.
pub const OP_PUT: u8 = 1;
pub const OP_DELETE: u8 = 2;
pub const OP_CREATE_TABLE: u8 = 3;
pub const OP_CREATE_INDEX: u8 = 4;

/// Encoded size of a delete operation (tag, table id, primary key).
pub const WAL_OP_DELETE_LEN: usize = 13;
/// Encoded size of a create-index operation (tag, table id, column).
pub const WAL_OP_CREATE_INDEX_LEN: usize = 9;

pub fn corrupt(message: &str) -> DbError {
    DbError::new(ERR_IO, message)
}

// ---------------------------------------------------------------------------
// CRC-32 (IEEE 802.3, reflected, polynomial 0xEDB88320)
// ---------------------------------------------------------------------------

const CRC_TABLE: [u32; 256] = build_crc_table();

const fn build_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 { 0xEDB8_8320 ^ (crc >> 1) } else { crc >> 1 };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Incremental CRC-32 state.
#[derive(Clone, Copy)]
pub struct Crc32 {
    state: u32,
}

impl Crc32 {
    pub fn new() -> Self {
        Crc32 { state: 0xFFFF_FFFF }
    }
    pub fn update(&mut self, data: &[u8]) {
        let mut state = self.state;
        for byte in data {
            state = CRC_TABLE[((state ^ *byte as u32) & 0xFF) as usize] ^ (state >> 8);
        }
        self.state = state;
    }
    pub fn finish(&self) -> u32 {
        self.state ^ 0xFFFF_FFFF
    }
}

impl Default for Crc32 {
    fn default() -> Self {
        Crc32::new()
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = Crc32::new();
    crc.update(data);
    crc.finish()
}

/// Write adapter that feeds everything through a CRC and counts bytes.
pub struct CrcWriter<W: Write> {
    inner: W,
    crc: Crc32,
    written: u64,
}

impl<W: Write> CrcWriter<W> {
    pub fn new(inner: W) -> Self {
        CrcWriter { inner, crc: Crc32::new(), written: 0 }
    }
    /// Continue an existing checksum (used when the record kind is hashed
    /// before the payload is streamed).
    pub fn with_crc(inner: W, crc: Crc32) -> Self {
        CrcWriter { inner, crc, written: 0 }
    }
    /// Returns the wrapped writer, the number of payload bytes and the checksum.
    pub fn finish(self) -> (W, u64, u32) {
        (self.inner, self.written, self.crc.finish())
    }
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc.update(&buf[..n]);
        self.written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Read adapter that verifies a known-length body through a CRC.
pub struct CrcReader<R: Read> {
    inner: R,
    crc: Crc32,
    remaining: u64,
}

impl<R: Read> CrcReader<R> {
    pub fn new(inner: R, remaining: u64) -> Self {
        CrcReader { inner, crc: Crc32::new(), remaining }
    }
    /// Continue an existing checksum (record kind hashed ahead of the payload).
    pub fn with_crc(inner: R, remaining: u64, crc: Crc32) -> Self {
        CrcReader { inner, crc, remaining }
    }
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
    /// Returns the wrapped reader and the checksum of the bytes consumed.
    pub fn finish(self) -> (R, u32) {
        (self.inner, self.crc.finish())
    }
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let max = buf.len().min(self.remaining as usize);
        let n = self.inner.read(&mut buf[..max])?;
        self.crc.update(&buf[..n]);
        self.remaining -= n as u64;
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// Record framing
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct FrameHeader {
    pub kind: u32,
    pub len: u32,
}

/// Total bytes occupied by a frame with the given payload length.
pub fn frame_total_len(payload_len: u64) -> u64 {
    HEADER_LEN as u64 + payload_len + TRAILER_LEN as u64
}

pub fn parse_frame_header(bytes: &[u8; HEADER_LEN]) -> Option<FrameHeader> {
    let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if magic != RECORD_MAGIC {
        return None;
    }
    let len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let kind = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    Some(FrameHeader { kind, len })
}

#[allow(dead_code)]
pub fn encode_frame(kind: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(frame_total_len(payload.len() as u64) as usize);
    let mut crc = Crc32::new();
    crc.update(&kind.to_le_bytes());
    crc.update(payload);
    out.extend_from_slice(&RECORD_MAGIC.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&kind.to_le_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&crc.finish().to_le_bytes());
    out
}

/// Stream one frame.  Writes the header first, so a crash leaves either a
/// header that overruns the file (torn) or a checksum mismatch (torn).
pub fn write_frame<W: Write>(writer: &mut W, kind: u32, payload: &[u8]) -> io::Result<()> {
    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(&RECORD_MAGIC.to_le_bytes());
    header[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[8..12].copy_from_slice(&kind.to_le_bytes());
    writer.write_all(&header)?;
    writer.write_all(payload)?;
    let mut crc = Crc32::new();
    crc.update(&kind.to_le_bytes());
    crc.update(payload);
    writer.write_all(&crc.finish().to_le_bytes())
}

/// Read one complete, checksum-validated frame.
pub fn read_frame<R: Read>(reader: &mut R, expected_len: u32) -> DbResult<(u32, Vec<u8>)> {
    let mut header = [0u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    let header = parse_frame_header(&header).ok_or_else(|| corrupt("bad record header"))?;
    if header.len != expected_len {
        return Err(corrupt("record length does not match the index"));
    }
    let mut payload = vec![0u8; header.len as usize];
    reader.read_exact(&mut payload)?;
    let mut trailer = [0u8; TRAILER_LEN];
    reader.read_exact(&mut trailer)?;
    let stored = u32::from_le_bytes(trailer);
    let mut crc = Crc32::new();
    crc.update(&header.kind.to_le_bytes());
    crc.update(&payload);
    if crc.finish() != stored {
        return Err(corrupt("record checksum mismatch"));
    }
    Ok((header.kind, payload))
}

// ---------------------------------------------------------------------------
// Byte cursor (decoding from a fully buffered payload)
// ---------------------------------------------------------------------------

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, pos: 0 }
    }
    fn take(&mut self, n: usize) -> DbResult<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| corrupt("record length overflow"))?;
        if end > self.bytes.len() {
            return Err(corrupt("record is truncated"));
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
    fn u8(&mut self) -> DbResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> DbResult<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> DbResult<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }
    fn i64(&mut self) -> DbResult<i64> {
        Ok(self.u64()? as i64)
    }
    fn f32(&mut self) -> DbResult<f32> {
        let b = self.take(4)?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn f64(&mut self) -> DbResult<f64> {
        let b = self.take(8)?;
        Ok(f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }
    fn utf8(&mut self, n: usize) -> DbResult<String> {
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| corrupt("invalid UTF-8 in record"))
    }
    fn empty(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

// ---------------------------------------------------------------------------
// Row encoding
// ---------------------------------------------------------------------------

/// Exact encoded size of a row.
pub fn encoded_row_len(row: &Row) -> usize {
    let mut len = 4usize;
    for value in row {
        len += 1;
        len += match value {
            Value::Null => 0,
            Value::Int(_) | Value::Float(_) => 8,
            Value::Str(s) => 4 + s.len(),
            Value::Vec(v) => 4 + 4 * v.len(),
        };
    }
    len
}

pub fn encode_row_into(out: &mut Vec<u8>, row: &Row) {
    out.extend_from_slice(&(row.len() as u32).to_le_bytes());
    for value in row {
        match value {
            Value::Null => out.push(TAG_NULL),
            Value::Int(v) => {
                out.push(TAG_INT);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float(v) => {
                out.push(TAG_FLOAT);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::Str(s) => {
                out.push(TAG_STR);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Value::Vec(components) => {
                out.push(TAG_VEC);
                out.extend_from_slice(&(components.len() as u32).to_le_bytes());
                for c in components {
                    out.extend_from_slice(&c.to_le_bytes());
                }
            }
        }
    }
}

#[allow(dead_code)]
pub fn encode_row(row: &Row) -> Vec<u8> {
    let mut out = Vec::with_capacity(encoded_row_len(row));
    encode_row_into(&mut out, row);
    out
}

pub fn decode_row(bytes: &[u8]) -> DbResult<Row> {
    let mut cursor = Cursor::new(bytes);
    let columns = cursor.u32()? as usize;
    if columns > 1_000_000 {
        return Err(corrupt("row has an implausible column count"));
    }
    let mut row = Vec::with_capacity(columns.min(64));
    for _ in 0..columns {
        let tag = cursor.u8()?;
        let value = match tag {
            TAG_NULL => Value::Null,
            TAG_INT => Value::Int(cursor.i64()?),
            TAG_FLOAT => Value::Float(cursor.f64()?),
            TAG_STR => {
                let len = cursor.u32()? as usize;
                Value::Str(cursor.utf8(len)?)
            }
            TAG_VEC => {
                let len = cursor.u32()? as usize;
                if len > 1_000_000 {
                    return Err(corrupt("vector has an implausible dimension"));
                }
                let mut components = Vec::with_capacity(len.min(4096));
                for _ in 0..len {
                    components.push(cursor.f32()?);
                }
                Value::Vec(components)
            }
            _ => return Err(corrupt("unknown value tag in row")),
        };
        row.push(value);
    }
    Ok(row)
}

// ---------------------------------------------------------------------------
// Table definition encoding
// ---------------------------------------------------------------------------

pub fn table_def_len(def: &TableDef) -> u64 {
    let mut len = 4 + 4 + 4 + def.name.len() as u64 + 4;
    for column in &def.columns {
        len += 4 + column.name.len() as u64 + 1 + 4;
    }
    len
}

pub fn encode_table_def_into(out: &mut Vec<u8>, def: &TableDef) {
    out.extend_from_slice(&def.id.to_le_bytes());
    out.extend_from_slice(&(def.pk as u32).to_le_bytes());
    out.extend_from_slice(&(def.name.len() as u32).to_le_bytes());
    out.extend_from_slice(def.name.as_bytes());
    out.extend_from_slice(&(def.columns.len() as u32).to_le_bytes());
    for column in &def.columns {
        out.extend_from_slice(&(column.name.len() as u32).to_le_bytes());
        out.extend_from_slice(column.name.as_bytes());
        match column.ty {
            ColType::BigInt => {
                out.push(TYPE_BIGINT);
                out.extend_from_slice(&0u32.to_le_bytes());
            }
            ColType::Varchar(n) => {
                out.push(TYPE_VARCHAR);
                out.extend_from_slice(&n.to_le_bytes());
            }
            ColType::Vector(d) => {
                out.push(TYPE_VECTOR);
                out.extend_from_slice(&d.to_le_bytes());
            }
        }
    }
}

fn decode_column_type(tag: u8, param: u32) -> DbResult<ColType> {
    match tag {
        TYPE_BIGINT => Ok(ColType::BigInt),
        TYPE_VARCHAR => Ok(ColType::Varchar(param)),
        TYPE_VECTOR => Ok(ColType::Vector(param)),
        _ => Err(corrupt("unknown column type tag")),
    }
}

fn decode_table_def(cursor: &mut Cursor<'_>) -> DbResult<TableDef> {
    let id = cursor.u32()?;
    let pk = cursor.u32()? as usize;
    let name_len = cursor.u32()? as usize;
    let name = cursor.utf8(name_len)?;
    let column_count = cursor.u32()? as usize;
    if column_count > 65535 {
        return Err(corrupt("table has an implausible column count"));
    }
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let column_name_len = cursor.u32()? as usize;
        let column_name = cursor.utf8(column_name_len)?;
        let tag = cursor.u8()?;
        let param = cursor.u32()?;
        columns.push(ColumnDef { name: column_name, ty: decode_column_type(tag, param)?, is_pk: false });
    }
    if pk >= columns.len() {
        return Err(corrupt("primary key column is out of range"));
    }
    for (index, column) in columns.iter_mut().enumerate() {
        column.is_pk = index == pk;
    }
    Ok(TableDef { id, name, columns, pk })
}

// ---------------------------------------------------------------------------
// WAL transaction records
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum WalOp {
    Put { table_id: u32, pk: i64, row: Row },
    Delete { table_id: u32, pk: i64 },
    CreateTable(TableDef),
    CreateIndex { table_id: u32, column: u32 },
}

/// Encoded payload bytes for one PUT operation.
pub fn wal_put_len(row: &Row) -> u64 {
    (1 + 4 + 8 + 4) as u64 + encoded_row_len(row) as u64
}

pub fn wal_create_table_len(def: &TableDef) -> u64 {
    1 + table_def_len(def)
}

/// Streaming writer for a transaction record payload.
pub struct TxnWriter<W: Write> {
    inner: W,
}

impl<W: Write> TxnWriter<W> {
    pub fn new(inner: W) -> Self {
        TxnWriter { inner }
    }
    pub fn put(mut self, table_id: u32, pk: i64, row: &Row) -> io::Result<Self> {
        let mut payload = Vec::with_capacity(12 + encoded_row_len(row));
        payload.push(OP_PUT);
        payload.extend_from_slice(&table_id.to_le_bytes());
        payload.extend_from_slice(&pk.to_le_bytes());
        payload.extend_from_slice(&(encoded_row_len(row) as u32).to_le_bytes());
        encode_row_into(&mut payload, row);
        self.inner.write_all(&payload)?;
        Ok(self)
    }
    pub fn delete(mut self, table_id: u32, pk: i64) -> io::Result<Self> {
        let mut buf = [0u8; WAL_OP_DELETE_LEN];
        buf[0] = OP_DELETE;
        buf[1..5].copy_from_slice(&table_id.to_le_bytes());
        buf[5..13].copy_from_slice(&pk.to_le_bytes());
        self.inner.write_all(&buf)?;
        Ok(self)
    }
    pub fn create_table(mut self, def: &TableDef) -> io::Result<Self> {
        let mut payload = Vec::with_capacity(table_def_len(def) as usize + 1);
        payload.push(OP_CREATE_TABLE);
        encode_table_def_into(&mut payload, def);
        self.inner.write_all(&payload)?;
        Ok(self)
    }
    pub fn create_index(mut self, table_id: u32, column: u32) -> io::Result<Self> {
        let mut buf = [0u8; WAL_OP_CREATE_INDEX_LEN];
        buf[0] = OP_CREATE_INDEX;
        buf[1..5].copy_from_slice(&table_id.to_le_bytes());
        buf[5..9].copy_from_slice(&column.to_le_bytes());
        self.inner.write_all(&buf)?;
        Ok(self)
    }
    pub fn finish(mut self, next_table_id: u32) -> io::Result<W> {
        self.inner.write_all(&next_table_id.to_le_bytes())?;
        Ok(self.inner)
    }
}

/// Write a count header for a transaction payload.
pub fn write_txn_header<W: Write>(writer: &mut W, op_count: u32) -> io::Result<()> {
    writer.write_all(&op_count.to_le_bytes())
}

/// Convenience encoder used by tests and small auto-commit records.
pub fn encode_txn(ops: &[WalOp], next_table_id: u32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    for op in ops {
        match op {
            WalOp::Put { table_id, pk, row } => {
                out.push(OP_PUT);
                out.extend_from_slice(&table_id.to_le_bytes());
                out.extend_from_slice(&pk.to_le_bytes());
                out.extend_from_slice(&(encoded_row_len(row) as u32).to_le_bytes());
                encode_row_into(&mut out, row);
            }
            WalOp::Delete { table_id, pk } => {
                out.push(OP_DELETE);
                out.extend_from_slice(&table_id.to_le_bytes());
                out.extend_from_slice(&pk.to_le_bytes());
            }
            WalOp::CreateTable(def) => {
                out.push(OP_CREATE_TABLE);
                encode_table_def_into(&mut out, def);
            }
            WalOp::CreateIndex { table_id, column } => {
                out.push(OP_CREATE_INDEX);
                out.extend_from_slice(&table_id.to_le_bytes());
                out.extend_from_slice(&column.to_le_bytes());
            }
        }
    }
    out.extend_from_slice(&next_table_id.to_le_bytes());
    out
}

/// Streaming decoder for a transaction record payload.
pub fn read_txn<R: Read>(reader: &mut R) -> DbResult<(Vec<WalOp>, u32)> {
    let op_count = stream_u32(reader)? as usize;
    if op_count > 50_000_000 {
        return Err(corrupt("transaction record has an implausible operation count"));
    }
    let mut ops = Vec::with_capacity(op_count.min(4096));
    for _ in 0..op_count {
        let tag = stream_u8(reader)?;
        let op = match tag {
            OP_PUT => {
                let table_id = stream_u32(reader)?;
                let pk = stream_u64(reader)? as i64;
                let len = stream_u32(reader)? as usize;
                if len as u64 > MAX_WAL_RECORD {
                    return Err(corrupt("row payload is implausibly large"));
                }
                let bytes = stream_bytes(reader, len)?;
                WalOp::Put { table_id, pk, row: decode_row(&bytes)? }
            }
            OP_DELETE => {
                let table_id = stream_u32(reader)?;
                let pk = stream_u64(reader)? as i64;
                WalOp::Delete { table_id, pk }
            }
            OP_CREATE_TABLE => WalOp::CreateTable(stream_table_def(reader)?),
            OP_CREATE_INDEX => {
                let table_id = stream_u32(reader)?;
                let column = stream_u32(reader)?;
                WalOp::CreateIndex { table_id, column }
            }
            _ => return Err(corrupt("unknown WAL operation tag")),
        };
        ops.push(op);
    }
    let next_table_id = stream_u32(reader)?;
    Ok((ops, next_table_id))
}

fn stream_u8<R: Read>(reader: &mut R) -> DbResult<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn stream_u32<R: Read>(reader: &mut R) -> DbResult<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn stream_u64<R: Read>(reader: &mut R) -> DbResult<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn stream_bytes<R: Read>(reader: &mut R, len: usize) -> DbResult<Vec<u8>> {
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

fn stream_table_def<R: Read>(reader: &mut R) -> DbResult<TableDef> {
    let id = stream_u32(reader)?;
    let pk = stream_u32(reader)? as usize;
    let name_len = stream_u32(reader)? as usize;
    let name = String::from_utf8(stream_bytes(reader, name_len)?)
        .map_err(|_| corrupt("invalid UTF-8 in table name"))?;
    let column_count = stream_u32(reader)? as usize;
    if column_count > 65535 {
        return Err(corrupt("table has an implausible column count"));
    }
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let column_name_len = stream_u32(reader)? as usize;
        let column_name = String::from_utf8(stream_bytes(reader, column_name_len)?)
            .map_err(|_| corrupt("invalid UTF-8 in column name"))?;
        let tag = stream_u8(reader)?;
        let param = stream_u32(reader)?;
        columns.push(ColumnDef { name: column_name, ty: decode_column_type(tag, param)?, is_pk: false });
    }
    if pk >= columns.len() {
        return Err(corrupt("primary key column is out of range"));
    }
    for (index, column) in columns.iter_mut().enumerate() {
        column.is_pk = index == pk;
    }
    Ok(TableDef { id, name, columns, pk })
}

// ---------------------------------------------------------------------------
// Catalog snapshot
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct CatalogImage {
    pub tables: Vec<TableDef>,
    pub indexes: Vec<(u32, u32)>,
    pub next_table_id: u32,
}

pub fn encode_catalog(image: &CatalogImage) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(image.tables.len() as u32).to_le_bytes());
    for def in &image.tables {
        encode_table_def_into(&mut out, def);
    }
    out.extend_from_slice(&(image.indexes.len() as u32).to_le_bytes());
    for (table_id, column) in &image.indexes {
        out.extend_from_slice(&table_id.to_le_bytes());
        out.extend_from_slice(&column.to_le_bytes());
    }
    out.extend_from_slice(&image.next_table_id.to_le_bytes());
    out
}

pub fn decode_catalog(body: &[u8]) -> DbResult<CatalogImage> {
    let mut cursor = Cursor::new(body);
    let table_count = cursor.u32()? as usize;
    if table_count > 1_000_000 {
        return Err(corrupt("catalog has an implausible table count"));
    }
    let mut tables = Vec::with_capacity(table_count.min(1024));
    for _ in 0..table_count {
        tables.push(decode_table_def(&mut cursor)?);
    }
    let index_count = cursor.u32()? as usize;
    if index_count > 1_000_000 {
        return Err(corrupt("catalog has an implausible index count"));
    }
    let mut indexes = Vec::with_capacity(index_count.min(1024));
    for _ in 0..index_count {
        indexes.push((cursor.u32()?, cursor.u32()?));
    }
    let next_table_id = cursor.u32()?;
    if !cursor.empty() {
        return Err(corrupt("catalog body has trailing bytes"));
    }
    Ok(CatalogImage { tables, indexes, next_table_id })
}

// ---------------------------------------------------------------------------
// Checkpoint index: table id -> sorted (primary key, frame offset, payload len)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot {
    /// Row primary key (index order key).
    pub pk: i64,
    /// Byte offset of the row frame inside the checkpoint data file.
    pub off: u64,
    /// Length of the row payload.
    pub len: u32,
}

pub fn write_index_body<W: Write>(writer: &mut W, tables: &[(u32, &[Slot])]) -> io::Result<()> {
    writer.write_all(&(tables.len() as u32).to_le_bytes())?;
    for (table_id, slots) in tables {
        writer.write_all(&table_id.to_le_bytes())?;
        writer.write_all(&(slots.len() as u64).to_le_bytes())?;
        for slot in *slots {
            writer.write_all(&slot.pk.to_le_bytes())?;
            writer.write_all(&slot.off.to_le_bytes())?;
            writer.write_all(&slot.len.to_le_bytes())?;
        }
    }
    Ok(())
}

pub fn read_index_body<R: Read>(reader: &mut R) -> DbResult<Vec<(u32, Vec<Slot>)>> {
    let table_count = stream_u32(reader)? as usize;
    if table_count > 1_000_000 {
        return Err(corrupt("index has an implausible table count"));
    }
    let mut tables = Vec::with_capacity(table_count.min(1024));
    for _ in 0..table_count {
        let table_id = stream_u32(reader)?;
        let count = stream_u64(reader)?;
        if count > 1_000_000_000 {
            return Err(corrupt("index has an implausible row count"));
        }
        let mut slots = Vec::with_capacity(count.min(1_048_576) as usize);
        for _ in 0..count {
            let pk = stream_u64(reader)? as i64;
            let off = stream_u64(reader)?;
            let len = stream_u32(reader)?;
            slots.push(Slot { pk, off, len });
        }
        tables.push((table_id, slots));
    }
    Ok(tables)
}

// ---------------------------------------------------------------------------
// Checkpoint container header
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct ContainerHeader {
    pub version: u32,
    pub body_len: u64,
    pub crc: u32,
}

pub fn container_header(magic: u32, body_len: u64, crc: u32) -> [u8; CONTAINER_HEADER_LEN] {
    let mut header = [0u8; CONTAINER_HEADER_LEN];
    header[0..4].copy_from_slice(&magic.to_le_bytes());
    header[4..8].copy_from_slice(&CONTAINER_VERSION.to_le_bytes());
    header[8..16].copy_from_slice(&body_len.to_le_bytes());
    header[16..20].copy_from_slice(&crc.to_le_bytes());
    header
}

pub fn parse_container_header(bytes: &[u8; CONTAINER_HEADER_LEN], magic: u32) -> Option<ContainerHeader> {
    let stored_magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if stored_magic != magic {
        return None;
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let body_len = u64::from_le_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    let crc = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    Some(ContainerHeader { version, body_len, crc })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_table() -> TableDef {
        TableDef {
            id: 7,
            name: "docs".to_string(),
            columns: vec![
                ColumnDef { name: "id".to_string(), ty: ColType::BigInt, is_pk: true },
                ColumnDef { name: "body".to_string(), ty: ColType::Varchar(16), is_pk: false },
                ColumnDef { name: "embedding".to_string(), ty: ColType::Vector(3), is_pk: false },
            ],
            pk: 0,
        }
    }

    fn sample_row() -> Row {
        vec![
            Value::Int(-9007199254740993),
            Value::Str("你好".to_string()),
            Value::Vec(vec![1.5, -2.0, 3.25]),
        ]
    }

    #[test]
    fn crc32_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn row_roundtrip_covers_all_types() {
        let row = vec![
            Value::Null,
            Value::Int(i64::MIN),
            Value::Float(1.25),
            Value::Str("a".repeat(100)),
            Value::Vec(vec![0.5, -0.5, 1.0]),
        ];
        let encoded = encode_row(&row);
        assert_eq!(encoded.len(), encoded_row_len(&row));
        assert_eq!(decode_row(&encoded).unwrap(), row);
    }

    #[test]
    fn row_decode_rejects_truncation() {
        let encoded = encode_row(&sample_row());
        assert!(decode_row(&encoded[..encoded.len() - 1]).is_err());
        assert!(decode_row(&[]).is_err());
    }

    #[test]
    fn txn_roundtrip() {
        let def = sample_table();
        let ops = vec![
            WalOp::CreateTable(def.clone()),
            WalOp::CreateIndex { table_id: 7, column: 2 },
            WalOp::Put { table_id: 7, pk: 1, row: sample_row() },
            WalOp::Delete { table_id: 7, pk: 2 },
        ];
        let payload = encode_txn(&ops, 9);
        let (decoded, next_id) = read_txn(&mut payload.as_slice()).unwrap();
        assert_eq!(decoded, ops);
        assert_eq!(next_id, 9);
    }

    #[test]
    fn catalog_roundtrip() {
        let image = CatalogImage {
            tables: vec![sample_table()],
            indexes: vec![(7, 2)],
            next_table_id: 7,
        };
        let body = encode_catalog(&image);
        let decoded = decode_catalog(&body).unwrap();
        assert_eq!(decoded.tables.len(), 1);
        assert_eq!(decoded.tables[0].id, 7);
        assert_eq!(decoded.tables[0].columns[2].ty, ColType::Vector(3));
        assert_eq!(decoded.tables[0].pk, 0);
        assert_eq!(decoded.indexes, vec![(7, 2)]);
        assert_eq!(decoded.next_table_id, 7);
    }

    #[test]
    fn index_body_roundtrip() {
        let slots = vec![
            Slot { pk: -1, off: 0, len: 16 },
            Slot { pk: 2, off: 32, len: 24 },
        ];
        let tables: Vec<(u32, &[Slot])> = vec![(3, slots.as_slice())];
        let mut body = Vec::new();
        write_index_body(&mut body, &tables).unwrap();
        let decoded = read_index_body(&mut body.as_slice()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, 3);
        assert_eq!(decoded[0].1, slots);
    }

    #[test]
    fn frame_roundtrip_and_torn_detection() {
        let frame = encode_frame(KIND_TXN, b"hello");
        let decoded = read_frame(&mut frame.as_slice(), 5).unwrap();
        assert_eq!(decoded, (KIND_TXN, b"hello".to_vec()));
        // Truncated trailer.
        assert!(read_frame(&mut &frame[..frame.len() - 1], 5).is_err());
        // Corrupted payload.
        let mut corrupted = frame.clone();
        corrupted[HEADER_LEN] ^= 0xFF;
        assert!(read_frame(&mut corrupted.as_slice(), 5).is_err());
        // Wrong expected length.
        assert!(read_frame(&mut frame.as_slice(), 6).is_err());
    }

    #[test]
    fn container_header_roundtrip() {
        let header = container_header(CATALOG_MAGIC, 1234, 0xDEAD_BEEF);
        let parsed = parse_container_header(&header, CATALOG_MAGIC).unwrap();
        assert_eq!(parsed.body_len, 1234);
        assert_eq!(parsed.crc, 0xDEAD_BEEF);
        assert_eq!(parsed.version, CONTAINER_VERSION);
        assert!(parse_container_header(&header, INDEX_MAGIC).is_none());
    }

    #[test]
    fn streaming_txn_writer_matches_batch_encoder() {
        let def = sample_table();
        let mut payload = Vec::new();
        write_txn_header(&mut payload, 3).unwrap();
        let writer = TxnWriter::new(&mut payload);
        let writer = writer.create_table(&def).unwrap();
        let writer = writer.put(7, 1, &sample_row()).unwrap();
        let writer = writer.delete(7, 2).unwrap();
        let _ = writer.finish(9).unwrap();
        let expected = encode_txn(
            &[
                WalOp::CreateTable(def),
                WalOp::Put { table_id: 7, pk: 1, row: sample_row() },
                WalOp::Delete { table_id: 7, pk: 2 },
            ],
            9,
        );
        assert_eq!(payload, expected);
    }
}
