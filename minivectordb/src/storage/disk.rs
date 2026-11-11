//! Durable storage engine (write-ahead log + segmented row files).
//!
//! Owned by the storage teammate. This stub keeps the crate compiling until
//! the WAL-backed engine lands; it delegates to the in-memory engine and is
//! therefore NOT crash safe. Replace the body, keep the trait contract.

use std::path::Path;

use crate::types::DbResult;

pub struct DiskStorage {
    inner: super::MemStorage,
    #[allow(dead_code)]
    dir: std::path::PathBuf,
}

impl DiskStorage {
    pub fn open(dir: &Path) -> DbResult<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(DiskStorage { inner: super::MemStorage::new(), dir: dir.to_path_buf() })
    }
}

impl super::Storage for DiskStorage {
    fn table(&self, name: &str) -> Option<crate::types::TableDef> {
        self.inner.table(name)
    }
    fn table_by_id(&self, id: u32) -> Option<crate::types::TableDef> {
        self.inner.table_by_id(id)
    }
    fn list_tables(&self) -> Vec<crate::types::TableDef> {
        self.inner.list_tables()
    }
    fn create_table(
        &mut self,
        name: &str,
        columns: Vec<crate::types::ColumnDef>,
        pk: usize,
    ) -> DbResult<crate::types::TableDef> {
        self.inner.create_table(name, columns, pk)
    }
    fn create_vector_index(&mut self, table_id: u32, column: usize) -> DbResult<()> {
        self.inner.create_vector_index(table_id, column)
    }
    fn vector_indexes(&self, table_id: u32) -> Vec<usize> {
        self.inner.vector_indexes(table_id)
    }
    fn in_txn(&self) -> bool {
        self.inner.in_txn()
    }
    fn begin(&mut self) -> DbResult<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> DbResult<()> {
        self.inner.commit()
    }
    fn rollback(&mut self) -> DbResult<()> {
        self.inner.rollback()
    }
    fn statement_mark(&mut self) {
        self.inner.statement_mark()
    }
    fn rollback_statement(&mut self) -> DbResult<()> {
        self.inner.rollback_statement()
    }
    fn get(&self, table_id: u32, pk: i64) -> DbResult<Option<crate::types::Row>> {
        self.inner.get(table_id, pk)
    }
    fn put(&mut self, table_id: u32, pk: i64, row: crate::types::Row) -> DbResult<()> {
        self.inner.put(table_id, pk, row)
    }
    fn delete(&mut self, table_id: u32, pk: i64) -> DbResult<bool> {
        self.inner.delete(table_id, pk)
    }
    fn scan(
        &self,
        table_id: u32,
        f: &mut dyn FnMut(i64, &crate::types::Row) -> bool,
    ) -> DbResult<()> {
        self.inner.scan(table_id, f)
    }
    fn flush(&mut self) -> DbResult<()> {
        self.inner.flush()
    }
}
