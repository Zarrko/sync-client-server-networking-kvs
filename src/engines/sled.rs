use sled::Db;
use crate::engines::KvsEngine;

#[derive(Clone)]
#[allow(missing_docs)]
pub struct SledKvsEngine(Db);

#[allow(missing_docs)]
impl SledKvsEngine {
    pub fn new(db: Db) -> Self {
        SledKvsEngine(db)
    }
}
/// An embedded LSM Tree Database.
/// Writes: Go to an in-memory buffer called MemTable which is a B-Tree/SkipList
/// When MemTables reaches a certain size, flush to Disk as immutable SortedStringTable (SSTs)
/// Over time, the number of SSTables would grow unbounded, compaction removes duplicate keys,
/// deleted entries are purged
/// Reads: Read memtable first then SSTs from newest to oldest.

#[allow(missing_docs)]
impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> crate::Result<()> {
        let _old_value = self.0.insert(key.as_bytes(), value.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> crate::Result<Option<String>> {
        match self.0.get(&key.as_bytes())? {
            Some(value) => {
                let val = String::from_utf8(value.to_vec())?;
                Ok(Some(val))
            },
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> crate::Result<()> {
        self.0.remove(&key.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }
}