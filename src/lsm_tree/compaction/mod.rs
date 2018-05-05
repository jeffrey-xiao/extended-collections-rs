pub mod size_tiered;

pub use self::size_tiered::SizeTieredStrategy;

use lsm_tree::{Result, SSTable, SSTableValue};
use std::path::Path;

/// An iterator for the SSTables on disk.
pub type CompactionIter<T, U> = Iterator<Item=Result<(T, U)>>;

/// Trait for types that have compaction logic for SSTables.
///
/// A compaction strategy should incrementally accept SStables and handle the logic for creating
/// new SSTables, deleting stale SSTables, and searching through current SSTables.
pub trait CompactionStrategy<T, U> {
    fn get_db_path(&self) -> &Path;

    fn get_max_in_memory_size(&self) -> u64;

    fn get_and_increment_logical_time(&mut self) -> Result<u64>;

    fn try_compact(&mut self, sstable: SSTable<T, U>) -> Result<()>;

    fn flush(&mut self) -> Result<()>;

    fn get(&mut self, key: &T) -> Result<Option<SSTableValue<U>>>;

    fn len_hint(&mut self) -> Result<usize>;

    fn len(&mut self) -> Result<usize>;

    fn is_empty(&mut self) -> Result<bool>;

    fn clear(&mut self) -> Result<()>;

    fn min(&mut self) -> Result<Option<T>>;

    fn max(&mut self) -> Result<Option<T>>;

    fn iter(&mut self) -> Result<Box<CompactionIter<T, U>>>;
}