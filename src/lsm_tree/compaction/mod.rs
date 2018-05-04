pub mod size_tiered;

pub use self::size_tiered::SizeTieredStrategy;

use lsm_tree::{SSTable, Result};
use std::path::Path;

/// Trait for types that have compaction logic for SSTables.
///
/// A compaction strategy should incrementally accept SStables and handle the logic for creating
/// new SSTables, deleting stale SSTables, and searching through current SSTables.
pub trait CompactionStrategy<T, U> {
    fn get_db_path(&self) -> &Path;

    fn get_max_in_memory_size(&self) -> u64;

    fn try_compact(&mut self, sstable: SSTable<T, U>) -> Result<()>;

    fn flush(&mut self) -> Result<()>;

    fn get(&mut self, key: &T) -> Result<Option<U>>;

    fn len_hint(&mut self) -> Result<usize>;

    fn len(&mut self) -> Result<usize>;

    fn clear(&mut self) -> Result<()>;

    fn min(&mut self) -> Result<Option<T>>;

    fn max(&mut self) -> Result<Option<T>>;

    fn iter(&mut self) -> Result<Box<Iterator<Item=Result<(T, U)>>>>;
}
