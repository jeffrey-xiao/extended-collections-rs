//! Strategies for merging disk-resident sorted runs of data.

mod leveled;
mod size_tiered;

pub use self::leveled::LeveledStrategy;
pub use self::size_tiered::SizeTieredStrategy;

use lsm_tree::{Result, SSTable, SSTableValue};
use std::borrow::Borrow;
use std::hash::Hash;
use std::path::Path;

/// An iterator for the disk-resident data.
pub type CompactionIter<T, U> = Iterator<Item = Result<(T, U)>>;

/// Trait for types that have compaction logic for disk-resident data.
///
/// A compaction strategy should incrementally accept SSTables and handle the logic for creating
/// new SSTables, deleting stale SSTables, and searching through current SSTables.
pub trait CompactionStrategy<T, U> {
    /// Returns the path of the disk-resident data.
    fn get_path(&self) -> &Path;

    /// Returns the maximum size of the in-memory tree in bytes.
    fn get_max_in_memory_size(&self) -> u64;

    /// Returns and increments the current logical time of the compaction strategy.
    fn get_and_increment_logical_time(&mut self) -> Result<u64>;

    /// Adds a SSTable to the compaction strategy and compacts the SSTables being tracked, if
    /// needed.
    fn try_compact(&mut self, sstable: SSTable<T, U>) -> Result<()>;

    /// Waits until the current compaction thread, if any, terminates and updates the metadata of
    /// the compaction strategy.
    fn flush(&mut self) -> Result<()>;

    /// Searches through disk-resident data and returns the value associated with a particular key.
    /// It will return `None` if the key does not exist in the disk-resident data.
    fn get<V>(&mut self, key: &V) -> Result<Option<SSTableValue<U>>>
    where
        T: Borrow<V>,
        V: Ord + Hash + ?Sized;

    /// Returns the approximate number of items in the disk-resident data.
    fn len_hint(&mut self) -> Result<usize>;

    /// Returns the number of items in the disk-resident data.
    fn len(&mut self) -> Result<usize>;

    /// Returns `true` if the disk-resident data is empty.
    fn is_empty(&mut self) -> Result<bool>;

    /// Clears the disk-resident data, removing all values.
    fn clear(&mut self) -> Result<()>;

    /// Returns the minimum key of the disk-resident data. Returns `None` if the disk-resident data
    /// is empty.
    fn min(&mut self) -> Result<Option<T>>;

    /// Returns the maximum key of the disk-resident data. Returns `None` if the disk-resident data
    /// is empty.
    fn max(&mut self) -> Result<Option<T>>;

    /// Returns an iterator over the disk-resident data. The iterator will yield key-value pairs
    /// in ascending order.
    fn iter(&mut self) -> Result<Box<CompactionIter<T, U>>>;
}
