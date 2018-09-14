use bincode::serialized_size;
use lsm_tree::compaction::{CompactionIter, CompactionStrategy};
use lsm_tree::{Result, SSTable, SSTableBuilder, SSTableValue};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::borrow::Borrow;
use std::cmp;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::mem;

/// An ordered map implemented using a log structured merge-tree.
///
/// A log-structured merge-tree comprises of two components -- an in-memory tree and on-disk sorted
/// immutable lists called Sorted Strings Tables (SSTables). The in-memory tree is incrementally
/// flushed onto disk into SSTables when its size exceeds a certain threshold. When there are many
/// fragmented SSTables, they are merged together using a compaction strategy. When an entry is
/// replaced, it could occur in multiple SSTables. The value in the most recent SSTable is fetched.
/// When an entry is deleted, a tombstone is inserted to indicate that the entry is deleted.
///
/// # Examples
///
/// ```
/// # use extended_collections::lsm_tree::Result;
/// # fn foo() -> Result<()> {
/// # use std::fs;
/// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
/// use extended_collections::lsm_tree::LsmMap;
///
/// let sts = SizeTieredStrategy::new("example_lsm_map", 10000, 4, 50000, 0.5, 1.5)?;
/// let mut map = LsmMap::new(sts);
///
/// map.insert(0, 1)?;
/// map.insert(3, 4)?;
///
/// assert_eq!(map.get(&0)?, Some(1));
/// assert_eq!(map.get(&1)?, None);
/// assert_eq!(map.len()?, 2);
/// assert!(map.len_hint()? >= 2);
///
/// assert_eq!(map.min()?, Some(0));
///
/// map.remove(0)?;
/// assert_eq!(map.get(&0)?, None);
///
/// map.flush();
/// # fs::remove_dir_all("example_lsm_map")?;
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
pub struct LsmMap<T, U, C> {
    in_memory_tree: BTreeMap<T, SSTableValue<U>>,
    in_memory_usage: u64,
    compaction_strategy: C,
}

impl<T, U, C> LsmMap<T, U, C>
where
    T: Clone + Ord + Hash + DeserializeOwned + Serialize,
    U: Clone + DeserializeOwned + Serialize,
    C: CompactionStrategy<T, U>,
{
    /// Constructs a new `LsmMap<T, U>` with a specific `CompactionStrategy<T, U>`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_new", 10000, 4, 50000, 0.5, 1.5)?;
    /// let map: LsmMap<u32, u32, _> = LsmMap::new(sts);
    /// # fs::remove_dir_all("example_lsm_map_new")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn new(compaction_strategy: C) -> Self {
        LsmMap {
            in_memory_tree: BTreeMap::new(),
            in_memory_usage: 0,
            compaction_strategy,
        }
    }

    fn try_compact(&mut self) -> Result<()> {
        self.in_memory_usage = 0;
        let mut sstable_builder = SSTableBuilder::new(
            self.compaction_strategy.get_path(),
            self.in_memory_tree.len(),
        )?;
        for entry in mem::replace(&mut self.in_memory_tree, BTreeMap::new()) {
            sstable_builder.append(entry.0, entry.1)?;
        }
        let sstable = SSTable::new(sstable_builder.flush()?)?;
        self.compaction_strategy.try_compact(sstable)
    }

    /// Inserts a key-value pair into the map. If the key-value pair causes the size of the
    /// in-memory tree to exceed its size threshold, it will flush the data into a SSTable and then
    /// compact the SSTables if necessary.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_insert", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// assert_eq!(map.get(&1)?, Some(1));
    ///
    /// map.insert(1, 2)?;
    /// assert_eq!(map.get(&1)?, Some(2));
    /// # fs::remove_dir_all("example_lsm_map_insert")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn insert(&mut self, key: T, value: U) -> Result<()> {
        let value = SSTableValue {
            data: Some(value),
            logical_time: self.compaction_strategy.get_and_increment_logical_time()?,
        };
        let key_size = serialized_size(&key)?;
        let value_size = serialized_size(&value)?;

        if let Some(ref value) = self.in_memory_tree.get(&key) {
            let value_size = serialized_size(value)?;
            self.in_memory_usage -= key_size + value_size;
        }

        self.in_memory_usage += key_size + value_size;
        self.in_memory_tree.insert(key, value);

        if self.in_memory_usage > self.compaction_strategy.get_max_in_memory_size() {
            self.try_compact()
        } else {
            Ok(())
        }
    }

    /// Removes a key-value pair into the map by inserting a tombstone. If the key-value pair causes
    /// the size of the in-memory tree to exceed its size threshold, it will flush the data into a
    /// SSTable and then compact the SSTables if necessary.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_remove", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// assert_eq!(map.get(&1)?, Some(1));
    ///
    /// map.remove(1)?;
    /// assert_eq!(map.get(&1)?, None);
    /// # fs::remove_dir_all("example_lsm_map_remove")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn remove(&mut self, key: T) -> Result<()> {
        let key_size = serialized_size(&key)?;
        let value = SSTableValue {
            data: None,
            logical_time: self.compaction_strategy.get_and_increment_logical_time()?,
        };

        if let Some(ref value) = self.in_memory_tree.get(&key) {
            let value_size = serialized_size(value)?;
            self.in_memory_usage -= key_size + value_size;
        }

        self.in_memory_usage += serialized_size(&key)?;
        self.in_memory_usage += serialized_size(&value)?;
        self.in_memory_tree.insert(key, value);

        if self.in_memory_usage > self.compaction_strategy.get_max_in_memory_size() {
            self.try_compact()
        } else {
            Ok(())
        }
    }

    /// Checks if a key exists in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_contains_key", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// assert!(!map.contains_key(&0)?);
    /// assert!(map.contains_key(&1)?);
    /// # fs::remove_dir_all("example_lsm_map_contains_key")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn contains_key<V>(&mut self, key: &V) -> Result<bool>
    where
        T: Borrow<V>,
        V: Ord + Hash + ?Sized,
    {
        self.get(key).map(|value| value.is_some())
    }

    /// Returns the value associated with a particular key. It will return `None` if the key does
    /// not exist in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_get", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// assert_eq!(map.get(&0)?, None);
    /// assert_eq!(map.get(&1)?, Some(1));
    /// # fs::remove_dir_all("example_lsm_map_get")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn get<V>(&mut self, key: &V) -> Result<Option<U>>
    where
        T: Borrow<V>,
        V: Ord + Hash + ?Sized,
    {
        if let Some(value) = self.in_memory_tree.get(&key) {
            Ok(value.data.clone())
        } else {
            self.compaction_strategy
                .get(key)
                .map(|value_opt| value_opt.and_then(|value| value.data))
        }
    }

    /// Returns the approximate number of elements in the map. The length returned will always be
    /// greater than or equal to the actual length. It counts all the non-tombstone entries stored
    /// in the SSTables, so it will overcount if there are duplicate entries or if a tombstone
    /// overrides previous entries. For an accurate, but slower way of getting the length, see
    /// `len`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_len_hint", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// assert!(map.len_hint()? >= 1);
    /// # fs::remove_dir_all("example_lsm_map_len_hint")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn len_hint(&mut self) -> Result<usize> {
        Ok(self.in_memory_tree.len() + self.compaction_strategy.len_hint()?)
    }

    /// Returns the number of elements in the map by first flushing the in-memory tree and then
    /// doing a full scan of all entries. For a more efficient, but approximate way of getting the
    /// length, see `len_hint`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_len", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// assert_eq!(map.len()?, 1);
    /// # fs::remove_dir_all("example_lsm_map_len")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn len(&mut self) -> Result<usize> {
        self.flush()?;
        self.compaction_strategy.len()
    }

    /// Returns `true` if the map is empty. The in-memory tree is flushed and then a full scan of
    /// all entries is performed to determine if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_is_empty", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    /// assert!(map.is_empty()?);
    ///
    /// map.insert(1, 1)?;
    /// assert!(!map.is_empty()?);
    /// # fs::remove_dir_all("example_lsm_map_is_empty")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn is_empty(&mut self) -> Result<bool> {
        self.len().map(|len| len == 0)
    }

    /// Clears the map, removing all values. This function will wait for any ongoing compaction
    /// thread to terminate before removing all SSTables.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_clear", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// map.insert(2, 2)?;
    /// map.clear()?;
    /// assert!(map.is_empty()?);
    /// # fs::remove_dir_all("example_lsm_map_clear")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn clear(&mut self) -> Result<()> {
        self.in_memory_tree.clear();
        self.compaction_strategy.clear()
    }

    /// Returns the minimum key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_min", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// map.insert(3, 3)?;
    /// assert_eq!(map.min()?, Some(1));
    /// # fs::remove_dir_all("example_lsm_map_min")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn min(&mut self) -> Result<Option<T>> {
        let in_memory_min = self
            .in_memory_tree
            .iter()
            .skip_while(|entry| entry.1.data.is_none())
            .next()
            .map(|entry| entry.0.clone());
        let disk_min = self.compaction_strategy.min()?;

        if in_memory_min.is_none() {
            Ok(disk_min)
        } else if disk_min.is_none() {
            Ok(in_memory_min)
        } else {
            Ok(cmp::min(in_memory_min, disk_min))
        }
    }

    /// Returns the maximum key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_max", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// map.insert(3, 3)?;
    /// assert_eq!(map.max()?, Some(3));
    /// # fs::remove_dir_all("example_lsm_map_max")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn max(&mut self) -> Result<Option<T>> {
        Ok(cmp::max(
            self.in_memory_tree
                .iter()
                .rev()
                .skip_while(|entry| entry.1.data.is_none())
                .next()
                .map(|entry| entry.0.clone()),
            self.compaction_strategy.max()?,
        ))
    }

    /// Flushes the in-memory tree into a SSTable if it is not empty. The map must be flushed
    /// before being dropped or the contents of the in-memory tree will be lost.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_flush", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// map.insert(3, 3)?;
    /// map.flush()?;
    /// # fs::remove_dir_all("example_lsm_map_flush")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn flush(&mut self) -> Result<()> {
        if !self.in_memory_tree.is_empty() {
            self.try_compact()?;
        }
        self.compaction_strategy.flush()
    }

    /// Returns an iterator over the map. The iterator will yield key-value pairs in ascending
    /// order. The in-memory tree will be flushed before yielding the iterator. The map will not
    /// perform any compactions if there are any undropped iterators.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    /// use extended_collections::lsm_tree::LsmMap;
    ///
    /// let sts = SizeTieredStrategy::new("example_lsm_map_iter", 10000, 4, 50000, 0.5, 1.5)?;
    /// let mut map = LsmMap::new(sts);
    ///
    /// map.insert(1, 1)?;
    /// map.insert(2, 2)?;
    ///
    /// let mut iterator = map.iter()?.map(|value| value.unwrap());
    /// assert_eq!(iterator.next(), Some((1, 1)));
    /// assert_eq!(iterator.next(), Some((2, 2)));
    /// assert_eq!(iterator.next(), None);
    /// # fs::remove_dir_all("example_lsm_map_iter")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn iter(&mut self) -> Result<Box<CompactionIter<T, U>>> {
        self.flush()?;
        self.compaction_strategy.iter()
    }
}

// impl<'a, T, U> IntoIterator for &'a LsmMap<T, U>
// where
//     T: 'a,
//     U: 'a,
// {
//     type Item = Result<(T, U)>;
//     type IntoIter = BpMapIterMut<'a, T, U>;

//     fn into_iter(self) -> Self::IntoIter {
//         self.iter_mut().unwrap()
//     }
// }
