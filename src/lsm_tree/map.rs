use bincode::serialized_size;
use lsm_tree::compaction::CompactionStrategy;
use lsm_tree::{SSTable, SSTableBuilder, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::cmp;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::mem;

pub struct LsmMap<T, U, V> {
    in_memory_tree: BTreeMap<T, Option<U>>,
    in_memory_usage: u64,
    compaction_strategy: V,
}

impl<T, U, V> LsmMap<T, U, V>
where
    T: Clone + Ord + Hash + DeserializeOwned + Serialize,
    U: Clone + DeserializeOwned + Serialize,
    V: CompactionStrategy<T, U>,
{
    pub fn new(compaction_strategy: V) -> Self {
        LsmMap {
            in_memory_tree: BTreeMap::new(),
            in_memory_usage: 0,
            compaction_strategy: compaction_strategy,
        }
    }

    fn compact(&mut self) -> Result<()> {
        self.in_memory_usage = 0;
        let mut sstable_builder = SSTableBuilder::new(
            self.compaction_strategy.get_db_path(),
            self.in_memory_tree.len(),
        )?;
        for entry in mem::replace(&mut self.in_memory_tree, BTreeMap::new()) {
            sstable_builder.append(entry.0, entry.1)?;
        }
        let sstable = SSTable::new(sstable_builder.flush()?)?;
        self.compaction_strategy.try_compact(sstable)
    }

    pub fn insert(&mut self, key: T, value: U) -> Result<()> {
        let key_size = serialized_size(&key)?;
        let value_size = serialized_size(&value)?;
        if self.in_memory_tree.contains_key(&key) {
            let value_size = serialized_size(&self.in_memory_tree[&key])?;
            self.in_memory_usage -= key_size + value_size;
        }
        self.in_memory_usage += key_size + value_size;
        self.in_memory_tree.insert(key, Some(value));

        if self.in_memory_usage > self.compaction_strategy.get_max_in_memory_size() {
            self.compact()
        } else {
            Ok(())
        }
    }

    pub fn remove(&mut self, key: T) -> Result<()> {
        let key_size = serialized_size(&key)?;
        if self.in_memory_tree.contains_key(&key) {
            let value_size = serialized_size(&self.in_memory_tree[&key])?;
            self.in_memory_usage -= key_size + value_size;
        }
        self.in_memory_usage += serialized_size(&key)?;
        self.in_memory_usage += serialized_size::<Option<U>>(&None)?;
        self.in_memory_tree.insert(key, None);

        if self.in_memory_usage > self.compaction_strategy.get_max_in_memory_size() {
            self.compact()
        } else {
            Ok(())
        }
    }

    pub fn contains_key(&mut self, key: &T) -> Result<bool> {
        self.get(key).map(|value| value.is_some())
    }

    pub fn get(&mut self, key: &T) -> Result<Option<U>> {
        if let Some(entry) = self.in_memory_tree.get(&key) {
            Ok(entry.clone())
        } else {
            self.compaction_strategy.get(key)
        }
    }

    pub fn len_hint(&mut self) -> Result<usize> {
        Ok(self.in_memory_tree.len() + self.compaction_strategy.len_hint()?)
    }

    pub fn len(&mut self) -> Result<usize> {
        Ok(self.iter()?.count())
    }

    pub fn is_empty(&mut self) -> Result<bool> {
        self.len().map(|len| len == 0)
    }

    pub fn clear(&mut self) -> Result<()> {
        self.in_memory_tree.clear();
        self.compaction_strategy.clear()
    }

    pub fn min(&mut self) -> Result<Option<T>> {
        let in_memory_min = self.in_memory_tree
            .iter()
            .skip_while(|entry| entry.1.is_none())
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

    pub fn max(&mut self) -> Result<Option<T>> {
        Ok(cmp::max(
            self.in_memory_tree
                .iter()
                .rev()
                .skip_while(|entry| entry.1.is_none())
                .next()
                .map(|entry| entry.0.clone()),
            self.compaction_strategy.max()?,
        ))
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.in_memory_tree.is_empty() {
            self.compact()?;
            self.compaction_strategy.flush()
        } else {
            Ok(())
        }
    }

    pub fn iter(&mut self) -> Result<Box<Iterator<Item=Result<(T, U)>>>> {
        self.flush()?;
        self.compaction_strategy.iter()
    }
}
