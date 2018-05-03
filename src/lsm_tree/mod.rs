mod size_tiered;
mod sstable;

pub use self::size_tiered::SizeTieredStrategy;
pub use self::sstable::{SSTable, SSTableBuilder, SSTableDataIter};

use bincode::{self, serialized_size};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::cmp;
use std::collections::BTreeMap;
use std::error;
use std::fmt;
use std::hash::Hash;
use std::io::{self};
use std::mem;
use std::path::Path;
use std::result;

#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    SerdeError(bincode::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Error {
        Error::SerdeError(err)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            &Error::IOError(ref error) => error.description(),
            &Error::SerdeError(ref error) => error.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            &Error::IOError(ref error) => error.cause(),
            &Error::SerdeError(ref error) => error.cause(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::IOError(ref error) => write!(f, "{}", error),
            &Error::SerdeError(ref error) => write!(f, "{}", error),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

pub trait CompactionStrategy<T, U> {
    fn get_db_path(&self) -> &Path;

    fn get_max_in_memory_size(&self) -> u64;

    fn try_compact(&mut self, sstable: SSTable<T, U>) -> Result<()>;

    fn get(&self, key: &T) -> Result<Option<U>>;

    fn len_hint(&self) -> Result<usize>;

    fn len(&self) -> Result<usize>;

    fn clear(&mut self) -> Result<()>;

    fn min(&self) -> Result<Option<T>>;

    fn max(&self) -> Result<Option<T>>;

    fn iter(&self) -> Result<Box<Iterator<Item=Result<(T, U)>>>>;
}

pub struct LsmMap<T, U, V> {
    in_memory_tree: BTreeMap<T, Option<U>>,
    in_memory_usage: u64,
    compaction_strategy: V,
}

impl<T, U, V> LsmMap<T, U, V>
where
    T: ::std::fmt::Debug + Clone + Ord + Hash + DeserializeOwned + Serialize,
    U: ::std::fmt::Debug + Clone + DeserializeOwned + Serialize,
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

    pub fn contains_key(&self, key: &T) -> Result<bool> {
        self.get(key).map(|value| value.is_some())
    }

    pub fn get(&self, key: &T) -> Result<Option<U>> {
        if let Some(entry) = self.in_memory_tree.get(&key) {
            Ok(entry.clone())
        } else {
            self.compaction_strategy.get(key)
        }
    }

    pub fn len_hint(&self) -> Result<usize> {
        Ok(self.in_memory_tree.len() + self.compaction_strategy.len_hint()?)
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.in_memory_tree.len() + self.compaction_strategy.len()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        self.len().map(|len| len == 0)
    }

    pub fn clear(&mut self) -> Result<()> {
        self.compaction_strategy.clear()
    }

    pub fn min(&self) -> Result<Option<T>> {
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

    pub fn max(&self) -> Result<Option<T>> {
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
            self.compact()
        } else {
            Ok(())
        }
    }
}
