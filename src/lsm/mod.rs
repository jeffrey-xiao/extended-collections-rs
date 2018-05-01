mod size_tiered;
mod sstable;
mod write_ahead_log;

pub use self::size_tiered::SizeTieredStrategy;
pub use self::sstable::{SSTable, SSTableBuilder};

use bincode::{deserialize, self, serialize, serialized_size};
use bloom::BloomFilter;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::hash::Hash;
use std::io::{Write, self};
use std::marker::{PhantomData, Send, Sync};
use std::mem;
use std::path::{Path, PathBuf};
use std::result;
use std::sync::{Arc, Mutex, atomic::AtomicBool};
use std::thread;

#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    SerdeError(bincode::Error),
}

pub type Result<T> = result::Result<T, Error>;

pub trait CompactionStrategy<T, U> {
    fn get_db_path(&self) -> &Path;

    fn get_max_in_memory_size(&self) -> u64;

    fn try_compact(&self, sstable: SSTable<T, U>) -> Result<()>;

    fn get(&self, key: &T) -> Result<Option<U>>;
}

pub struct Tree<T, U, V> {
    db_path: PathBuf,
    in_memory_tree: BTreeMap<T, Option<U>>,
    in_memory_usage: u64,
    compaction_strategy: Arc<V>,
}

impl<T, U, V> Tree<T, U, V>
where
    T: Clone + Ord + Hash + DeserializeOwned + Serialize,
    U: Clone + DeserializeOwned + Serialize,
    V: CompactionStrategy<T, U>,
{
    pub fn new(compaction_strategy: V) -> Result<Self> {
        let db_path = PathBuf::from(compaction_strategy.get_db_path());
        fs::create_dir(db_path.clone()).map_err(Error::IOError)?;
        Ok(Tree {
            db_path,
            in_memory_tree: BTreeMap::new(),
            in_memory_usage: 0,
            compaction_strategy: Arc::new(compaction_strategy),
        })
    }

    fn try_compact(&mut self) -> Result<()> {
        if self.in_memory_usage <= self.compaction_strategy.get_max_in_memory_size() {
            return Ok(());
        }
        self.in_memory_usage = 0;
        let mut sstable_builder = SSTableBuilder::new(&self.db_path, self.in_memory_tree.len())?;
        for entry in mem::replace(&mut self.in_memory_tree, BTreeMap::new()) {
            sstable_builder.append(entry.0, entry.1)?;
        }
        let sstable = SSTable::new(sstable_builder.flush()?)?;
        self.compaction_strategy.try_compact(sstable)?;
        Ok(())
    }

    pub fn insert(&mut self, key: T, value: U) -> Result<()> {
        let key_size = serialized_size(&key).map_err(Error::SerdeError)?;
        let value_size = serialized_size(&value).map_err(Error::SerdeError)?;
        if self.in_memory_tree.contains_key(&key) {
            let value_size = serialized_size(&self.in_memory_tree[&key])
                .map_err(Error::SerdeError)?;
            self.in_memory_usage -= key_size + value_size;
        }
        self.in_memory_usage += key_size + value_size;
        self.in_memory_tree.insert(key, Some(value));
        println!("{}", self.in_memory_usage);
        self.try_compact()
    }

    pub fn remove(&mut self, key: T) -> Result<()> {
        let key_size = serialized_size(&key).map_err(Error::SerdeError)?;
        if self.in_memory_tree.contains_key(&key) {
            let value_size = serialized_size(&self.in_memory_tree[&key])
                .map_err(Error::SerdeError)?;
            self.in_memory_usage -= key_size + value_size;
        }
        self.in_memory_usage += serialized_size(&key).map_err(Error::SerdeError)?;
        self.in_memory_usage += serialized_size::<Option<U>>(&None).map_err(Error::SerdeError)?;
        self.in_memory_tree.insert(key, None);
        self.try_compact()
    }

    pub fn get(&self, key: &T) -> Result<Option<U>> {
        if let Some(entry) = self.in_memory_tree.get(&key) {
            Ok(entry.clone())
        } else {
            self.compaction_strategy.get(key)
        }
    }
}
