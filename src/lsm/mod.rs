use bincode::{deserialize, serialize};
use bloom::BloomFilter;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{Error, Result, Write};
use std::marker::{PhantomData, Send, Sync};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

pub trait CompactionStrategy {
    fn should_compact<T>(&self, metadata: &Metadata<T>) -> bool
    where
        T: Ord + Clone,
    ;

    fn compact<T>(&self, db_path: PathBuf, metadata: Metadata<T>) -> Metadata<T>
    where
        T: Ord + Clone,
    ;
}

struct SizeTieredStrategy {
    tier_size: usize,
}

impl SizeTieredStrategy {
    fn should_compact<T>(&self, metadata: &Metadata<T>) -> bool
    where
        T: Ord + Clone,
    {
        metadata.entries.iter().any(|entry| entry.len() > self.tier_size)
    }

    fn compact<T>(&self, db_path: PathBuf, metadata: Metadata<T>) -> Metadata<T>
    where
        T: Ord + Clone,
    {
        Metadata::new()
    }
}

struct LevelTieredStrategy {}

pub struct SSTable<T: Hash, U>
where
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    file: File,
    bloom_filter: BloomFilter<T>,
    data: Option<(T, Option<U>)>,
    _marker: PhantomData<(T, U)>,
}

#[derive(Clone)]
pub struct Metadata<T>
where
    T: Ord + Clone,
{
    entries: Vec<BTreeMap<T, (T, PathBuf)>>,
}

impl<T> Metadata<T>
where
    T: Ord + Clone,
{
    pub fn new() -> Self {
        Metadata {
            entries: Vec::new(),
        }
    }
}

pub struct WriteAheadLog<T, U>
where
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    log: File,
    _marker: PhantomData<(T, U)>,
}

impl<T, U> WriteAheadLog<T, U>
where
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    pub fn new(log_path: &str) -> Result<WriteAheadLog<T, U>> {
        let log = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(log_path)?;

        Ok(WriteAheadLog {
            log,
            _marker: PhantomData,
        })
    }

    pub fn clear(&self) -> Result<()> {
        self.log.set_len(0)
    }

    pub fn append(&mut self, key: T, value: Option<U>) {
        self.log
            .write_all(&serialize(&(key, value)).unwrap())
            .unwrap();
    }
}

pub struct Tree<T, U, V>
where
    T: Ord + Clone + Send + Sync,
    V: CompactionStrategy + Send + Sync,
{
    db_path: PathBuf,
    curr_metadata: Arc<Mutex<Metadata<T>>>,
    next_metadata: Arc<Mutex<Option<Metadata<T>>>>,
    in_memory_tree: BTreeMap<T, Option<U>>,
    compaction_strategy: Arc<V>,
}

impl<T, U, V> Tree<T, U, V>
where
    T: Ord + Clone + Send + Sync + 'static,
    V: CompactionStrategy + Send + Sync,
{
    pub fn new(db_path: PathBuf, compaction_strategy: V) -> Self {
        Tree {
            db_path,
            curr_metadata: Arc::new(Mutex::new(Metadata::new())),
            next_metadata: Arc::new(Mutex::new(None)),
            in_memory_tree: BTreeMap::new(),
            compaction_strategy: Arc::new(compaction_strategy),
        }
    }

    fn spawn_compaction_thread(&self) {
        let curr_metadata = { self.curr_metadata.lock().unwrap().clone() };
        let next_metadata = Arc::clone(&self.next_metadata);
        let db_path = self.db_path.clone();
        let compaction_strategy = Arc::clone(&self.compaction_strategy);

        if compaction_strategy.should_compact(&curr_metadata) {
            thread::spawn(move || {
                *next_metadata.lock().unwrap() = None;
            });
        }
    }

    pub fn insert(&mut self, key: T, value: U) {
        self.in_memory_tree.insert(key, Some(value));
        self.spawn_compaction_thread();
    }

    pub fn remove(&mut self, key: T) {
        self.in_memory_tree.insert(key, None);
        self.spawn_compaction_thread();
    }
}
