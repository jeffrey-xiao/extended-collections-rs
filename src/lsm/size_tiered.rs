use bincode::{deserialize, self, serialize, serialized_size};
use lsm::{CompactionStrategy, sstable::SSTable};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::hash::Hash;
use std::io::{BufWriter, BufReader, BufRead, Read, Write};
use std::path::{PathBuf};
use std::sync::{Arc, Mutex, atomic::AtomicBool};

struct SizeTieredMetadata<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    sstables: Vec<SSTable<T, U>>,
}

impl<T, U> SizeTieredMetadata<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    pub fn new() -> Self {
        SizeTieredMetadata {
            sstables: Vec::new(),
        }
    }

    pub fn should_compact(&self) -> bool { false }
}

pub struct SizeTieredStrategy<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    min_sstable_count: usize,
    min_sstable_size: u64,
    bucket_low: f64,
    bucket_high: f64,
    max_in_memory_size: u64,
    should_compact: AtomicBool,
    is_compacting: AtomicBool,
    curr_metadata: Arc<Mutex<SizeTieredMetadata<T, U>>>,
    next_metadata: Arc<Mutex<Option<SizeTieredMetadata<T, U>>>>,
    new_sstables: Vec<SSTable<T, U>>,
}

impl<T, U> SizeTieredStrategy<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    pub fn new(
        min_sstable_count: usize,
        min_sstable_size: u64,
        bucket_low: f64,
        bucket_high: f64,
        max_in_memory_size: u64,
    ) -> Self {
        SizeTieredStrategy {
            min_sstable_count,
            min_sstable_size,
            bucket_low,
            bucket_high,
            max_in_memory_size,
            should_compact: AtomicBool::new(false),
            is_compacting: AtomicBool::new(false),
            curr_metadata: Arc::new(Mutex::new(SizeTieredMetadata::new())),
            next_metadata: Arc::new(Mutex::new(None)),
            new_sstables: Vec::new(),
        }
    }

}

impl<T, U> CompactionStrategy<T, U> for SizeTieredStrategy<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn should_compact(&self) -> bool { false }

    fn try_compact(&self, sstable: PathBuf) { }

    fn get_max_in_memory_size(&self) -> u64 {
        self.max_in_memory_size
    }
}
