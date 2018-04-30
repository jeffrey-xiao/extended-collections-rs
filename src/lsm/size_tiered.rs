use bincode::{deserialize, self, serialize, serialized_size};
use lsm::{CompactionStrategy, sstable::SSTable, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::hash::Hash;
use std::io::{BufWriter, BufReader, BufRead, Read, Write};
use std::path::{PathBuf};
use std::sync::{Arc, Mutex, atomic::AtomicBool};

struct SizeTieredMetadata<T, U> {
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

pub struct SizeTieredStrategy<T, U> {
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

    fn get_compaction_range(&self) -> Option<(usize, usize)> {
        let curr_metadata = self.curr_metadata.lock().unwrap();
        let mut start = 0;
        let mut curr = 0;
        let mut range_size = 0;
        while curr < curr_metadata.sstables.len() {
            let start_size = curr_metadata.sstables[start].summary.size;
            let curr_size = curr_metadata.sstables[curr].summary.size;
            let curr_avg = (range_size + curr_size) as f64 / (curr - start + 1) as f64;

            let in_min_bucket = curr_size <= self.min_sstable_size;
            let in_bucket = curr_avg * self.bucket_low <= start_size as f64
                && curr_size as f64 <= curr_avg * self.bucket_high;

            curr += 1;
            if in_min_bucket || in_bucket {
                range_size += curr_size;
            } else if curr - start > self.min_sstable_count {
                return Some((start, curr));
            } else {
                range_size = 0;
                start = curr;
            }
        }

        if curr - start > self.min_sstable_count {
            Some((start, curr))
        } else {
            None
        }
    }
}

impl<T, U> CompactionStrategy<T, U> for SizeTieredStrategy<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn try_compact(&self, mut sstable: SSTable<T, U>) -> Result<()> {
        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        sstable.summary.tag = {
            let sstable = curr_metadata.sstables
                .iter()
                .max_by_key(|sstable| sstable.summary.tag);
            match sstable {
                Some(sstable) => sstable.summary.tag,
                None => 0,
            }
        };
        curr_metadata.sstables.push(sstable);
        curr_metadata.sstables.sort_by_key(|sstable| sstable.summary.size);
        drop(curr_metadata);

        if let Some(range) = self.get_compaction_range() {
            println!("COMPACTING");
        }

        Ok(())
    }

    fn get_max_in_memory_size(&self) -> u64 {
        self.max_in_memory_size
    }
}
